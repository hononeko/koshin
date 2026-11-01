package dev.hononeko.koshin.adapters.nats

import dev.hononeko.koshin.core.model.Platform
import io.nats.client.JetStreamApiException
import io.nats.client.JetStreamManagement
import io.nats.client.api.ConsumerInfo
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Moves exhausted jobs to the DLQ when nobody handled their MaxDeliver advisory: it's core NATS, delivered at most
 * once, so it's lost if no forwarder is subscribed (e.g. during a rolling deployment) or the forwarder keeps failing.
 *
 * NATS keeps an exhausted job in the work queue (it only deletes on ACK). Running jobs and jobs waiting for
 * redelivery are ACK-pending and hold the consumer's ack floor back, while exhausted ones aren't and don't. So a job
 * still stored is exhausted if it's at or below the ack floor, or, when nothing is ACK-pending, at or below the last
 * delivered sequence (an exhausted job at the end of the queue never gets a later ACK to move the floor past it).
 * Each candidate is moved after [confirmDelay], and only if still stored: in a cluster a just-ACKed job can stay
 * visible briefly.
 */
class ExhaustedMessageSweeper(
    private val jsm: JetStreamManagement,
    private val config: NatsConfig,
    private val platforms: Collection<Platform>,
    private val confirmDelay: Duration = DEFAULT_CONFIRM_DELAY,
    private val maxPerSweep: Int = DEFAULT_MAX_PER_SWEEP,
    private val ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
    /** Moves the job to the DLQ; false if it's no longer stored. */
    private val moveToDeadLetter: (streamSeq: Long) -> Boolean,
) {
    private val logger = LoggerFactory.getLogger(ExhaustedMessageSweeper::class.java)

    /** One pass over every platform; returns how many jobs it moved to the DLQ. */
    suspend fun sweep(): Int = withContext(ioDispatcher) {
        val candidates = platforms.flatMap(::candidatesOrNone)
        if (candidates.isEmpty()) return@withContext 0
        delay(confirmDelay)
        var moved = 0
        for (seq in candidates) {
            try {
                if (moveToDeadLetter(seq)) moved++
            } catch (e: CancellationException) {
                throw e
            } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
                logger.warn("Could not move exhausted job seq {} to the DLQ; the next sweep retries", seq, e)
            }
        }
        if (moved > 0) {
            logger.atWarn().addKeyValue("moved", moved)
                .log("Moved {} exhausted job(s) whose MaxDeliver advisory was missed to the DLQ", moved)
        }
        moved
    }

    /** Sweeps every [interval], starting after a random part of it so workers don't all sweep at once. */
    fun start(scope: CoroutineScope, interval: Duration): Job = scope.launch {
        delay(interval * Random.nextDouble())
        while (isActive) {
            try {
                sweep()
            } catch (e: CancellationException) {
                throw e
            } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
                logger.warn("Exhausted-job sweep failed; retrying in {}", interval, e)
            }
            delay(interval)
        }
    }

    /** One platform's failure doesn't stop the others. */
    private fun candidatesOrNone(platform: Platform): List<Long> = try {
        candidates(platform)
    } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
        logger.warn("Could not look for exhausted {} jobs; the next sweep retries", platform, e)
        emptyList()
    }

    private fun candidates(platform: Platform): List<Long> {
        val bound = consumerInfo(platform)?.let(::exhaustedBound) ?: 0L
        val subject = config.jobsSubjectFor(platform)
        val found = mutableListOf<Long>()
        var next = if (bound > 0L) nextStored(1L, subject) else null
        while (next != null && next <= bound && found.size < maxPerSweep) {
            found += next
            next = nextStored(next + 1, subject)
        }
        return found
    }

    /** The highest sequence a stored job of this consumer can have and still be exhausted. */
    private fun exhaustedBound(consumer: ConsumerInfo): Long {
        val floor = consumer.ackFloor.streamSequence
        return if (consumer.numAckPending == 0L) maxOf(floor, consumer.delivered.streamSequence) else floor
    }

    private fun consumerInfo(platform: Platform): ConsumerInfo? = try {
        jsm.getConsumerInfo(config.jobsStreamName, config.consumerNameFor(platform))
    } catch (e: JetStreamApiException) {
        if (!e.isNotFound()) throw e
        null
    }

    private fun nextStored(fromSeq: Long, subject: String): Long? = try {
        jsm.getNextMessage(config.jobsStreamName, fromSeq, subject)?.seq
    } catch (e: JetStreamApiException) {
        if (!e.isNotFound()) throw e
        null
    }

    private fun JetStreamApiException.isNotFound() = apiErrorCode in NOT_FOUND_CODES || errorCode in NOT_FOUND_CODES

    companion object {
        private val DEFAULT_CONFIRM_DELAY = 5.seconds
        private const val DEFAULT_MAX_PER_SWEEP = 1000

        /** No message, no consumer, no stream. */
        private val NOT_FOUND_CODES = setOf(10037, 10014, 10059)
    }
}
