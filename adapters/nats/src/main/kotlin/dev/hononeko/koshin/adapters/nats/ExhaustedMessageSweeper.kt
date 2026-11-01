package dev.hononeko.koshin.adapters.nats

import dev.hononeko.koshin.core.model.Platform
import io.nats.client.JetStreamApiException
import io.nats.client.JetStreamManagement
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Moves exhausted jobs to the DLQ when nobody handled their MaxDeliver advisory: it's core NATS, delivered at most
 * once, so it's lost if no forwarder is subscribed (e.g. during a rolling deployment) or the forwarder keeps failing.
 *
 * NATS keeps an exhausted job in the work queue (it only deletes on ACK) but moves the consumer's ack floor past it
 * once a later job is ACKed. So a job still stored at or below its consumer's ack floor can only be an exhausted one.
 * Each candidate is checked again after [confirmDelay], because in a cluster a just-ACKed job can stay visible briefly.
 */
class ExhaustedMessageSweeper(
    private val jsm: JetStreamManagement,
    private val config: NatsConfig,
    private val platforms: Collection<Platform>,
    private val confirmDelay: Duration = DEFAULT_CONFIRM_DELAY,
    private val maxPerSweep: Int = DEFAULT_MAX_PER_SWEEP,
    private val moveToDeadLetter: (streamSeq: Long) -> Unit,
) {
    private val logger = LoggerFactory.getLogger(ExhaustedMessageSweeper::class.java)

    /** One pass over every platform; returns how many jobs it moved to the DLQ. */
    suspend fun sweep(): Int {
        val candidates = platforms.flatMap(::candidates)
        if (candidates.isEmpty()) return 0
        delay(confirmDelay)
        var moved = 0
        for (seq in candidates) {
            if (!isStored(seq)) continue
            try {
                moveToDeadLetter(seq)
                moved++
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
        return moved
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

    private fun candidates(platform: Platform): List<Long> {
        val floor = ackFloor(platform)
        if (floor <= 0L) return emptyList()
        val subject = config.jobsSubjectFor(platform)
        val found = mutableListOf<Long>()
        var next = nextStored(1L, subject)
        while (next != null && next <= floor && found.size < maxPerSweep) {
            found += next
            next = nextStored(next + 1, subject)
        }
        return found
    }

    private fun ackFloor(platform: Platform): Long = try {
        jsm.getConsumerInfo(config.jobsStreamName, config.consumerNameFor(platform)).ackFloor.streamSequence
    } catch (e: JetStreamApiException) {
        if (!e.isNotFound()) throw e
        0L
    }

    private fun nextStored(fromSeq: Long, subject: String): Long? = try {
        jsm.getNextMessage(config.jobsStreamName, fromSeq, subject)?.seq
    } catch (e: JetStreamApiException) {
        if (!e.isNotFound()) throw e
        null
    }

    private fun isStored(seq: Long): Boolean = try {
        jsm.getMessage(config.jobsStreamName, seq) != null
    } catch (e: JetStreamApiException) {
        if (!e.isNotFound()) throw e
        false
    }

    private fun JetStreamApiException.isNotFound() = apiErrorCode in NOT_FOUND_CODES || errorCode in NOT_FOUND_CODES

    companion object {
        private val DEFAULT_CONFIRM_DELAY = 5.seconds
        private const val DEFAULT_MAX_PER_SWEEP = 1000

        /** No message, no consumer, no stream. */
        private val NOT_FOUND_CODES = setOf(10037, 10014, 10059)
    }
}
