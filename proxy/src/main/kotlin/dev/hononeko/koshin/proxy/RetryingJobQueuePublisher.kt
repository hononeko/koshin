package dev.hononeko.koshin.proxy

import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.ports.JobQueuePublisher
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import org.slf4j.LoggerFactory
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class RetryingJobQueuePublisher(
    private val delegate: JobQueuePublisher,
    private val baseDelay: Duration = DEFAULT_BASE_DELAY,
    private val maxTotalDelay: Duration = DEFAULT_MAX_TOTAL_DELAY,
    private val delayFn: suspend (Duration) -> Unit = { delay(it) },
) : JobQueuePublisher {
    private val logger = LoggerFactory.getLogger(RetryingJobQueuePublisher::class.java)

    init {
        require(!baseDelay.isNegative() && baseDelay.isFinite()) {
            "baseDelay must be non-negative and finite: $baseDelay"
        }
        require(!maxTotalDelay.isNegative() && maxTotalDelay.isFinite()) {
            "maxTotalDelay must be non-negative and finite: $maxTotalDelay"
        }
    }

    override suspend fun publish(event: RenovateJobEvent) {
        executeWithRetry("publish(${event.repo}:${event.deliveryId})") {
            delegate.publish(event)
        }
    }

    override suspend fun publishToDeadLetter(event: RenovateJobEvent) {
        executeWithRetry("publishToDeadLetter(${event.repo}:${event.deliveryId})") {
            delegate.publishToDeadLetter(event)
        }
    }

    @Suppress("TooGenericExceptionCaught")
    private suspend fun executeWithRetry(operationName: String, block: suspend () -> Unit) {
        var attempt = 0
        var totalWait = Duration.ZERO
        var nextDelay = baseDelay

        while (true) {
            attempt++
            try {
                block()
                return
            } catch (e: CancellationException) {
                throw e
            } catch (e: IllegalArgumentException) {
                throw e
            } catch (e: Exception) {
                val sleepDuration = computeRetryDelayOrThrow(e, operationName, attempt, totalWait, nextDelay)
                delayFn(sleepDuration)
                totalWait += sleepDuration
                if (nextDelay > Duration.ZERO) {
                    nextDelay *= BACKOFF_MULTIPLIER
                }
            }
        }
    }

    private fun computeRetryDelayOrThrow(
        error: Exception,
        operationName: String,
        attempt: Int,
        totalWait: Duration,
        nextDelay: Duration,
    ): Duration {
        if (maxTotalDelay == Duration.ZERO || totalWait >= maxTotalDelay) {
            logger.error(
                "Failed {} after {} attempt(s) (total backoff {}); exhausting retries.",
                operationName,
                attempt,
                totalWait,
                error,
            )
            throw error
        }

        val remainingBudget = maxTotalDelay - totalWait
        val sleepDuration = if (nextDelay <= remainingBudget && nextDelay > Duration.ZERO) {
            nextDelay
        } else {
            remainingBudget
        }

        logger.warn(
            "Transient failure during {} on attempt {}, retrying in {}...",
            operationName,
            attempt,
            sleepDuration,
            error,
        )
        return sleepDuration
    }

    companion object {
        private const val BACKOFF_MULTIPLIER = 2
        val DEFAULT_BASE_DELAY: Duration = 100.milliseconds
        val DEFAULT_MAX_TOTAL_DELAY: Duration = 5.seconds
    }
}
