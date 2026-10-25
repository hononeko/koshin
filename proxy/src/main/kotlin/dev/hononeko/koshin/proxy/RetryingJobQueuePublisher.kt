package dev.hononeko.koshin.proxy

import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.ports.JobQueuePublisher
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import org.slf4j.LoggerFactory
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeMark
import kotlin.time.TimeSource

class RetryingJobQueuePublisher(
    private val delegate: JobQueuePublisher,
    private val baseDelay: Duration = DEFAULT_BASE_DELAY,
    private val maxTotalDelay: Duration = DEFAULT_MAX_TOTAL_DELAY,
    private val timeSource: TimeSource = TimeSource.Monotonic,
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
        require(!maxTotalDelay.isPositive() || baseDelay.isPositive()) {
            "baseDelay must be positive when maxTotalDelay > 0: baseDelay=$baseDelay, maxTotalDelay=$maxTotalDelay"
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
        val startMark = timeSource.markNow()
        var attempt = 0
        var accumulatedDelay = Duration.ZERO
        var nextDelay = baseDelay

        while (true) {
            attempt++
            val elapsed = maxOf(startMark.elapsedNow(), accumulatedDelay)
            try {
                runAttemptWithTimeout(elapsed, block)
                return
            } catch (e: TimeoutCancellationException) {
                val sleepDuration = computeRetryDelayOrThrow(
                    error = e,
                    operationName = operationName,
                    attempt = attempt,
                    startMark = startMark,
                    accumulatedDelay = accumulatedDelay,
                    nextDelay = nextDelay,
                )
                delayFn(sleepDuration)
                accumulatedDelay += sleepDuration
                nextDelay *= BACKOFF_MULTIPLIER
            } catch (e: CancellationException) {
                throw e
            } catch (e: IllegalArgumentException) {
                throw e
            } catch (e: Exception) {
                val sleepDuration = computeRetryDelayOrThrow(
                    error = e,
                    operationName = operationName,
                    attempt = attempt,
                    startMark = startMark,
                    accumulatedDelay = accumulatedDelay,
                    nextDelay = nextDelay,
                )
                delayFn(sleepDuration)
                accumulatedDelay += sleepDuration
                nextDelay *= BACKOFF_MULTIPLIER
            }
        }
    }

    private suspend fun runAttemptWithTimeout(elapsed: Duration, block: suspend () -> Unit) {
        if (maxTotalDelay > Duration.ZERO) {
            val remaining = (maxTotalDelay - elapsed).coerceAtLeast(1.milliseconds)
            withTimeout(remaining) {
                block()
            }
        } else {
            block()
        }
    }

    @Suppress("LongParameterList")
    private fun computeRetryDelayOrThrow(
        error: Exception,
        operationName: String,
        attempt: Int,
        startMark: TimeMark,
        accumulatedDelay: Duration,
        nextDelay: Duration,
    ): Duration {
        val elapsed = maxOf(startMark.elapsedNow(), accumulatedDelay)
        if (maxTotalDelay == Duration.ZERO || elapsed >= maxTotalDelay) {
            logger.error(
                "Failed {} after {} attempt(s) (elapsed {}); exhausting retries.",
                operationName,
                attempt,
                elapsed,
                error,
            )
            throw error
        }

        val remainingBudget = maxTotalDelay - elapsed
        val sleepDuration = minOf(nextDelay, remainingBudget)

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
