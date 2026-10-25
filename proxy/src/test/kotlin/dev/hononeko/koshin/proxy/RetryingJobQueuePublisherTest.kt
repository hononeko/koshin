package dev.hononeko.koshin.proxy

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.model.TriggerReason
import dev.hononeko.koshin.core.ports.JobQueuePublisher
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.IOException
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class RetryingJobQueuePublisherTest {

    private val sampleEvent = RenovateJobEvent(
        repo = RepoRef(Platform.GITHUB, "hononeko/koshin"),
        deliveryId = "deliv-1",
        triggerReason = TriggerReason.WEBHOOK,
    )

    @Test
    fun `retries transient failures with exponential backoff and succeeds when broker recovers`() = runTest {
        val delegate = mockk<JobQueuePublisher>()
        var attempts = 0
        coEvery { delegate.publish(sampleEvent) } answers {
            attempts++
            if (attempts < 3) {
                throw IOException("NATS leader election in progress")
            }
        }

        val recordedDelays = mutableListOf<Duration>()
        val retrying = RetryingJobQueuePublisher(
            delegate = delegate,
            baseDelay = 100.milliseconds,
            maxTotalDelay = 5.seconds,
            delayFn = { recordedDelays.add(it) },
        )

        retrying.publish(sampleEvent)

        assertEquals(3, attempts)
        assertEquals(listOf(100.milliseconds, 200.milliseconds), recordedDelays)
    }

    @Test
    fun `exhausts retry budget and rethrows exception when broker stays down`() = runTest {
        val delegate = mockk<JobQueuePublisher>()
        coEvery { delegate.publish(sampleEvent) } throws IOException("NATS unreachable")

        val recordedDelays = mutableListOf<Duration>()
        val retrying = RetryingJobQueuePublisher(
            delegate = delegate,
            baseDelay = 100.milliseconds,
            maxTotalDelay = 700.milliseconds,
            delayFn = { recordedDelays.add(it) },
        )

        assertThrows<IOException> {
            retrying.publish(sampleEvent)
        }

        assertEquals(listOf(100.milliseconds, 200.milliseconds, 400.milliseconds), recordedDelays)
        coVerify(exactly = 4) { delegate.publish(sampleEvent) }
    }

    @Test
    fun `caps final retry delay to remaining budget`() = runTest {
        val delegate = mockk<JobQueuePublisher>()
        coEvery { delegate.publishToDeadLetter(sampleEvent) } throws IOException("NATS DLQ failure")

        val recordedDelays = mutableListOf<Duration>()
        val retrying = RetryingJobQueuePublisher(
            delegate = delegate,
            baseDelay = 200.milliseconds,
            maxTotalDelay = 500.milliseconds,
            delayFn = { recordedDelays.add(it) },
        )

        assertThrows<IOException> {
            retrying.publishToDeadLetter(sampleEvent)
        }

        assertEquals(listOf(200.milliseconds, 300.milliseconds), recordedDelays)
    }

    @Test
    fun `bounds hanging delegate publish by maxTotalDelay timeout`() = runTest {
        val hangingDelegate = object : JobQueuePublisher {
            override suspend fun publish(event: RenovateJobEvent) {
                delay(10.seconds)
            }

            override suspend fun publishToDeadLetter(event: RenovateJobEvent) = Unit
        }

        val retrying = RetryingJobQueuePublisher(
            delegate = hangingDelegate,
            baseDelay = 50.milliseconds,
            maxTotalDelay = 150.milliseconds,
        )

        assertThrows<TimeoutCancellationException> {
            retrying.publish(sampleEvent)
        }
    }

    @Test
    fun `does not retry CancellationException or IllegalArgumentException`() = runTest {
        val delegate = mockk<JobQueuePublisher>()
        coEvery { delegate.publish(sampleEvent) } throws CancellationException("cancelled")

        val retrying = RetryingJobQueuePublisher(
            delegate = delegate,
            baseDelay = 10.milliseconds,
            maxTotalDelay = 1.seconds,
        )

        assertThrows<CancellationException> {
            retrying.publish(sampleEvent)
        }
        coVerify(exactly = 1) { delegate.publish(sampleEvent) }

        val badDelegate = mockk<JobQueuePublisher>()
        coEvery { badDelegate.publish(sampleEvent) } throws IllegalArgumentException("bad argument")
        val retryingBad = RetryingJobQueuePublisher(
            delegate = badDelegate,
            baseDelay = 10.milliseconds,
            maxTotalDelay = 1.seconds,
        )
        assertThrows<IllegalArgumentException> {
            retryingBad.publish(sampleEvent)
        }
        coVerify(exactly = 1) { badDelegate.publish(sampleEvent) }
    }

    @Test
    fun `rejects negative or zero baseDelay when maxTotalDelay is positive`() {
        val delegate = mockk<JobQueuePublisher>()
        assertThrows<IllegalArgumentException> {
            RetryingJobQueuePublisher(delegate, baseDelay = (-1).milliseconds)
        }
        assertThrows<IllegalArgumentException> {
            RetryingJobQueuePublisher(delegate, maxTotalDelay = (-1).milliseconds)
        }
        assertThrows<IllegalArgumentException> {
            RetryingJobQueuePublisher(delegate, baseDelay = Duration.ZERO, maxTotalDelay = 1.seconds)
        }
    }
}
