package dev.hononeko.koshin.worker

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.usecase.ProcessRenovateJobUseCase
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.job
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.IOException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class WorkerDaemonTest {

    private val platforms = setOf(Platform.GITHUB, Platform.GITLAB)

    @Test
    fun `keeps polling after idle iterations and failures`() = runBlocking {
        val calls = AtomicInteger()
        val reachedFive = CompletableDeferred<Unit>()
        val useCase = mockk<ProcessRenovateJobUseCase>()
        coEvery { useCase.processNext(platforms) } answers {
            val n = calls.incrementAndGet()
            if (n >= 5) reachedFive.complete(Unit)
            when (n % 3) {
                0 -> throw IOException("valkey blip")
                1 -> false
                else -> true
            }
        }
        val daemon = WorkerDaemon(platforms, idleDelay = 1.milliseconds, errorDelay = 1.milliseconds) { useCase }

        daemon.start()
        daemon.start()
        withTimeout(5.seconds) { reachedFive.await() }

        assertTrue(daemon.stop())
        val afterStop = calls.get()
        delay(50)
        assertTrue(calls.get() == afterStop)
    }

    @Test
    fun `stop cancels the in-flight job and waits for it`() {
        val started = CompletableDeferred<Unit>()
        val cleanedUp = CompletableDeferred<Unit>()
        val useCase = mockk<ProcessRenovateJobUseCase>()
        coEvery { useCase.processNext(any()) } coAnswers {
            currentCoroutineContext().job.invokeOnCompletion { cleanedUp.complete(Unit) }
            started.complete(Unit)
            awaitCancellation()
        }
        val daemon = WorkerDaemon(platforms) { useCase }

        daemon.start()
        runBlocking { withTimeout(5.seconds) { started.await() } }

        assertTrue(daemon.stop())
        assertTrue(cleanedUp.isCompleted)
    }

    @Test
    fun `stop reports a job that does not finish in time`() {
        val started = CountDownLatch(1)
        val useCase = mockk<ProcessRenovateJobUseCase>()
        coEvery { useCase.processNext(any()) } coAnswers {
            started.countDown()
            Thread.sleep(1_000)
            false
        }
        val daemon = WorkerDaemon(platforms) { useCase }
        daemon.start()
        assertTrue(started.await(5, TimeUnit.SECONDS))

        assertFalse(daemon.stop(timeout = 50.milliseconds))
    }

    @Test
    fun `stop before start is a no-op`() {
        val useCase = mockk<ProcessRenovateJobUseCase>()
        assertTrue(WorkerDaemon(platforms) { useCase }.stop())
        coVerify(exactly = 0) { useCase.processNext(any()) }
    }

    /** A use case that finds a job [jobs] times, then none; [processed] counts the jobs. */
    private fun drainingUseCase(jobs: Int, processed: AtomicInteger, runTime: Duration = Duration.ZERO) =
        mockk<ProcessRenovateJobUseCase>().also { useCase ->
            coEvery { useCase.processNext(platforms) } coAnswers {
                if (processed.get() < jobs) {
                    delay(runTime)
                    processed.incrementAndGet()
                    true
                } else {
                    false
                }
            }
        }

    @Test
    fun `with an idle timeout it drains the queued jobs back to back, then finishes`() {
        val processed = AtomicInteger()
        val finished = CountDownLatch(1)
        val useCase = drainingUseCase(jobs = 3, processed)
        val daemon = WorkerDaemon(
            platforms,
            idleDelay = 10.milliseconds,
            idleTimeout = 100.milliseconds,
            onFinished = finished::countDown,
        ) { useCase }

        daemon.start()

        assertTrue(finished.await(5, TimeUnit.SECONDS))
        assertEquals(3, processed.get())
        assertTrue(daemon.stop())
    }

    @Test
    fun `with an idle timeout it finishes when every queued job is in a cooled-down scope`() {
        // processNext skips scopes in cooldown, so it finds nothing to run
        val useCase = mockk<ProcessRenovateJobUseCase>()
        coEvery { useCase.processNext(platforms) } returns false
        val finished = CountDownLatch(1)
        val daemon = WorkerDaemon(
            platforms,
            idleDelay = 10.milliseconds,
            idleTimeout = 100.milliseconds,
            onFinished = finished::countDown,
        ) { useCase }

        daemon.start()

        assertTrue(finished.await(5, TimeUnit.SECONDS))
    }

    @Test
    fun `failing iterations count as idle`() {
        val useCase = mockk<ProcessRenovateJobUseCase>()
        coEvery { useCase.processNext(platforms) } throws IOException("NATS down")
        val finished = CountDownLatch(1)
        val daemon = WorkerDaemon(
            platforms,
            errorDelay = 10.milliseconds,
            idleTimeout = 100.milliseconds,
            onFinished = finished::countDown,
        ) { useCase }

        daemon.start()

        assertTrue(finished.await(5, TimeUnit.SECONDS))
    }

    @Test
    fun `past its max lifetime it finishes the job in flight and fetches no other`() {
        val processed = AtomicInteger()
        val finished = CountDownLatch(1)
        // Every job takes longer than the max lifetime
        val useCase = drainingUseCase(jobs = 5, processed, runTime = 200.milliseconds)
        val daemon = WorkerDaemon(
            platforms,
            idleDelay = 10.milliseconds,
            idleTimeout = 10.seconds,
            maxLifetime = 100.milliseconds,
            onFinished = finished::countDown,
        ) { useCase }

        daemon.start()

        assertTrue(finished.await(5, TimeUnit.SECONDS))
        assertEquals(1, processed.get())
    }

    @Test
    fun `without an idle timeout it keeps running across idle periods`() {
        val processed = AtomicInteger()
        val finished = CountDownLatch(1)
        val useCase = drainingUseCase(jobs = 1, processed)
        val daemon = WorkerDaemon(
            platforms,
            idleDelay = 10.milliseconds,
            onFinished = finished::countDown,
        ) { useCase }

        daemon.start()

        assertFalse(finished.await(300, TimeUnit.MILLISECONDS))
        assertEquals(1, processed.get())
        coVerify(atLeast = 5) { useCase.processNext(platforms) }
        assertTrue(daemon.stop())
        assertEquals(1L, finished.count, "stopping isn't finishing")
    }

    @Test
    fun `with an idle timeout it finishes when it can't connect`() {
        val finished = CountDownLatch(1)
        val daemon = WorkerDaemon(platforms, idleTimeout = 100.milliseconds, onFinished = finished::countDown) {
            awaitCancellation()
        }

        daemon.start()

        assertTrue(finished.await(5, TimeUnit.SECONDS))
    }

    @Test
    fun `rejects a negative idle timeout or max lifetime`() {
        val useCase = mockk<ProcessRenovateJobUseCase>()
        assertThrows<IllegalArgumentException> { WorkerDaemon(platforms, idleTimeout = (-1).seconds) { useCase } }
        assertThrows<IllegalArgumentException> { WorkerDaemon(platforms, maxLifetime = (-1).seconds) { useCase } }
    }
}
