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
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.concurrent.atomic.AtomicInteger
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
        val useCase = mockk<ProcessRenovateJobUseCase>()
        coEvery { useCase.processNext(any()) } coAnswers {
            Thread.sleep(1_000)
            false
        }
        val daemon = WorkerDaemon(platforms) { useCase }
        daemon.start()
        Thread.sleep(50)

        assertFalse(daemon.stop(timeout = 50.milliseconds))
    }

    @Test
    fun `stop before start is a no-op`() {
        val useCase = mockk<ProcessRenovateJobUseCase>()
        assertTrue(WorkerDaemon(platforms) { useCase }.stop())
        coVerify(exactly = 0) { useCase.processNext(any()) }
    }
}
