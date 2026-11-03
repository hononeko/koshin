package dev.hononeko.koshin.worker

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.ports.WarmWorkerPort
import dev.hononeko.koshin.core.usecase.ProcessRenovateJobUseCase
import io.mockk.coEvery
import io.mockk.mockk
import kotlinx.coroutines.awaitCancellation
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/** Drain-then-exit and the warm-worker lease (KSH-31). */
class WorkerLifecycleTest {
    private val platforms = setOf(Platform.GITHUB)

    private class RecordingLeases : WarmWorkerPort {
        val renewals = CopyOnWriteArrayList<Pair<String, Duration>>()
        val removals = CopyOnWriteArrayList<String>()

        override suspend fun renew(workerId: String, ttl: Duration) {
            renewals += workerId to ttl
        }

        override suspend fun remove(workerId: String) {
            removals += workerId
        }

        override suspend fun liveCount(): Int = renewals.map { it.first }.toSet().size - removals.size
    }

    private fun awaitTrue(what: String, condition: () -> Boolean) {
        val deadline = System.nanoTime() + 5.seconds.inWholeNanoseconds
        while (!condition()) {
            check(System.nanoTime() < deadline) { "Timed out waiting for $what" }
            Thread.sleep(10)
        }
    }

    @Test
    fun `the warm lease is renewed every interval until stopped, then dropped`() {
        val leases = RecordingLeases()
        val presence = WarmPresence(interval = 20.milliseconds, ttl = 60.milliseconds, workerId = "worker-a") { leases }

        presence.start()
        presence.start()
        awaitTrue("three renewals") { leases.renewals.size >= 3 }
        presence.stop()

        assertTrue(leases.renewals.all { it == "worker-a" to 60.milliseconds })
        assertEquals(listOf("worker-a"), leases.removals)
        val renewed = leases.renewals.size
        Thread.sleep(60)
        assertEquals(renewed, leases.renewals.size, "no renewal after stop")
    }

    @Test
    fun `renewal failures are retried, and stopping before connecting drops nothing`() {
        val failing = mockk<WarmWorkerPort>()
        var attempts = 0
        coEvery { failing.renew(any(), any()) } answers {
            attempts++
            error("Valkey down")
        }
        val presence = WarmPresence(interval = 10.milliseconds, ttl = 30.milliseconds) { failing }
        presence.start()
        awaitTrue("a retried renewal") { attempts >= 2 }
        coEvery { failing.remove(any()) } throws IllegalStateException("still down")
        presence.stop()

        val neverConnected = WarmPresence(interval = 10.milliseconds, ttl = 30.milliseconds) { awaitCancellation() }
        neverConnected.start()
        neverConnected.stop()
        WarmPresence(interval = 10.milliseconds, ttl = 30.milliseconds) { awaitCancellation() }.stop()
    }

    @Test
    fun `the lease must outlive its renewal interval`() {
        assertThrows<IllegalArgumentException> { WarmPresence(interval = 15.seconds, ttl = 15.seconds) { mockk() } }
    }

    @Test
    fun `a drained daemon stops the server and reports that it finished`() {
        val useCase = mockk<ProcessRenovateJobUseCase>()
        coEvery { useCase.processNext(platforms) } returns false
        val leases = RecordingLeases()
        val stoppedHooks = CopyOnWriteArrayList<String>()

        val finished = serveWorker(
            settings = WorkerSettings(port = 0),
            readinessCheck = { true },
            presence = WarmPresence(10.milliseconds, 30.milliseconds, workerId = "w") { leases },
            onStopped = { stoppedHooks += "backends closed" },
        ) { onFinished ->
            WorkerDaemon(
                platforms,
                idleDelay = 10.milliseconds,
                idleTimeout = 100.milliseconds,
                onFinished = onFinished,
            ) { useCase }
        }

        assertTrue(finished)
        assertEquals(listOf("backends closed"), stoppedHooks)
        assertEquals(listOf("w"), leases.removals)
    }
}
