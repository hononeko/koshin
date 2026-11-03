package dev.hononeko.koshin.proxy

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.model.TriggerReason
import dev.hononeko.koshin.core.ports.BurstWorkerPort
import dev.hononeko.koshin.core.ports.JobQueuePublisher
import dev.hononeko.koshin.core.ports.LeaderLeasePort
import dev.hononeko.koshin.core.ports.PlatformThrottlePort
import dev.hononeko.koshin.core.ports.QueueDepth
import dev.hononeko.koshin.core.ports.QueueDepthPort
import dev.hononeko.koshin.core.ports.WarmWorkerPort
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class WorkerDispatcherTest {
    /** One lease shared by every replica, on a clock the test moves. */
    private class Lease : LeaderLeasePort {
        val now = AtomicLong()
        var holder: String? = null
        var expiresAt = 0L

        override suspend fun acquireOrRenew(holderId: String, ttl: Duration): Boolean = synchronized(this) {
            if (holder == null || holder == holderId || now.get() >= expiresAt) {
                holder = holderId
                expiresAt = now.get() + ttl.inWholeMilliseconds
                true
            } else {
                false
            }
        }

        override suspend fun release(holderId: String) = synchronized(this) {
            if (holder == holderId) holder = null
        }
    }

    private class Jobs : BurstWorkerPort {
        val started = AtomicInteger()

        override suspend fun active() = started.get()

        override suspend fun start(count: Int) {
            started.addAndGet(count)
        }
    }

    private val lease = Lease()
    private val throttle = mockk<PlatformThrottlePort> {
        coEvery { activeRunSlots(any()) } returns 0
        coEvery { cooldownRemaining(any()) } returns null
    }
    private val warmWorkers = mockk<WarmWorkerPort> { coEvery { liveCount() } returns 0 }
    private val pending = AtomicLong()
    private val queue = object : QueueDepthPort {
        override suspend fun depth(platform: Platform) = QueueDepth(pending.get(), 0)
    }
    private val jobs = Jobs()
    private val ports = ProxyValkey.DispatchPorts(lease, throttle, warmWorkers)
    private val config = DispatcherConfig(enabled = true, maxWorkers = 6, interval = 5.seconds)

    private fun dispatcher(id: String, valkey: () -> ProxyValkey.DispatchPorts? = { ports }) =
        WorkerDispatcher(config, setOf(Platform.GITHUB), valkey, queue, jobs, holderId = id)

    @Test
    fun `only the lease holder starts Jobs, and the other takes over once the lease expires`() = runTest {
        val a = dispatcher("proxy-a")
        val b = dispatcher("proxy-b")
        pending.set(2)

        assertEquals(2, a.reconcileOnce()?.started)
        assertNull(b.reconcileOnce(), "proxy-b doesn't lead")
        assertTrue(a.isLeader)
        assertFalse(b.isLeader)

        // proxy-a stops renewing (e.g. it died): past the lease TTL, proxy-b leads
        lease.now.addAndGet(config.leaseTtl.inWholeMilliseconds)
        pending.set(3)
        assertEquals(1, b.reconcileOnce()?.started)
        assertTrue(b.isLeader)
        assertNull(a.reconcileOnce())
        assertFalse(a.isLeader)
        assertEquals(3, jobs.started.get())
    }

    @Test
    fun `stopping hands the lease over at once`() = runTest {
        val a = dispatcher("proxy-a")
        val b = dispatcher("proxy-b")
        a.reconcileOnce()

        a.stop()
        b.stop()

        assertFalse(a.isLeader)
        assertTrue(b.reconcileOnce() != null)
    }

    @Test
    fun `without Valkey nothing is dispatched`() = runTest {
        val disconnected = dispatcher("proxy-a") { null }
        pending.set(1)

        assertNull(disconnected.reconcileOnce())
        disconnected.stop()
        assertEquals(0, jobs.started.get())
    }

    @Test
    fun `a publish wakes the dispatcher well before the next tick`() = runBlocking {
        val slow = DispatcherConfig(enabled = true, interval = 1.minutes)
        val dispatcher = WorkerDispatcher(slow, setOf(Platform.GITHUB), { ports }, queue, jobs, holderId = "a")
        val scope = CoroutineScope(SupervisorJob())
        try {
            dispatcher.start(scope)
            delay(100.milliseconds)
            pending.set(1)
            val publisher = mockk<JobQueuePublisher>(relaxed = true)
            val event = RenovateJobEvent(RepoRef(Platform.GITHUB, "o/r"), "d", TriggerReason.WEBHOOK)
            WakingPublisher(publisher, dispatcher::wake).publish(event)

            val deadline = System.nanoTime() + 5.seconds.inWholeNanoseconds
            while (jobs.started.get() == 0 && System.nanoTime() < deadline) delay(10.milliseconds)
            assertEquals(1, jobs.started.get())
            coVerify(exactly = 1) { publisher.publish(event) }
        } finally {
            scope.cancel()
        }
    }

    @Test
    fun `a failing reconciliation is retried on the next tick`() = runBlocking {
        val failing = mockk<QueueDepthPort>()
        coEvery { failing.depth(any()) } throws IllegalStateException("NATS down") andThen QueueDepth(1, 0)
        val fast = DispatcherConfig(enabled = true, interval = 20.milliseconds)
        val dispatcher = WorkerDispatcher(fast, setOf(Platform.GITHUB), { ports }, failing, jobs, holderId = "a")
        val scope = CoroutineScope(SupervisorJob())
        try {
            dispatcher.start(scope)
            val deadline = System.nanoTime() + 5.seconds.inWholeNanoseconds
            while (jobs.started.get() == 0 && System.nanoTime() < deadline) delay(10.milliseconds)
            assertEquals(1, jobs.started.get())
        } finally {
            scope.cancel()
        }
    }

    @Test
    fun `an unreadable warm count falls back to MIN_WARM_WORKERS`() = runTest {
        coEvery { warmWorkers.liveCount() } throws IllegalStateException("Valkey down")
        val warmFallback = DispatcherConfig(enabled = true, minWarmWorkers = 1)
        val dispatcher = WorkerDispatcher(warmFallback, setOf(Platform.GITHUB), { ports }, queue, jobs, holderId = "a")
        pending.set(1)

        assertEquals(0, dispatcher.reconcileOnce()?.started)
    }

    @Test
    fun `dispatcher settings are validated and the lease outlives two missed renewals`() {
        assertEquals(15.seconds, DispatcherConfig().leaseTtl)
        assertThrows<IllegalArgumentException> { DispatcherConfig(podTemplateName = " ") }
        assertThrows<IllegalArgumentException> { DispatcherConfig(namespace = " ") }
        assertThrows<IllegalArgumentException> { DispatcherConfig(maxWorkers = 0) }
        assertThrows<IllegalArgumentException> { DispatcherConfig(maxRunsPerPlatform = 0) }
        assertThrows<IllegalArgumentException> { DispatcherConfig(minWarmWorkers = 4) }
        assertThrows<IllegalArgumentException> { DispatcherConfig(interval = Duration.ZERO) }
        assertThrows<IllegalArgumentException> { DispatcherConfig(jobActiveDeadline = Duration.ZERO) }
        assertThrows<IllegalArgumentException> { DispatcherConfig(jobTtlAfterFinished = (-1).hours) }
    }
}
