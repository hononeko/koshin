package dev.hononeko.koshin.core.usecase

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.ports.BurstWorkerPort
import dev.hononeko.koshin.core.ports.PlatformThrottlePort
import dev.hononeko.koshin.core.ports.QueueDepth
import dev.hononeko.koshin.core.ports.QueueDepthPort
import dev.hononeko.koshin.core.ports.WarmWorkerPort
import io.mockk.coEvery
import io.mockk.mockk
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.seconds

class DispatchWorkersUseCaseTest {
    private val github = Platform.GITHUB
    private val gitlab = Platform.GITLAB

    private class Queue(val depths: MutableMap<Platform, QueueDepth> = mutableMapOf()) : QueueDepthPort {
        override suspend fun depth(platform: Platform) = depths[platform] ?: QueueDepth(0, 0)
    }

    private class BurstWorkers(var active: Int = 0) : BurstWorkerPort {
        var activeCalls = 0
        val started = mutableListOf<Int>()

        override suspend fun active(): Int = active.also { activeCalls++ }

        override suspend fun start(count: Int) {
            started += count
            active += count
        }
    }

    private val queue = Queue()
    private val throttle = mockk<PlatformThrottlePort>()
    private val warmWorkers = mockk<WarmWorkerPort>()
    private val burst = BurstWorkers()
    private val warmFailures = mutableListOf<Throwable>()

    private fun useCase(
        platforms: Set<Platform> = setOf(github),
        maxWorkers: Int = 6,
        maxRunsPerScope: Int = 3,
        minWarmWorkers: Int = 0,
    ) = DispatchWorkersUseCase(
        queue,
        throttle,
        warmWorkers,
        burst,
        DispatchConfig(platforms, maxWorkers, maxRunsPerScope, minWarmWorkers),
    ) { warmFailures += it }

    private fun state(
        platform: Platform = github,
        pending: Long = 0,
        ackPending: Long = 0,
        slots: Int = 0,
        cooldown: Boolean = false,
    ) {
        queue.depths[platform] = QueueDepth(pending, ackPending)
        coEvery { throttle.activeRunSlots(platform) } returns slots
        coEvery { throttle.cooldownRemaining(platform) } returns if (cooldown) 30.seconds else null
    }

    private fun warm(count: Int) {
        coEvery { warmWorkers.liveCount() } returns count
    }

    @Test
    fun `scale to zero - one runnable job starts one burst worker`() = runTest {
        state(pending = 1)
        warm(0)

        val result = useCase().reconcile()

        assertEquals(DispatchResult(desiredWorkers = 1, warmWorkers = 0, desiredBurst = 1, started = 1), result)
        assertEquals(listOf(1), burst.started)
    }

    @Test
    fun `one warm worker - a single job needs no burst worker, three need two`() = runTest {
        warm(1)
        state(pending = 1)
        assertEquals(0, useCase().reconcile().started)
        assertEquals(0, burst.activeCalls, "nothing to start: the cluster isn't asked")

        state(pending = 3)
        assertEquals(2, useCase().reconcile().started)
    }

    @Test
    fun `while the warm worker is unavailable a single job gets a burst worker`() = runTest {
        warm(0)
        state(pending = 1)

        assertEquals(1, useCase(minWarmWorkers = 1).reconcile().started)
    }

    @Test
    fun `an unreadable warm count falls back to MIN_WARM_WORKERS`() = runTest {
        coEvery { warmWorkers.liveCount() } throws IllegalStateException("Valkey down")
        state(pending = 1)

        assertEquals(0, useCase(minWarmWorkers = 1).reconcile().started)
        assertEquals(1, warmFailures.size)
    }

    @Test
    fun `an idle worker waiting on a fetch holds a slot but isn't running a job`() = runTest {
        warm(1)
        state(pending = 1, ackPending = 0, slots = 1)

        val result = useCase().reconcile()

        assertEquals(1, result.desiredWorkers)
        assertEquals(0, result.started)
    }

    @Test
    fun `a busy warm worker and one more job start one burst worker`() = runTest {
        warm(1)
        state(pending = 1, ackPending = 1, slots = 1)

        assertEquals(1, useCase().reconcile().started)
    }

    @Test
    fun `a delivery NAKed for a later retry still needs a worker`() = runTest {
        warm(0)
        state(pending = 0, ackPending = 2, slots = 0)

        assertEquals(2, useCase().reconcile().started)
    }

    @Test
    fun `a scope in cooldown adds no demand, but its running jobs still count`() = runTest {
        warm(0)
        state(github, pending = 5, ackPending = 1, slots = 1, cooldown = true)
        state(gitlab, pending = 1)

        val result = useCase(setOf(github, gitlab)).reconcile()

        assertEquals(2, result.desiredWorkers, "GitHub's running job + GitLab's pending one")
        assertEquals(2, result.started)
    }

    @Test
    fun `run slots per scope and max workers cap the demand`() = runTest {
        warm(0)
        state(github, pending = 10)
        state(gitlab, pending = 10, ackPending = 2, slots = 2)

        val result = useCase(setOf(github, gitlab), maxWorkers = 5, maxRunsPerScope = 3).reconcile()

        assertEquals(5, result.desiredWorkers, "min(5, 2 running + 3 GitHub + 1 GitLab)")
        assertEquals(5, result.desiredBurst)
    }

    @Test
    fun `burst workers already started count, and none is ever stopped`() = runTest {
        warm(0)
        state(pending = 3)
        burst.active = 2
        assertEquals(1, useCase().reconcile().started)

        burst.active = 5
        assertEquals(0, useCase().reconcile().started)
        assertEquals(listOf(1), burst.started)
    }

    @Test
    fun `more warm workers than demand starts nothing`() = runTest {
        warm(4)
        state(pending = 2)

        assertEquals(0, useCase(maxWorkers = 3).reconcile().desiredBurst)
        assertEquals(0, burst.activeCalls)
    }

    @Test
    fun `invalid configurations and depths are rejected`() {
        assertThrows(IllegalArgumentException::class.java) { DispatchConfig(emptySet(), 1, 1) }
        assertThrows(IllegalArgumentException::class.java) { DispatchConfig(setOf(github), 0, 1) }
        assertThrows(IllegalArgumentException::class.java) { DispatchConfig(setOf(github), 1, 0) }
        assertThrows(IllegalArgumentException::class.java) { DispatchConfig(setOf(github), 1, 1, minWarmWorkers = 2) }
        assertThrows(IllegalArgumentException::class.java) { QueueDepth(-1, 0) }
    }
}
