package dev.hononeko.koshin.adapters.nats

import dev.hononeko.koshin.core.model.Platform
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.nats.client.Connection
import io.nats.client.Dispatcher
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.job
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.IOException
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

@OptIn(ExperimentalCoroutinesApi::class)
class NatsHousekeepingTest {
    private val dispatcher = mockk<Dispatcher>(relaxed = true)
    private val connection = mockk<Connection>(relaxed = true) {
        every { createDispatcher(any()) } returns dispatcher
    }
    private val scope = CoroutineScope(SupervisorJob())

    @Test
    fun `subscribes the forwarder and starts the sweeper, and closing stops both`() {
        val housekeeping = NatsHousekeeping.start(connection, NatsConfig(), scope)

        verify { dispatcher.subscribe("\$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.RENOVATE_JOBS.*", any<String>()) }
        assertEquals(1, scope.coroutineContext.job.children.count(), "the periodic pass")

        housekeeping.close()

        verify { connection.closeDispatcher(dispatcher) }
        assertTrue(scope.coroutineContext.job.children.none { it.isActive })
        scope.cancel()
    }

    @Test
    fun `the periodic pass runs on an interval and survives a failed pass`() = runTest {
        val passes = mutableListOf<Long>()
        val job = NatsHousekeeping.periodically(this, 1.minutes, firstDelay = 10.seconds) {
            passes += currentTime
            if (passes.size == 1) throw IllegalStateException(CancellationException("not a real cancellation"))
        }
        // Halfway between passes, so when exactly advanceTimeBy stops doesn't matter
        advanceTimeBy(2.minutes + 40.seconds)
        job.cancel()

        assertEquals(listOf(10_000L, 70_000L, 130_000L), passes, "the first pass failed, the loop kept going")
    }

    @Test
    fun `the first pass starts at a random point in the first interval`() {
        val random = Random(seed = 2026)
        val delays = List(1_000) { NatsHousekeeping.firstDelay(1.minutes, random) }

        assertTrue(delays.all { it >= Duration.ZERO && it < 1.minutes }, "within the first interval")
        assertTrue(delays.toSet().size > 1, "not one fixed point, so replicas spread out")
    }

    @Test
    fun `each consumer upkeep step runs even when the one before failed`() {
        val consumers = mockk<NatsScopeConsumers> {
            every { migrateLegacy(any()) } throws IOException("NATS down")
            every { collectIdle() } throws IllegalStateException("odd")
            every { ensureForStoredJobs() } returns 1
        }

        NatsHousekeeping.tidy(consumers, listOf(Platform.GITHUB))

        verify { consumers.migrateLegacy(listOf(Platform.GITHUB)) }
        verify { consumers.collectIdle() }
        verify { consumers.ensureForStoredJobs() }
    }

    @Test
    fun `a zero sweep interval runs the forwarder alone`() {
        NatsHousekeeping.start(connection, NatsConfig(exhaustedSweepInterval = Duration.ZERO), scope).close()

        assertEquals(0, scope.coroutineContext.job.children.count())
        verify { connection.closeDispatcher(dispatcher) }
        scope.cancel()
    }
}
