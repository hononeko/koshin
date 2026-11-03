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
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.IOException
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
        var calls = 0
        val job = NatsHousekeeping.periodically(this, 1.minutes) {
            calls++
            if (calls == 1) throw IllegalStateException(CancellationException("not a real cancellation"))
        }
        advanceTimeBy(3.minutes + 2.seconds)
        job.cancel()

        assertEquals(3, calls, "the first pass failed, the loop kept going")
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
