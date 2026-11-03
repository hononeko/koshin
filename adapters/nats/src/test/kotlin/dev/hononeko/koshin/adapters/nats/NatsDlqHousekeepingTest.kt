package dev.hononeko.koshin.adapters.nats

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.nats.client.Connection
import io.nats.client.Dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.job
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.time.Duration

class NatsDlqHousekeepingTest {
    private val dispatcher = mockk<Dispatcher>(relaxed = true)
    private val connection = mockk<Connection>(relaxed = true) {
        every { createDispatcher(any()) } returns dispatcher
    }
    private val scope = CoroutineScope(SupervisorJob())

    @Test
    fun `subscribes the forwarder and starts the sweeper, and closing stops both`() {
        val housekeeping = NatsDlqHousekeeping.start(connection, NatsConfig(), scope)

        verify { dispatcher.subscribe("\$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.RENOVATE_JOBS.*", any<String>()) }
        assertEquals(1, scope.coroutineContext.job.children.count(), "the sweeper")

        housekeeping.close()

        verify { connection.closeDispatcher(dispatcher) }
        assertTrue(scope.coroutineContext.job.children.none { it.isActive })
        scope.cancel()
    }

    @Test
    fun `a zero sweep interval runs the forwarder alone`() {
        NatsDlqHousekeeping.start(connection, NatsConfig(exhaustedSweepInterval = Duration.ZERO), scope).close()

        assertEquals(0, scope.coroutineContext.job.children.count())
        verify { connection.closeDispatcher(dispatcher) }
        scope.cancel()
    }
}
