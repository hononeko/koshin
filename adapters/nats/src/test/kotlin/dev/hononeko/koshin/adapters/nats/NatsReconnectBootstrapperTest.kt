package dev.hononeko.koshin.adapters.nats

import io.mockk.mockk
import io.nats.client.Connection
import io.nats.client.ConnectionListener
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceUntilIdle
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.io.IOException
import kotlin.time.Duration.Companion.seconds

@OptIn(ExperimentalCoroutinesApi::class)
class NatsReconnectBootstrapperTest {

    private val connection = mockk<Connection>()

    @Test
    fun `bootstraps again after a reconnect, retrying until JetStream is back`() {
        val scope = TestScope()
        var attempts = 0
        val listener = NatsReconnectBootstrapper(scope, retryDelay = 1.seconds) {
            attempts++
            if (attempts < 3) throw IOException("JetStream not ready")
        }

        listener.connectionEvent(connection, ConnectionListener.Events.RECONNECTED, null, null)
        scope.advanceUntilIdle()

        assertEquals(3, attempts)
    }

    @Test
    fun `ignores other connection events`() {
        val scope = TestScope()
        var attempts = 0
        val listener = NatsReconnectBootstrapper(scope) { attempts++ }

        ConnectionListener.Events.entries
            .filter { it != ConnectionListener.Events.RECONNECTED }
            .forEach { listener.connectionEvent(connection, it, null, null) }
        scope.advanceUntilIdle()

        assertEquals(0, attempts)
    }

    @Test
    fun `gives up after max attempts`() {
        val scope = TestScope()
        var attempts = 0
        val listener = NatsReconnectBootstrapper(scope, maxAttempts = 4) {
            attempts++
            throw IOException("still down")
        }

        listener.connectionEvent(connection, ConnectionListener.Events.RECONNECTED, null, null)
        scope.advanceUntilIdle()

        assertEquals(4, attempts)
    }
}
