package dev.hononeko.koshin.adapters.nats

import io.nats.client.Connection
import io.nats.client.ConnectionListener
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Re-runs [bootstrap] after every reconnect. A NATS server on ephemeral storage (the `minimal` profile) comes back
 * without streams or consumers, and nothing else would recreate them.
 */
class NatsReconnectBootstrapper(
    private val scope: CoroutineScope,
    private val retryDelay: Duration = 1.seconds,
    private val maxAttempts: Int = DEFAULT_MAX_ATTEMPTS,
    private val bootstrap: (Connection) -> Unit,
) : ConnectionListener {

    private val logger = LoggerFactory.getLogger(NatsReconnectBootstrapper::class.java)

    @Deprecated("jnats calls the four-argument overload", ReplaceWith("connectionEvent(conn, type, null, null)"))
    override fun connectionEvent(conn: Connection, type: ConnectionListener.Events) =
        connectionEvent(conn, type, null, null)

    override fun connectionEvent(conn: Connection, type: ConnectionListener.Events, time: Long?, uriDetails: String?) {
        if (type != ConnectionListener.Events.RECONNECTED) return
        scope.launch {
            // JetStream may still be starting right after the server accepts connections
            for (attempt in 1..maxAttempts) {
                try {
                    bootstrap(conn)
                    logger.info("Re-ensured JetStream streams and consumers after reconnect")
                    return@launch
                } catch (e: CancellationException) {
                    throw e
                } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
                    logger.warn("JetStream bootstrap after reconnect failed (attempt {}/{})", attempt, maxAttempts, e)
                    delay(retryDelay)
                }
            }
            logger.error("Giving up re-ensuring JetStream streams and consumers after reconnect")
        }
    }

    companion object {
        const val DEFAULT_MAX_ATTEMPTS = 30
    }
}
