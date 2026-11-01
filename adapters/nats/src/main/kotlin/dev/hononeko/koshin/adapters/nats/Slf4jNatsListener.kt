package dev.hononeko.koshin.adapters.nats

import io.nats.client.Connection
import io.nats.client.ConnectionListener
import io.nats.client.Consumer
import io.nats.client.ErrorListener
import io.nats.client.JetStreamSubscription
import io.nats.client.Message
import io.nats.client.Options
import io.nats.client.support.Status
import org.slf4j.LoggerFactory

/**
 * Logs the NATS client's connection events and errors through SLF4J. jnats' default listener writes them with
 * `java.util.logging`, as plain lines that bypass the JSON log encoder. Connection events are passed on to [delegate],
 * since a connection takes only one [ConnectionListener].
 */
class Slf4jNatsListener(private val delegate: ConnectionListener? = null) :
    ErrorListener,
    ConnectionListener {

    private val logger = LoggerFactory.getLogger("dev.hononeko.koshin.nats")

    @Deprecated("jnats calls the four-argument overload", ReplaceWith("connectionEvent(conn, type, null, null)"))
    override fun connectionEvent(conn: Connection, type: ConnectionListener.Events) =
        connectionEvent(conn, type, null, null)

    override fun connectionEvent(conn: Connection, type: ConnectionListener.Events, time: Long?, uriDetails: String?) {
        val event = when (type) {
            ConnectionListener.Events.DISCONNECTED, ConnectionListener.Events.LAME_DUCK -> logger.atWarn()

            // CLOSED follows every failed connect attempt, and koshin logs its own shutdown
            ConnectionListener.Events.CLOSED,
            ConnectionListener.Events.RESUBSCRIBED,
            ConnectionListener.Events.DISCOVERED_SERVERS,
            -> logger.atDebug()

            else -> logger.atInfo()
        }
        event.addKeyValue("event", type.name)
            .addKeyValue("server", uriDetails ?: conn.connectedUrl)
            .log("NATS connection {}", type.event)
        delegate?.connectionEvent(conn, type, time, uriDetails)
    }

    override fun errorOccurred(conn: Connection, error: String) {
        logger.atWarn().addKeyValue("error", error).log("NATS server error: {}", error)
    }

    /** Fires on every failed (re)connect attempt, so the stack trace only goes to debug. */
    override fun exceptionOccurred(conn: Connection, exp: Exception) {
        logger.atWarn().addKeyValue("exception", exp.javaClass.name).log("NATS connection error: {}", exp.message)
        logger.debug("NATS connection error", exp)
    }

    override fun slowConsumerDetected(conn: Connection, consumer: Consumer) {
        logger.atWarn().addKeyValue("pendingMessages", consumer.pendingMessageCount)
            .log("NATS slow consumer: messages are being dropped")
    }

    override fun messageDiscarded(conn: Connection, msg: Message) {
        logger.atWarn().addKeyValue("subject", msg.subject).log("NATS discarded a message")
    }

    override fun heartbeatAlarm(
        conn: Connection,
        sub: JetStreamSubscription,
        lastStreamSequence: Long,
        lastConsumerSequence: Long,
    ) {
        logger.atWarn().addKeyValue("lastStreamSequence", lastStreamSequence)
            .addKeyValue("lastConsumerSequence", lastConsumerSequence)
            .log("NATS consumer heartbeat missed")
    }

    override fun unhandledStatus(conn: Connection, sub: JetStreamSubscription, status: Status) {
        logger.atWarn().addKeyValue("status", status.code).log("NATS unhandled status: {}", status.message)
    }

    override fun pullStatusWarning(conn: Connection, sub: JetStreamSubscription, status: Status) {
        logger.atDebug().addKeyValue("status", status.code).log("NATS pull status: {}", status.message)
    }

    override fun pullStatusError(conn: Connection, sub: JetStreamSubscription, status: Status) {
        logger.atWarn().addKeyValue("status", status.code).log("NATS pull error: {}", status.message)
    }

    override fun flowControlProcessed(
        conn: Connection,
        sub: JetStreamSubscription,
        subject: String,
        source: ErrorListener.FlowControlSource,
    ) {
        logger.atDebug().addKeyValue("subject", subject).log("NATS flow control processed ({})", source)
    }

    override fun socketWriteTimeout(conn: Connection) {
        logger.warn("NATS socket write timed out")
    }
}

/** Sends the connection's events and errors to SLF4J, passing connection events on to [connectionListener]. */
fun Options.Builder.slf4jLogging(connectionListener: ConnectionListener? = null): Options.Builder =
    Slf4jNatsListener(connectionListener).let { errorListener(it).connectionListener(it) }
