package dev.hononeko.koshin.adapters.nats

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.nats.client.Connection
import io.nats.client.ConnectionListener
import io.nats.client.Consumer
import io.nats.client.ErrorListener
import io.nats.client.JetStreamSubscription
import io.nats.client.Message
import io.nats.client.Options
import io.nats.client.support.Status
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.net.ConnectException

class Slf4jNatsListenerTest {
    private val logger = LoggerFactory.getLogger("dev.hononeko.koshin.nats") as Logger
    private val appender = ListAppender<ILoggingEvent>()
    private val conn = mockk<Connection> { every { connectedUrl } returns "nats://nats:4222" }
    private val sub = mockk<JetStreamSubscription>()
    private val status = mockk<Status> {
        every { code } returns 409
        every { message } returns "Exceeded MaxWaiting"
    }

    @BeforeEach
    fun attach() {
        appender.start()
        logger.addAppender(appender)
        logger.level = Level.TRACE
    }

    @AfterEach
    fun detach() {
        logger.detachAppender(appender)
        logger.level = null
    }

    private fun ILoggingEvent.kv(key: String): Any? = keyValuePairs?.firstOrNull { it.key == key }?.value

    @Test
    fun `connection events are logged with their level and passed on`() {
        val delegate = mockk<ConnectionListener>(relaxed = true)
        val listener = Slf4jNatsListener(delegate)

        listener.connectionEvent(conn, ConnectionListener.Events.DISCONNECTED, 1L, "nats://nats-1:4222")
        listener.connectionEvent(conn, ConnectionListener.Events.RECONNECTED, 2L, null)
        listener.connectionEvent(conn, ConnectionListener.Events.DISCOVERED_SERVERS, 3L, null)
        listener.connectionEvent(conn, ConnectionListener.Events.CLOSED, 4L, null)

        assertEquals(listOf(Level.WARN, Level.INFO, Level.DEBUG, Level.DEBUG), appender.list.map { it.level })
        assertEquals("DISCONNECTED", appender.list[0].kv("event"))
        assertEquals("nats://nats-1:4222", appender.list[0].kv("server"))
        assertEquals("nats://nats:4222", appender.list[1].kv("server"))
        verify { delegate.connectionEvent(conn, ConnectionListener.Events.RECONNECTED, 2L, null) }
    }

    @Test
    fun `connection errors are one warning line, with the stack trace at debug`() {
        Slf4jNatsListener().exceptionOccurred(conn, ConnectException("Connection refused"))

        val (warning, debug) = appender.list
        assertEquals(Level.WARN, warning.level)
        assertEquals("NATS connection error: Connection refused", warning.formattedMessage)
        assertNull(warning.throwableProxy)
        assertEquals(Level.DEBUG, debug.level)
        assertEquals("java.net.ConnectException", debug.throwableProxy.className)
    }

    @Test
    fun `other client errors are logged through SLF4J`() {
        val listener = Slf4jNatsListener()
        listener.errorOccurred(conn, "Authorization Violation")
        listener.slowConsumerDetected(conn, mockk<Consumer> { every { pendingMessageCount } returns 42 })
        listener.messageDiscarded(conn, mockk<Message> { every { subject } returns "renovate.jobs.github" })
        listener.heartbeatAlarm(conn, sub, 7, 3)
        listener.unhandledStatus(conn, sub, status)
        listener.pullStatusWarning(conn, sub, status)
        listener.pullStatusError(conn, sub, status)
        listener.flowControlProcessed(conn, sub, "fc", ErrorListener.FlowControlSource.HEARTBEAT)
        listener.socketWriteTimeout(conn)

        val warn = Level.WARN
        assertEquals(
            listOf(warn, warn, warn, warn, warn, Level.DEBUG, warn, Level.DEBUG, warn),
            appender.list.map { it.level },
        )
        assertEquals(42L, appender.list[1].kv("pendingMessages"))
        assertEquals("renovate.jobs.github", appender.list[2].kv("subject"))
    }

    @Test
    fun `slf4jLogging installs the listener for errors and connection events`() {
        val options = Options.Builder().slf4jLogging().build()

        assertSame(options.errorListener, options.connectionListener)
        assertEquals(Slf4jNatsListener::class, options.errorListener::class)
    }
}
