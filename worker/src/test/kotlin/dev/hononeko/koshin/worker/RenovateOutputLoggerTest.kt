package dev.hononeko.koshin.worker

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import net.logstash.logback.encoder.LogstashEncoder
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.slf4j.event.Level as Slf4jLevel

class RenovateOutputLoggerTest {
    private val logger = LoggerFactory.getLogger("test.renovate.output") as Logger
    private val appender = ListAppender<ILoggingEvent>()
    private val output = RenovateOutputLogger(logger)

    private val warnLine =
        """{"name":"renovate","level":40,"logContext":"c1","repository":"hononeko/koshin",""" +
            """"err":{"message":"Bad credentials","statusCode":401},"msg":"Repository has no token","time":"t","v":0}"""

    @BeforeEach
    fun attach() {
        appender.start()
        logger.addAppender(appender)
        logger.level = Level.TRACE
    }

    @AfterEach
    fun detach() {
        logger.detachAppender(appender)
    }

    @Test
    fun `json lines are logged at their Renovate level with msg as the message`() {
        output.log(warnLine, "stdout")

        val event = appender.list.single()
        assertEquals(Level.WARN, event.level)
        assertEquals("Repository has no token", event.formattedMessage)
    }

    @Test
    fun `the encoded line keeps Renovate's fields queryable`() {
        output.log(warnLine, "stdout")
        val encoder = LogstashEncoder().apply {
            context = LoggerFactory.getILoggerFactory() as LoggerContext
            start()
        }

        val encoded = encoder.encode(appender.list.single()).decodeToString()
        val json = Json.parseToJsonElement(encoded).jsonObject

        assertFalse(encoded.trimEnd().contains('\n'), "one line per event")
        assertEquals("Repository has no token", json["message"]?.jsonPrimitive?.content)
        assertEquals("WARN", json["level"]?.jsonPrimitive?.content)
        assertEquals("stdout", json["stream"]?.jsonPrimitive?.content)
        val renovate = json["renovate"]!!.jsonObject
        assertEquals("40", renovate["level"]?.jsonPrimitive?.content)
        assertEquals("hononeko/koshin", renovate["repository"]?.jsonPrimitive?.content)
        assertEquals("401", renovate["err"]!!.jsonObject["statusCode"]?.jsonPrimitive?.content)
    }

    @Test
    fun `other lines are logged as they are`() {
        output.log("DEBUG: plain text", "stdout")
        output.log("Error: boom", "stderr")
        output.log("{not json", "stdout")
        output.log("""{"level":"warn","msg":42}""", "stdout")

        assertEquals(
            listOf(Level.INFO, Level.WARN, Level.INFO, Level.INFO),
            appender.list.map { it.level },
        )
        assertEquals(
            listOf("DEBUG: plain text", "Error: boom", "{not json", ""),
            appender.list.map { it.formattedMessage },
        )
    }

    @Test
    fun `renovate levels map to log levels`() {
        assertEquals(
            listOf(
                Slf4jLevel.TRACE,
                Slf4jLevel.DEBUG,
                Slf4jLevel.INFO,
                Slf4jLevel.WARN,
                Slf4jLevel.ERROR,
                Slf4jLevel.ERROR,
                Slf4jLevel.INFO,
            ),
            listOf(10, 20, 30, 40, 50, 60, null).map(RenovateOutputLogger::levelOf),
        )
    }
}
