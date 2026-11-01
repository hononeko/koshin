package dev.hononeko.koshin.proxy

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.joran.JoranConfigurator
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.spi.LoggingEvent
import ch.qos.logback.core.ConsoleAppender
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.slf4j.MDC

/** The production logback.xml, not the logback-test.xml the other tests run with. */
class LogbackConfigTest {

    private fun configure(vararg properties: Pair<String, String>): LoggerContext = LoggerContext().apply {
        mdcAdapter = MDC.getMDCAdapter()
        properties.forEach { (key, value) -> putProperty(key, value) }
        JoranConfigurator().also { it.context = this }
            .doConfigure(checkNotNull(javaClass.classLoader.getResource("logback.xml")))
    }

    @Test
    fun `LOG_LEVEL sets koshin's level`() {
        assertEquals(Level.INFO, configure().getLogger("dev.hononeko.koshin").level)
        assertEquals(Level.DEBUG, configure("LOG_LEVEL" to "debug").getLogger("dev.hononeko.koshin").level)
    }

    @Test
    fun `events are JSON with the message in the message field`() {
        val context = configure()

        @Suppress("UNCHECKED_CAST")
        val appender = context.getLogger("ROOT").getAppender("STDOUT") as ConsoleAppender<ILoggingEvent>
        val event = LoggingEvent("test", context.getLogger("x"), Level.INFO, "Enqueued {}", null, arrayOf("job"))

        val json = Json.parseToJsonElement(appender.encoder.encode(event).decodeToString()).jsonObject

        assertEquals("Enqueued job", json["message"]?.jsonPrimitive?.content)
    }
}
