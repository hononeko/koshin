package dev.hononeko.koshin.worker

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
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.slf4j.MDC
import org.slf4j.event.KeyValuePair

/** The production logback.xml, not the logback-test.xml the other tests run with. */
class LogbackConfigTest {

    private fun configure(vararg properties: Pair<String, String>): LoggerContext = LoggerContext().apply {
        mdcAdapter = MDC.getMDCAdapter()
        properties.forEach { (key, value) -> putProperty(key, value) }
        JoranConfigurator().also { it.context = this }
            .doConfigure(checkNotNull(javaClass.classLoader.getResource("logback.xml")))
    }

    @Test
    fun `LOG_LEVEL sets koshin's level and ROOT_LOG_LEVEL everything else's`() {
        val defaults = configure()
        assertEquals(Level.INFO, defaults.getLogger("dev.hononeko.koshin").level)
        assertEquals(Level.INFO, defaults.getLogger("ROOT").level)
        assertEquals(Level.TRACE, defaults.getLogger("renovate").level)

        val debug = configure("LOG_LEVEL" to "debug", "ROOT_LOG_LEVEL" to "warn")
        assertEquals(Level.DEBUG, debug.getLogger("dev.hononeko.koshin").level)
        assertEquals(Level.WARN, debug.getLogger("ROOT").level)
        assertTrue(debug.getLogger("dev.hononeko.koshin.worker.WorkerDaemon").isDebugEnabled)
    }

    @Test
    fun `events are single-line JSON with MDC and key-value fields at the top level`() {
        val context = configure()
        val logger = context.getLogger("dev.hononeko.koshin.worker.Test")

        @Suppress("UNCHECKED_CAST")
        val appender = context.getLogger("ROOT").getAppender("STDOUT") as ConsoleAppender<ILoggingEvent>
        MDC.put("repo", "github:hononeko/koshin")
        val event = try {
            LoggingEvent("test", logger, Level.WARN, "Run failed for {}", IllegalStateException("boom"), arrayOf("x"))
                .apply {
                    prepareForDeferredProcessing()
                    addKeyValuePair(KeyValuePair("cache", "miss"))
                }
        } finally {
            MDC.remove("repo")
        }

        val encoded = appender.encoder.encode(event).decodeToString()
        val json = Json.parseToJsonElement(encoded).jsonObject

        assertFalse(encoded.trimEnd().contains('\n'))
        assertEquals("Run failed for x", json["message"]?.jsonPrimitive?.content)
        assertEquals("WARN", json["level"]?.jsonPrimitive?.content)
        assertEquals("github:hononeko/koshin", json["repo"]?.jsonPrimitive?.content)
        assertEquals("miss", json["cache"]?.jsonPrimitive?.content)
        assertTrue(json["stack_trace"]?.jsonPrimitive?.content.orEmpty().contains("IllegalStateException: boom"))
        assertNull(json["@version"])
        assertNull(json["level_value"])
    }
}
