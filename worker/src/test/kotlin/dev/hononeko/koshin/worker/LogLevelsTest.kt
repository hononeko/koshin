package dev.hononeko.koshin.worker

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.slf4j.Logger

class LogLevelsTest {
    private val context = LoggerContext().apply {
        getLogger("dev.hononeko.koshin").level = Level.DEBUG
        getLogger(Logger.ROOT_LOGGER_NAME).level = Level.DEBUG
    }

    @Test
    fun `unknown levels fall back to INFO`() {
        checkLogLevels(mapOf("LOG_LEVEL" to "warning", "ROOT_LOG_LEVEL" to "verbose"), context)

        assertEquals(Level.INFO, context.getLogger("dev.hononeko.koshin").level)
        assertEquals(Level.INFO, context.getLogger(Logger.ROOT_LOGGER_NAME).level)
    }

    @Test
    fun `valid or unset levels are left alone`() {
        checkLogLevels(mapOf("LOG_LEVEL" to "debug"), context)
        checkLogLevels(emptyMap(), null)

        assertEquals(Level.DEBUG, context.getLogger("dev.hononeko.koshin").level)
        assertEquals(Level.DEBUG, context.getLogger(Logger.ROOT_LOGGER_NAME).level)
    }
}
