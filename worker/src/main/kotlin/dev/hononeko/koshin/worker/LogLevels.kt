package dev.hononeko.koshin.worker

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/** Logback reads an unknown level (e.g. `warning`) as DEBUG: fall back to INFO instead, and say so. */
internal fun checkLogLevels(
    env: Map<String, String> = System.getenv(),
    context: LoggerContext? = LoggerFactory.getILoggerFactory() as? LoggerContext,
) {
    context ?: return
    val loggers = mapOf("LOG_LEVEL" to "dev.hononeko.koshin", "ROOT_LOG_LEVEL" to Logger.ROOT_LOGGER_NAME)
    loggers.forEach { (variable, name) ->
        val value = env[variable]?.trim()?.takeIf { it.isNotEmpty() } ?: return@forEach
        if (Level.toLevel(value, null) == null) {
            context.getLogger(name).level = Level.INFO
            context.getLogger("dev.hononeko.koshin").warn("Unknown {} '{}', using INFO", variable, value)
        }
    }
}
