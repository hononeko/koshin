package dev.hononeko.koshin.worker

import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.intOrNull
import net.logstash.logback.marker.Markers
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.event.Level

/**
 * Logs Renovate's output (`LOG_FORMAT=json`). Each JSON line is logged at its Renovate level, with `msg` (or the whole
 * line, without one) as the message and the line embedded as the `renovate` field, so its fields (`level`, `msg`,
 * `repository`, `err`, ...) stay queryable. Other lines are logged as they are.
 */
class RenovateOutputLogger(private val logger: Logger = LoggerFactory.getLogger(LOGGER_NAME)) {

    fun log(line: String, stream: String) {
        val json = parse(line)
        if (json == null) {
            logger.atLevel(if (stream == STDERR) Level.WARN else Level.INFO)
                .addKeyValue("stream", stream)
                .log(line)
            return
        }
        val renovateLevel = (json["level"] as? JsonPrimitive)?.intOrNull
        val message = (json["msg"] as? JsonPrimitive)?.takeIf { it.isString }?.content ?: line
        logger.atLevel(levelOf(renovateLevel))
            .addMarker(Markers.appendRaw(RENOVATE_FIELD, line))
            .addKeyValue("stream", stream)
            .log(message)
    }

    private fun parse(line: String): JsonObject? {
        if (!line.trimStart().startsWith("{")) return null
        return try {
            Json.parseToJsonElement(line) as? JsonObject
        } catch (_: SerializationException) {
            null
        }
    }

    companion object {
        const val LOGGER_NAME = "renovate"
        const val RENOVATE_FIELD = "renovate"
        private const val STDERR = "stderr"
        private const val TRACE = 10
        private const val DEBUG = 20
        private const val INFO = 30
        private const val WARN = 40

        /** Renovate (bunyan) levels: 10 trace, 20 debug, 30 info, 40 warn, 50 error, 60 fatal. */
        fun levelOf(renovateLevel: Int?): Level = when {
            renovateLevel == null -> Level.INFO
            renovateLevel <= TRACE -> Level.TRACE
            renovateLevel <= DEBUG -> Level.DEBUG
            renovateLevel <= INFO -> Level.INFO
            renovateLevel <= WARN -> Level.WARN
            else -> Level.ERROR
        }
    }
}
