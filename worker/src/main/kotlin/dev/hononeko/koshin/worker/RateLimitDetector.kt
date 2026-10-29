package dev.hononeko.koshin.worker

import dev.hononeko.koshin.core.model.JobExecutionResult
import dev.hononeko.koshin.core.model.Platform
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonPrimitive
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant
import kotlin.time.toKotlinInstant

/**
 * Scans Renovate's JSON log lines (`LOG_FORMAT=json`) for a run aborted by the platform's rate limit. Renovate exits 0
 * in that case, so the logs are the only signal. Checked against Renovate 44.178.3:
 * * GitHub: `Rate limit exceeded - aborting` (warn) when the repository is aborted with `rate-limit-exceeded`. A
 *   secondary limit is logged before that as `GitHub failure: secondary rate limit` with the response headers.
 * * GitLab: a 429 aborts the repository as `Host error` (warn) with `hostType: "gitlab"` and the response headers.
 */
class RateLimitDetector(
    platform: Platform,
    private val abortPattern: Regex = Regex(DEFAULT_ABORT_PATTERN),
    private val clock: Clock = Clock.System,
) {
    private val hostType = platform.name.lowercase()

    @Volatile
    private var aborted = false
    private val retryAfter = AtomicReference<Duration?>(null)

    fun inspect(line: String) {
        val entry = parse(line) ?: return
        val msg = entry.string("msg") ?: return
        val response = (entry["err"] as? JsonObject)?.get("response") as? JsonObject
        val platformThrottled = msg == HOST_ERROR &&
            entry.string("hostType") == hostType &&
            response?.get("statusCode")?.jsonPrimitive?.intOrNull == TOO_MANY_REQUESTS

        if (abortPattern.containsMatchIn(msg) || platformThrottled) {
            aborted = true
        }
        if (msg == SECONDARY_LIMIT || platformThrottled) {
            (response?.get("headers") as? JsonObject)?.let(::waitFrom)?.let { wait ->
                retryAfter.updateAndGet { current -> if (current == null || wait > current) wait else current }
            }
        }
    }

    /** The verdict once the run has exited and its output is drained, or null if it wasn't rate-limited. */
    fun result(): JobExecutionResult.RateLimited? =
        if (aborted) JobExecutionResult.RateLimited(retryAfter.get()) else null

    private fun parse(line: String): JsonObject? = if (line.startsWith("{")) {
        runCatching { json.parseToJsonElement(line) as? JsonObject }.getOrNull()
    } else {
        null
    }

    private fun waitFrom(headers: JsonObject): Duration? {
        val retryAfterHeader = headers.string("retry-after")?.trim()
        val fromRetryAfter = retryAfterHeader?.toLongOrNull()?.seconds ?: retryAfterHeader?.let(::untilHttpDate)
        val fromReset = headers.string("x-ratelimit-reset")
            ?.takeIf { headers.string("x-ratelimit-remaining") == "0" }
            ?.toLongOrNull()
            ?.let { Instant.fromEpochSeconds(it) - clock.now() }
        return (fromRetryAfter ?: fromReset)?.takeIf { it.isPositive() }
    }

    private fun untilHttpDate(value: String): Duration? = try {
        ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant().toKotlinInstant() - clock.now()
    } catch (_: DateTimeParseException) {
        null
    }

    private fun JsonObject.string(key: String): String? = (this[key] as? JsonPrimitive)?.contentOrNull

    companion object {
        const val DEFAULT_ABORT_PATTERN = "^Rate limit exceeded - aborting$"
        private const val HOST_ERROR = "Host error"
        private const val SECONDARY_LIMIT = "GitHub failure: secondary rate limit"
        private const val TOO_MANY_REQUESTS = 429
        private val json = Json { ignoreUnknownKeys = true }
    }
}
