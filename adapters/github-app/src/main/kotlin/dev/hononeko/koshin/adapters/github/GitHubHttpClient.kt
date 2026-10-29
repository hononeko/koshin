package dev.hononeko.koshin.adapters.github

import io.ktor.client.HttpClient
import io.ktor.client.engine.HttpClientEngine
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.HttpRequestRetry
import io.ktor.client.plugins.api.Send
import io.ktor.client.plugins.api.createClientPlugin
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.util.AttributeKey
import kotlinx.coroutines.delay
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import kotlin.math.pow
import kotlin.random.Random
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant
import kotlin.time.toKotlinInstant

data class HttpRetryConfig(
    val maxRetries: Int = DEFAULT_MAX_RETRIES,
    val baseDelay: Duration = DEFAULT_BASE_DELAY,
    /** Longest wait between attempts; a rate limit that asks for longer fails fast instead. */
    val maxDelay: Duration = DEFAULT_MAX_DELAY,
) {
    init {
        require(maxRetries >= 0) { "maxRetries must be >= 0: $maxRetries" }
        require(baseDelay.isPositive()) { "baseDelay must be positive: $baseDelay" }
        require(maxDelay >= baseDelay) { "maxDelay ($maxDelay) must be >= baseDelay ($baseDelay)" }
    }

    companion object {
        const val DEFAULT_MAX_RETRIES = 3
        val DEFAULT_BASE_DELAY: Duration = 1.seconds
        val DEFAULT_MAX_DELAY: Duration = 1.minutes
    }
}

/**
 * The client for koshin's own GitHub API calls (App token minting, repository discovery). Retries 429, 5xx and
 * rate-limit 403s (`x-ratelimit-remaining: 0`, secondary limits) with exponential backoff and jitter, and waits as
 * long as `Retry-After` / `x-ratelimit-reset` ask when they're within [HttpRetryConfig.maxDelay]. Add callers' own
 * plugins with `client.config { ... }`.
 */
fun gitHubHttpClient(
    engine: HttpClientEngine = CIO.create(),
    retry: HttpRetryConfig = HttpRetryConfig(),
    clock: Clock = Clock.System,
    random: Random = Random.Default,
    sleep: suspend (Duration) -> Unit = { delay(it) },
): HttpClient {
    val policy = RateLimitRetryPolicy(retry, clock, random)
    return HttpClient(engine) {
        install(HttpRequestRetry) {
            retryIf(retry.maxRetries) { _, response -> policy.shouldRetry(response) }
            delayMillis(respectRetryAfterHeader = false) { attempt ->
                policy.delayFor(response, attempt).inWholeMilliseconds
            }
            delay { sleep(it.milliseconds) }
        }
        // Installed after HttpRequestRetry, so it runs inside each attempt and tags the response before retryIf.
        install(SecondaryRateLimitTagger)
    }
}

internal class RateLimitRetryPolicy(
    private val retry: HttpRetryConfig,
    private val clock: Clock,
    private val random: Random,
) {
    fun shouldRetry(response: HttpResponse): Boolean {
        val retryable = response.status.value == TOO_MANY_REQUESTS ||
            response.status.value in SERVER_ERRORS ||
            (response.status == HttpStatusCode.Forbidden && isRateLimited(response))
        return retryable && (requiredWait(response) ?: Duration.ZERO) <= retry.maxDelay
    }

    fun delayFor(response: HttpResponse?, attempt: Int): Duration {
        val backoff = minOf(retry.baseDelay * 2.0.pow(attempt - 1), retry.maxDelay)
        val jittered = backoff * (1 + random.nextDouble() * JITTER)
        return maxOf(response?.let(::requiredWait) ?: Duration.ZERO, jittered)
    }

    private fun isRateLimited(response: HttpResponse): Boolean = response.headers[REMAINING] == "0" ||
        response.headers[HttpHeaders.RetryAfter] != null ||
        response.call.attributes.contains(SecondaryRateLimit)

    /** What the server asked for: `Retry-After`, the primary limit's reset, or a minute for a bare secondary limit. */
    private fun requiredWait(response: HttpResponse): Duration? {
        val retryAfter = response.headers[HttpHeaders.RetryAfter]?.trim()?.let { value ->
            value.toLongOrNull()?.seconds ?: untilHttpDate(value)
        }
        val reset = response.headers[RESET]
            ?.takeIf { response.headers[REMAINING] == "0" }
            ?.toLongOrNull()
            ?.let { Instant.fromEpochSeconds(it) - clock.now() }
        val secondary = SECONDARY_LIMIT_WAIT.takeIf { response.call.attributes.contains(SecondaryRateLimit) }
        return (retryAfter ?: reset ?: secondary)?.coerceAtLeast(Duration.ZERO)
    }

    private fun untilHttpDate(value: String): Duration? = try {
        ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant().toKotlinInstant() - clock.now()
    } catch (_: DateTimeParseException) {
        null
    }

    private companion object {
        const val TOO_MANY_REQUESTS = 429
        val SERVER_ERRORS = 500..599
        const val REMAINING = "x-ratelimit-remaining"
        const val RESET = "x-ratelimit-reset"
        const val JITTER = 0.2

        // GitHub: without retry-after or x-ratelimit-* headers, wait at least a minute after a secondary limit.
        val SECONDARY_LIMIT_WAIT: Duration = 1.minutes
    }
}

private val SecondaryRateLimit = AttributeKey<Unit>("GitHubSecondaryRateLimit")

/** GitHub only says "secondary rate limit" in the body of a 403, which `retryIf` can't read. */
private val SecondaryRateLimitTagger = createClientPlugin("GitHubSecondaryRateLimitTagger") {
    on(Send) { request ->
        val call = proceed(request)
        if (call.response.status == HttpStatusCode.Forbidden &&
            call.response.bodyAsText().contains("secondary rate limit", ignoreCase = true)
        ) {
            call.attributes.put(SecondaryRateLimit, Unit)
        }
        call
    }
}
