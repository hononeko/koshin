package dev.hononeko.koshin.adapters.github

import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.MockRequestHandleScope
import io.ktor.client.engine.mock.respond
import io.ktor.client.request.HttpResponseData
import io.ktor.client.request.get
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.random.Random
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

class GitHubHttpClientTest {
    private val now = Instant.parse("2026-10-30T12:00:00Z")
    private val clock = object : Clock {
        override fun now(): Instant = now
    }

    /** No jitter, so backoff steps are exact. */
    private val noJitter = object : Random() {
        override fun nextBits(bitCount: Int): Int = 0
    }

    private val sleeps = mutableListOf<Duration>()
    private var attempts = 0

    private fun client(
        retry: HttpRetryConfig = HttpRetryConfig(),
        random: Random = noJitter,
        vararg responses: MockRequestHandleScope.() -> HttpResponseData,
    ): HttpClient {
        attempts = 0
        return gitHubHttpClient(
            engine = MockEngine { responses[minOf(attempts++, responses.lastIndex)]() },
            retry = retry,
            clock = clock,
            random = random,
            sleep = { sleeps += it },
        )
    }

    private fun status(
        code: Int,
        body: String = "",
        vararg headers: Pair<String, String>,
    ): MockRequestHandleScope.() -> HttpResponseData = {
        val headerValues = headers.map { (name, value) -> name to listOf(value) }.toTypedArray()
        respond(body, HttpStatusCode.fromValue(code), headersOf(*headerValues))
    }

    private val ok = status(200, "{}")

    private suspend fun get(client: HttpClient) = client.get("https://api.github.com/app").status.value

    @Test
    fun `429 waits for Retry-After`() = runTest {
        val client = client(responses = arrayOf(status(429, "", "Retry-After" to "7"), ok))
        assertEquals(200, get(client))
        assertEquals(2, attempts)
        assertEquals(listOf(7.seconds), sleeps)
    }

    @Test
    fun `403 with an exhausted primary limit waits until the reset`() = runTest {
        val reset = (now.epochSeconds + 30).toString()
        val limited = status(
            403,
            """{"message":"API rate limit exceeded"}""",
            "x-ratelimit-remaining" to "0",
            "x-ratelimit-reset" to reset,
        )
        assertEquals(200, get(client(responses = arrayOf(limited, ok))))
        assertEquals(listOf(30.seconds), sleeps)
    }

    @Test
    fun `secondary-limit 403 without headers waits a minute`() = runTest {
        val secondary = status(403, """{"message":"You have exceeded a secondary rate limit. Please wait."}""")
        assertEquals(200, get(client(responses = arrayOf(secondary, ok))))
        assertEquals(listOf(1.minutes), sleeps)
    }

    @Test
    fun `secondary-limit 403 with Retry-After honours it`() = runTest {
        val secondary = status(403, """{"message":"You have exceeded a secondary rate limit"}""", "Retry-After" to "20")
        assertEquals(200, get(client(responses = arrayOf(secondary, ok))))
        assertEquals(listOf(20.seconds), sleeps)
    }

    @Test
    fun `5xx backs off exponentially`() = runTest {
        val client = client(responses = arrayOf(status(502), status(503), status(500), ok))
        assertEquals(200, get(client))
        assertEquals(listOf(1.seconds, 2.seconds, 4.seconds), sleeps)
    }

    @Test
    fun `backoff is capped and jittered by up to 20 percent`() = runTest {
        val retry = HttpRetryConfig(maxRetries = 6, baseDelay = 1.seconds, maxDelay = 4.seconds)
        val client = client(retry, Random(42), status(500))
        assertEquals(500, get(client))
        assertEquals(7, attempts, "1 attempt + maxRetries")
        val ceilings = listOf(1, 2, 4, 4, 4, 4).map { it.seconds }
        assertEquals(ceilings.size, sleeps.size)
        sleeps.zip(ceilings).forEach { (slept, base) ->
            assertTrue(slept >= base && slept <= base * 1.2, "$slept vs $base")
        }
    }

    @Test
    fun `a rate limit asking for longer than maxDelay fails fast`() = runTest {
        val client = client(responses = arrayOf(status(429, "", "Retry-After" to "3600"), ok))
        assertEquals(429, get(client))
        assertEquals(1, attempts)
    }

    @Test
    fun `other 4xx responses are not retried`() = runTest {
        val forbidden = status(
            403,
            """{"message":"Resource not accessible by integration"}""",
            "x-ratelimit-remaining" to "4999",
        )
        assertEquals(403, get(client(responses = arrayOf(forbidden, ok))))
        assertEquals(1, attempts)
        assertEquals(404, get(client(responses = arrayOf(status(404), ok))))
        assertEquals(1, attempts)
        assertTrue(sleeps.isEmpty())
    }
}
