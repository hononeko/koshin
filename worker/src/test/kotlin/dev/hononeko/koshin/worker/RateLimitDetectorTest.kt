package dev.hononeko.koshin.worker

import dev.hononeko.koshin.core.model.JobExecutionResult
import dev.hononeko.koshin.core.model.Platform
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import kotlin.time.Clock
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

class RateLimitDetectorTest {
    private val now = Instant.parse("2026-10-30T12:00:00Z")
    private val clock = object : Clock {
        override fun now(): Instant = now
    }

    private fun detect(platform: Platform = Platform.GITHUB, vararg lines: String): JobExecutionResult.RateLimited? {
        val detector = RateLimitDetector(platform, clock = clock)
        lines.forEach(detector::inspect)
        return detector.result()
    }

    // Shapes as logged by Renovate 44.178.3 with LOG_FORMAT=json.
    private fun entry(msg: String, level: Int = 40, extra: String = "") =
        """{"name":"renovate","level":$level,"logContext":"ctx","repository":"hononeko/koshin",$extra"msg":"$msg","v":0}"""

    private fun httpError(status: Int, headers: String) =
        """"err":{"name":"HTTPError","message":"Response code $status","response":{"statusCode":$status,""" +
            """"headers":{$headers}}},"""

    private fun hostError(hostType: String) =
        entry("Host error", extra = """"hostType":"$hostType",${httpError(429, """"retry-after":"30"""")}""")

    private val aborted = entry("Rate limit exceeded - aborting")
    private val finished = entry("Repository finished", level = 30, extra = """"result":"rate-limit-exceeded",""")

    @Test
    fun `a GitHub rate-limit abort without headers has no retry-after`() {
        assertEquals(JobExecutionResult.RateLimited(null), detect(lines = arrayOf(aborted, finished)))
    }

    @Test
    fun `a secondary limit's retry-after header is used`() {
        val secondary = entry(
            "GitHub failure: secondary rate limit",
            extra = httpError(403, """"retry-after":"60","x-ratelimit-remaining":"4990""""),
        )
        assertEquals(JobExecutionResult.RateLimited(60.seconds), detect(lines = arrayOf(secondary, aborted)))
    }

    @Test
    fun `an exhausted primary limit waits until x-ratelimit-reset`() {
        val reset = now.epochSeconds + 900
        val secondary = entry(
            "GitHub failure: secondary rate limit",
            extra = httpError(403, """"x-ratelimit-remaining":"0","x-ratelimit-reset":"$reset""""),
        )
        assertEquals(JobExecutionResult.RateLimited(900.seconds), detect(lines = arrayOf(secondary, aborted)))
    }

    @Test
    fun `an HTTP-date retry-after is relative to now`() {
        val secondary = entry(
            "GitHub failure: secondary rate limit",
            extra = httpError(403, """"retry-after":"Fri, 30 Oct 2026 12:02:00 GMT""""),
        )
        assertEquals(JobExecutionResult.RateLimited(120.seconds), detect(lines = arrayOf(secondary, aborted)))
    }

    @Test
    fun `the longest of several waits wins`() {
        val short = entry("GitHub failure: secondary rate limit", extra = httpError(403, """"retry-after":"10""""))
        val long = entry("GitHub failure: secondary rate limit", extra = httpError(403, """"retry-after":"90""""))
        assertEquals(JobExecutionResult.RateLimited(90.seconds), detect(lines = arrayOf(long, short, aborted)))
    }

    @Test
    fun `a GitLab 429 aborts the run as a platform host error`() {
        assertEquals(JobExecutionResult.RateLimited(30.seconds), detect(Platform.GITLAB, hostError("gitlab")))
    }

    @Test
    fun `a 429 from another host is not a platform rate limit`() {
        val npm = hostError("npm")
        assertNull(detect(Platform.GITLAB, npm))
        assertNull(detect(Platform.GITHUB, npm))
    }

    @Test
    fun `a secondary limit Renovate recovered from is not a rate-limited run`() {
        val secondary = entry("GitHub failure: secondary rate limit", extra = httpError(403, """"retry-after":"60""""))
        val done = entry("Repository finished", level = 30, extra = """"result":"done",""")
        assertNull(detect(lines = arrayOf(secondary, done)))
    }

    @Test
    fun `plain text and malformed lines are ignored`() {
        assertNull(detect(lines = arrayOf("Rate limit exceeded - aborting", "{not json", "", "{}", """{"msg":42}""")))
    }

    @Test
    fun `the abort pattern is configurable`() {
        val detector = RateLimitDetector(Platform.GITHUB, Regex("quota exhausted"), clock)
        detector.inspect(aborted)
        assertNull(detector.result())
        detector.inspect(entry("API quota exhausted"))
        assertEquals(JobExecutionResult.RateLimited(null), detector.result())
    }
}
