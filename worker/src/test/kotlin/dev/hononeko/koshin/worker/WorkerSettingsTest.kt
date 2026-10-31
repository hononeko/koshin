package dev.hononeko.koshin.worker

import dev.hononeko.koshin.core.model.Platform
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.io.path.Path
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class WorkerSettingsTest {

    @Test
    fun `defaults match the env contract`() {
        val settings = WorkerSettings.fromEnv(emptyMap())
        assertEquals(8080, settings.port)
        assertTrue(settings.http2Enabled)
        assertEquals(setOf(Platform.GITHUB), settings.platforms)
        assertEquals("nats://koshin-nats:4222", settings.natsUrl)
        assertEquals(Path("/tmp/renovate"), settings.renovateBaseDir)
        assertEquals("global", settings.renovateBinarySource)
        assertEquals("redis://koshin-valkey:6379", settings.renovateRedisUrl)
        assertNull(settings.githubToken)
        assertFalse(settings.podDeletionCostEnabled)
        assertEquals(45.seconds, settings.jobConfig.lockTtl)
        assertEquals(45.minutes, settings.jobConfig.cliTimeout)
        assertEquals(3, settings.throttle.maxConcurrentRuns)
        assertEquals(60.seconds, settings.throttle.backoffBase)
        assertEquals(3600.seconds, settings.throttle.backoffMax)
        assertEquals(RenovateRequestLimits(), settings.requestLimits)
        assertEquals(RateLimitDetector.DEFAULT_ABORT_PATTERN, settings.rateLimitLogPattern)
        assertEquals(Path("/tmp/renovate/cache"), settings.renovateCacheDir)
        assertEquals(RepoCacheSettings(), settings.repoCache)
        assertEquals(RepoCacheProvider.NATS, settings.repoCache.provider)
        assertEquals("koshin-repo-cache", settings.repoCache.bucket)
    }

    @Test
    fun `fromEnv parses repo cache settings`() {
        val settings = WorkerSettings.fromEnv(
            mapOf(
                "RENOVATE_BASE_DIR" to "/work",
                "REPO_CACHE_PROVIDER" to "NATS",
                "REPO_CACHE_BUCKET" to "caches",
                "REPO_CACHE_MAX_BYTES" to "1073741824",
                "REPO_CACHE_TTL_DAYS" to "30",
            ),
        )
        assertEquals(Path("/work/cache"), settings.renovateCacheDir)
        assertEquals(
            RepoCacheSettings(bucket = "caches", maxBytes = 1_073_741_824, ttl = 30.days),
            settings.repoCache,
        )
        assertEquals(Path("/cache"), WorkerSettings.fromEnv(mapOf("RENOVATE_CACHE_DIR" to "/cache")).renovateCacheDir)
    }

    @Test
    fun `fromEnv parses s3 repo cache settings, accepting Renovate's legacy names`() {
        val s3 = mapOf(
            "REPO_CACHE_PROVIDER" to "s3",
            "RENOVATE_REPOSITORY_CACHE_TYPE" to "s3://renovate/repo-cache/",
            "RENOVATE_X_S3_ENDPOINT" to "http://seaweedfs:8333",
        )
        assertEquals(
            RepoCacheSettings(
                provider = RepoCacheProvider.S3,
                s3Url = "s3://renovate/repo-cache/",
                s3Endpoint = "http://seaweedfs:8333",
                s3PathStyle = true,
            ),
            WorkerSettings.fromEnv(s3).repoCache,
        )
        val canonical = WorkerSettings.fromEnv(
            s3 + mapOf("RENOVATE_S3_ENDPOINT" to "http://s3:9000", "RENOVATE_S3_PATH_STYLE" to "false"),
        ).repoCache
        assertEquals("http://s3:9000", canonical.s3Endpoint)
        assertFalse(canonical.s3PathStyle)
    }

    @Test
    fun `fromEnv rejects invalid repo cache settings`() {
        assertThrows<IllegalArgumentException> { WorkerSettings.fromEnv(mapOf("REPO_CACHE_PROVIDER" to "redis")) }
        assertThrows<IllegalArgumentException> { WorkerSettings.fromEnv(mapOf("REPO_CACHE_PROVIDER" to "s3")) }
        assertThrows<IllegalArgumentException> {
            WorkerSettings.fromEnv(
                mapOf("REPO_CACHE_PROVIDER" to "s3", "RENOVATE_REPOSITORY_CACHE_TYPE" to "local"),
            )
        }
        assertThrows<IllegalArgumentException> { WorkerSettings.fromEnv(mapOf("REPO_CACHE_BUCKET" to "a.b")) }
        assertThrows<IllegalArgumentException> { WorkerSettings.fromEnv(mapOf("REPO_CACHE_MAX_BYTES" to "0")) }
        assertThrows<IllegalArgumentException> { WorkerSettings.fromEnv(mapOf("RENOVATE_CACHE_DIR" to "cache")) }
    }

    @Test
    fun `fromEnv parses rate-limit settings`() {
        val settings = WorkerSettings.fromEnv(
            mapOf(
                "MAX_CONCURRENT_RUNS_PER_PLATFORM" to "5",
                "RATE_LIMIT_BACKOFF_BASE_SECONDS" to "30",
                "RATE_LIMIT_BACKOFF_MAX_SECONDS" to "600",
                "RENOVATE_MAX_REQUESTS_PER_SECOND" to "2.5",
                "RENOVATE_CONCURRENT_REQUEST_LIMIT" to "4",
                "RATE_LIMIT_LOG_PATTERN" to "quota exhausted",
            ),
        )
        assertEquals(5, settings.throttle.maxConcurrentRuns)
        assertEquals(30.seconds, settings.throttle.backoffBase)
        assertEquals(600.seconds, settings.throttle.backoffMax)
        assertEquals(RenovateRequestLimits(2.5, 4), settings.requestLimits)
        assertEquals("quota exhausted", settings.rateLimitLogPattern)
    }

    @Test
    fun `invalid rate-limit settings are rejected`() {
        listOf(
            mapOf("MAX_CONCURRENT_RUNS_PER_PLATFORM" to "0"),
            mapOf("RATE_LIMIT_BACKOFF_BASE_SECONDS" to "120", "RATE_LIMIT_BACKOFF_MAX_SECONDS" to "60"),
            mapOf("RENOVATE_MAX_REQUESTS_PER_SECOND" to "fast"),
            mapOf("RENOVATE_MAX_REQUESTS_PER_SECOND" to "0"),
            mapOf("RENOVATE_CONCURRENT_REQUEST_LIMIT" to "-1"),
            mapOf("RATE_LIMIT_LOG_PATTERN" to "(unclosed"),
        ).forEach { env ->
            assertThrows<IllegalArgumentException>(env.toString()) { WorkerSettings.fromEnv(env) }
        }
    }

    @Test
    fun `fromEnv parses configured values`() {
        val settings = WorkerSettings.fromEnv(
            mapOf(
                "PORT" to "9090",
                "HTTP2_ENABLED" to "FALSE",
                "PLATFORMS" to "github, gitlab",
                "NATS_URL" to "nats://nats:4222",
                "RENOVATE_BASE_DIR" to "/work",
                "RENOVATE_BINARY_SOURCE" to "install",
                "VALKEY_URL" to "redis://coord:6379",
                "GITHUB_TOKEN" to "ghp",
                "GITHUB_API_URL" to "https://ghe/api/v3",
                "GITLAB_TOKEN" to "glpat",
                "GITLAB_ENDPOINT" to "https://gitlab.example.com/api/v4",
                "POD_DELETION_COST_ENABLED" to "true",
                "POD_NAME" to "koshin-worker-0",
                "POD_NAMESPACE" to "koshin",
                "NATS_ACK_WAIT_SECONDS" to "90",
                "LOCK_TTL_SECONDS" to "60",
                "HEARTBEAT_INTERVAL_SECONDS" to "20",
                "DIRTY_TTL_SECONDS" to "7200",
                "LOCK_COLLISION_RETRY_SECONDS" to "10",
                "CLI_TIMEOUT_MINUTES" to "30",
                "MAX_RATE_LIMIT_REQUEUES" to "2",
            ),
        )
        assertEquals(9090, settings.port)
        assertFalse(settings.http2Enabled)
        assertEquals(setOf(Platform.GITHUB, Platform.GITLAB), settings.platforms)
        assertEquals(Path("/work"), settings.renovateBaseDir)
        assertEquals("install", settings.renovateBinarySource)
        assertEquals("redis://coord:6379", settings.renovateRedisUrl)
        assertEquals("https://ghe/api/v3", settings.githubEndpoint)
        assertEquals("https://gitlab.example.com/api/v4", settings.gitlabEndpoint)
        assertTrue(settings.podDeletionCostEnabled)
        assertEquals("koshin-worker-0", settings.podName)
        with(settings.jobConfig) {
            assertEquals(90.seconds, natsAckWait)
            assertEquals(60.seconds, lockTtl)
            assertEquals(20.seconds, heartbeatInterval)
            assertEquals(7200.seconds, dirtyTtl)
            assertEquals(10.seconds, lockCollisionRetryDelay)
            assertEquals(30.minutes, cliTimeout)
            assertEquals(2, maxRateLimitRequeues)
        }
    }

    @Test
    fun `legacy RENOVATE_BINARYSOURCE is still accepted`() {
        val settings = WorkerSettings.fromEnv(mapOf("RENOVATE_BINARYSOURCE" to "install"))
        assertEquals("install", settings.renovateBinarySource)
    }

    @Test
    fun `RENOVATE_REDIS_URL overrides VALKEY_URL`() {
        val settings = WorkerSettings.fromEnv(
            mapOf("VALKEY_URL" to "redis://coord:6379", "RENOVATE_REDIS_URL" to "redis://cache:6379"),
        )
        assertEquals("redis://cache:6379", settings.renovateRedisUrl)
    }

    @Test
    fun `sentinel VALKEY_URL is not reused as the Renovate package cache`() {
        val settings = WorkerSettings.fromEnv(mapOf("VALKEY_URL" to "redis-sentinel://sentinel:26379#mymaster"))
        assertNull(settings.renovateRedisUrl)
    }

    @Test
    fun `toString redacts tokens and NATS credentials`() {
        val text = WorkerSettings(
            natsUrl = "nats://koshin:natspw@nats:4222",
            githubToken = "ghp_secret",
            gitlabToken = "glpat_secret",
        ).toString()
        assertFalse(text.contains("ghp_secret"))
        assertFalse(text.contains("glpat_secret"))
        assertFalse(text.contains("natspw"))
        assertTrue(text.contains("nats://***@nats:4222"))
    }

    @Test
    fun `invalid values are rejected`() {
        assertThrows<IllegalArgumentException> { WorkerSettings.fromEnv(mapOf("PORT" to "eighty")) }
        assertThrows<IllegalArgumentException> { WorkerSettings.fromEnv(mapOf("HTTP2_ENABLED" to "maybe")) }
        assertThrows<IllegalArgumentException> { WorkerSettings.fromEnv(mapOf("PLATFORMS" to "bitbucket")) }
        assertThrows<IllegalArgumentException> { WorkerSettings.fromEnv(mapOf("PLATFORMS" to " , ")) }
        assertThrows<IllegalArgumentException> { WorkerSettings.fromEnv(mapOf("RENOVATE_BASE_DIR" to "relative")) }
        assertThrows<IllegalArgumentException> { WorkerSettings.fromEnv(mapOf("LOCK_TTL_SECONDS" to "90")) }
        assertThrows<IllegalArgumentException> {
            WorkerSettings.fromEnv(mapOf("POD_DELETION_COST_ENABLED" to "true", "POD_NAME" to "pod"))
        }
        assertThrows<IllegalArgumentException> { WorkerSettings(port = 70000) }
        assertThrows<IllegalArgumentException> { WorkerSettings(platforms = emptySet()) }
    }

    @Test
    fun `static credentials are selected by platform`() = runTest {
        val provider = StaticCredentialsProvider(
            WorkerSettings(githubToken = "ghp", githubEndpoint = "https://ghe/api/v3"),
        )
        val creds = provider.credentialsFor(dev.hononeko.koshin.core.model.RepoRef(Platform.GITHUB, "o/r"))
        assertEquals("ghp", creds.token)
        assertEquals("https://ghe/api/v3", creds.endpoint)
        assertTrue(provider.isConfigured(Platform.GITHUB))
        assertFalse(provider.isConfigured(Platform.GITLAB))
        assertThrows<IllegalStateException> {
            provider.credentialsFor(dev.hononeko.koshin.core.model.RepoRef(Platform.GITLAB, "g/p"))
        }
    }
}
