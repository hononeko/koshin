package dev.hononeko.koshin.proxy

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.usecase.IngestConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class ProxyConfigTest {

    @Test
    fun `default config has expected defaults`() {
        val config = ProxyConfig()
        assertEquals(8080, config.port)
        assertTrue(config.http2Enabled)
        assertEquals(setOf(Platform.GITHUB), config.platforms)
        assertTrue(config.isPlatformEnabled(Platform.GITHUB))
        assertFalse(config.isPlatformEnabled(Platform.GITLAB))
        assertEquals("nats://koshin-nats:4222", config.natsUrl)
        assertNull(config.githubWebhookSecret)
        assertNull(config.gitlabWebhookSecret)
        assertNull(config.internalTriggerToken)
        assertNull(config.sweepRepositoriesFile)
        assertEquals(setOf("renovate[bot]", "renovate", "renovate-bot"), config.botLogins)
        assertEquals(100.milliseconds, config.publishRetryBaseDelay)
        assertEquals(5.seconds, config.publishRetryBudget)
        assertEquals(10 * 1024 * 1024, config.maxRequestBodyBytes)
        assertEquals(15, config.requestReadTimeoutSeconds)
    }

    @Test
    fun `fromEnv parses configured environment variables`() {
        val env = mapOf(
            "PORT" to "9090",
            "HTTP2_ENABLED" to "false",
            "PLATFORMS" to "github, gitlab",
            "NATS_URL" to "nats://localhost:4222",
            "GITHUB_WEBHOOK_SECRET" to "gh-secret",
            "GITLAB_WEBHOOK_SECRET" to "gl-secret",
            "INTERNAL_TRIGGER_TOKEN" to "trigger-secret",
            "SWEEP_REPOSITORIES_FILE" to "/etc/koshin/repos.json",
            "RENOVATE_BOT_LOGINS" to "custom-bot[bot], second-bot",
            "GITHUB_BOT_LOGIN" to "gh-app[bot]",
            "GITLAB_BOT_LOGIN" to "gl-renovate",
            "QUEUED_TTL_SECONDS" to "7200",
            "DIRTY_TTL_SECONDS" to "1800",
            "COALESCE_TIMEOUT_MS" to "250",
        )

        val config = ProxyConfig.fromEnv(env)
        assertEquals(9090, config.port)
        assertFalse(config.http2Enabled)
        assertEquals(setOf(Platform.GITHUB, Platform.GITLAB), config.platforms)
        assertEquals("nats://localhost:4222", config.natsUrl)
        assertEquals("gh-secret", config.githubWebhookSecret)
        assertEquals("gl-secret", config.gitlabWebhookSecret)
        assertEquals("trigger-secret", config.internalTriggerToken)
        assertEquals("/etc/koshin/repos.json", config.sweepRepositoriesFile)
        val expectedBots = listOf("renovate[bot]", "renovate-bot", "custom-bot[bot]", "gh-app[bot]", "gl-renovate")
        assertTrue(config.botLogins.containsAll(expectedBots))
        assertEquals(
            IngestConfig(queuedTtl = 2.hours, dirtyTtl = 30.minutes, coalesceTimeout = 250.milliseconds),
            config.ingest,
        )
    }

    @Test
    fun `fromEnv uses defaults when optional env vars are blank or absent`() {
        val config = ProxyConfig.fromEnv(
            mapOf(
                "PLATFORMS" to "   ",
                "NATS_URL" to "  ",
                "GITHUB_WEBHOOK_SECRET" to " ",
                "GITLAB_WEBHOOK_SECRET" to "",
                "INTERNAL_TRIGGER_TOKEN" to " ",
                "SWEEP_REPOSITORIES_FILE" to "",
            ),
        )
        assertEquals(setOf(Platform.GITHUB), config.platforms)
        assertEquals("nats://koshin-nats:4222", config.natsUrl)
        assertNull(config.githubWebhookSecret)
        assertNull(config.gitlabWebhookSecret)
        assertNull(config.internalTriggerToken)
        assertNull(config.sweepRepositoriesFile)
    }

    @Test
    fun `fromEnv and constructor reject invalid values`() {
        assertThrows<IllegalArgumentException> {
            ProxyConfig.fromEnv(mapOf("PORT" to "not-a-port"))
        }
        assertThrows<IllegalArgumentException> {
            ProxyConfig.fromEnv(mapOf("HTTP2_ENABLED" to "maybe"))
        }
        assertThrows<IllegalArgumentException> {
            ProxyConfig.fromEnv(mapOf("PLATFORMS" to "bitbucket"))
        }
        assertThrows<IllegalArgumentException> {
            ProxyConfig.fromEnv(mapOf("PLATFORMS" to " , "))
        }
        assertThrows<IllegalArgumentException> {
            ProxyConfig.fromEnv(mapOf("QUEUED_TTL_SECONDS" to "a day"))
        }
        assertThrows<IllegalArgumentException> {
            ProxyConfig.fromEnv(mapOf("COALESCE_TIMEOUT_MS" to "0"))
        }
        assertThrows<IllegalArgumentException> {
            ProxyConfig(port = -1)
        }
        assertThrows<IllegalArgumentException> {
            ProxyConfig(port = 70000)
        }
        assertThrows<IllegalArgumentException> {
            ProxyConfig(platforms = emptySet())
        }
        assertThrows<IllegalArgumentException> {
            ProxyConfig(natsUrl = " ")
        }
        assertThrows<IllegalArgumentException> {
            ProxyConfig(publishRetryBaseDelay = (-1).milliseconds)
        }
        assertThrows<IllegalArgumentException> {
            ProxyConfig(publishRetryBudget = (-1).seconds)
        }
        assertThrows<IllegalArgumentException> {
            ProxyConfig(publishRetryBaseDelay = Duration.ZERO, publishRetryBudget = 1.seconds)
        }
        assertThrows<IllegalArgumentException> {
            ProxyConfig(maxRequestBodyBytes = 0)
        }
        assertThrows<IllegalArgumentException> {
            ProxyConfig(requestReadTimeoutSeconds = 0)
        }
    }

    @Test
    fun `fromEnv parses the worker dispatcher's settings`() {
        val defaults = ProxyConfig.fromEnv(
            mapOf("PLATFORMS" to "github,gitlab", "MAX_CONCURRENT_RUNS_PER_PLATFORM" to "2"),
        )
        assertFalse(defaults.dispatcher.enabled)
        assertEquals(4, defaults.dispatcher.maxWorkers, "run slots × platforms")
        assertEquals("koshin-worker", defaults.dispatcher.podTemplateName)

        val config = ProxyConfig.fromEnv(
            mapOf(
                "WORKER_DISPATCHER_ENABLED" to "true",
                "WORKER_POD_TEMPLATE_NAME" to "rel-koshin-worker",
                "POD_NAMESPACE" to "koshin",
                "WORKER_MAX_REPLICAS" to "8",
                "MAX_CONCURRENT_RUNS_PER_PLATFORM" to "4",
                "MIN_WARM_WORKERS" to "1",
                "WORKER_DISPATCH_INTERVAL_SECONDS" to "2",
                "WORKER_JOB_ACTIVE_DEADLINE_SECONDS" to "3600",
                "WORKER_JOB_TTL_SECONDS" to "30",
            ),
        )
        assertEquals(
            DispatcherConfig(
                enabled = true,
                podTemplateName = "rel-koshin-worker",
                namespace = "koshin",
                maxWorkers = 8,
                maxRunsPerPlatform = 4,
                minWarmWorkers = 1,
                interval = 2.seconds,
                jobActiveDeadline = 1.hours,
                jobTtlAfterFinished = 30.seconds,
            ),
            config.dispatcher,
        )
        assertThrows<IllegalArgumentException> { ProxyConfig.fromEnv(mapOf("WORKER_DISPATCHER_ENABLED" to "yes")) }
    }
}
