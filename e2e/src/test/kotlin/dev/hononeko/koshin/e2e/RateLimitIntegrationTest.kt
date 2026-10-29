package dev.hononeko.koshin.e2e

import dev.hononeko.koshin.adapters.valkey.PlatformThrottleConfig
import dev.hononeko.koshin.core.model.JobExecutionResult
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.model.TriggerReason
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/** Platform cooldown and the rate-limit requeue cap, with the real Valkey throttle and NATS queue. */
class RateLimitIntegrationTest {
    private val github = RepoRef(Platform.GITHUB, "hononeko/koshin")
    private val gitlab = RepoRef(Platform.GITLAB, "hononeko/koshin")
    private val clusters = mutableListOf<E2eCluster>()

    private fun cluster(maxRateLimitRequeues: Int = 5, throttle: PlatformThrottleConfig) =
        E2eCluster(maxRateLimitRequeues, throttle).also { clusters += it }

    @AfterEach
    fun tearDown() {
        clusters.forEach { it.close() }
    }

    private fun job(repo: RepoRef) = RenovateJobEvent(repo, UUID.randomUUID().toString(), TriggerReason.WEBHOOK)

    @Test
    fun `a rate-limited platform cools down while the other keeps running, then its requeued job runs`() {
        val cluster = cluster(throttle = PlatformThrottleConfig(backoffBase = 3.seconds, backoffMax = 12.seconds))
        val worker = cluster.startWorker("worker-1", platforms = setOf(Platform.GITHUB, Platform.GITLAB))
        val firstGithubRun = AtomicBoolean(true)
        worker.cli.outcome = { repo ->
            if (repo == github && firstGithubRun.getAndSet(false)) {
                JobExecutionResult.RateLimited(null)
            } else {
                JobExecutionResult.Success
            }
        }

        cluster.publish(job(github))
        val rateLimit = awaitValue("the GitHub rate limit") { worker.throttle.rateLimits.firstOrNull() }
        assertEquals(Platform.GITHUB, rateLimit.platform)
        assertTrue(rateLimit.cooldown in 2400.milliseconds..3600.milliseconds, "base 3s ± 20%: ${rateLimit.cooldown}")
        assertTrue(cluster.hasBackoff(Platform.GITHUB))
        val cooldownEnds = rateLimit.at + rateLimit.cooldown

        cluster.publish(job(gitlab))
        val gitlabRun = awaitValue("the GitLab run") { worker.runs().firstOrNull { it.repo == gitlab } }
        assertTrue(gitlabRun.startedAt < cooldownEnds, "GitLab isn't held up by GitHub's cooldown")

        val requeued = awaitValue("the requeued GitHub run") {
            worker.runs().filter { it.repo == github }.getOrNull(1)
        }
        cluster.awaitQuiescent(github)
        cluster.awaitQuiescent(gitlab)

        val slack = 50.milliseconds
        val githubFetchesDuringCooldown = worker.consumer.fetches.filter {
            it.platform == Platform.GITHUB && it.at > rateLimit.at && it.at < cooldownEnds - slack
        }
        assertEquals(emptyList<Fetch>(), githubFetchesDuringCooldown, "no GitHub fetches during its cooldown")
        assertTrue(requeued.startedAt >= cooldownEnds - slack, "the requeued job runs after the cooldown")

        val requeuedDelivery = worker.deliveries.single { it.event.triggerReason == TriggerReason.RATE_LIMIT_REQUEUE }
        assertEquals(1, requeuedDelivery.event.rateLimitAttempt)
        assertEquals(JobExecutionResult.Success, requeued.result)
        assertFalse(cluster.hasBackoff(Platform.GITHUB), "a successful run resets the backoff")
        assertTrue(cluster.deadLetters().isEmpty())
    }

    @Test
    fun `a run that is always rate-limited is dead-lettered after the requeue cap`() {
        val cluster = cluster(
            maxRateLimitRequeues = 2,
            throttle = PlatformThrottleConfig(backoffBase = 300.milliseconds, backoffMax = 1.seconds),
        )
        val worker = cluster.startWorker("worker-1")
        worker.cli.outcome = { JobExecutionResult.RateLimited(null) }

        cluster.publish(job(github))
        val deadLetter = awaitValue("the job to reach the DLQ") { cluster.deadLetters().singleOrNull() }
        cluster.awaitQuiescent(github)

        assertEquals(github, deadLetter.repo)
        assertEquals(2, deadLetter.rateLimitAttempt)
        assertEquals(3, worker.runs().size, "the initial run + 2 requeues")
        assertEquals(listOf(0, 1, 2), worker.deliveries.map { it.event.rateLimitAttempt })
        assertTrue(worker.deliveries.all { it.settlement == Settlement.Ack })
        val cooldowns = worker.throttle.rateLimits.map { it.cooldown }
        assertEquals(3, cooldowns.size)
        assertTrue(cooldowns[1] > cooldowns[0], "the backoff grows between consecutive rate limits: $cooldowns")
    }
}
