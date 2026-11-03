package dev.hononeko.koshin.e2e

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.model.TriggerReason
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/** Burst workers that drain and exit, and the warm worker's lease (KSH-31), against real NATS and Valkey. */
class WorkerLifecycleIntegrationTest {
    private val cluster = E2eCluster()

    @AfterEach
    fun tearDown() {
        cluster.close()
    }

    private fun repo(name: String) = RepoRef(Platform.GITHUB, "hononeko/$name")

    private fun publish(repo: RepoRef) =
        cluster.publish(RenovateJobEvent(repo, UUID.randomUUID().toString(), TriggerReason.SCHEDULE))

    private fun TestWorker.awaitFinished() =
        assertTrue(finished.await(30, TimeUnit.SECONDS), "$name to finish on its own")

    @Test
    fun `a burst worker drains the queued jobs back to back, then finishes`() {
        val repos = listOf(repo("a"), repo("b"), repo("c"))
        repos.forEach(::publish)

        val worker = cluster.startWorker("burst-1", idleTimeout = 2.seconds)
        worker.awaitFinished()

        val runs = worker.runs()
        assertEquals(repos.toSet(), runs.map { it.repo }.toSet())
        assertTrue(runs.all { it.state == CliRun.State.COMPLETED })
        assertTrue(
            runs.zipWithNext().all { (a, b) -> b.startedAt - a.startedAt >= E2eCluster.DEFAULT_RUN_TIME },
            "one job at a time",
        )
        assertEquals(0, cluster.liveWarmWorkers(), "a burst worker keeps no warm lease")
        repos.forEach(cluster::awaitQuiescent)
    }

    @Test
    fun `a burst worker finishes when the only queued jobs are in a cooled-down platform`() {
        runBlocking { cluster.throttlePort.registerRateLimit(Platform.GITHUB, 5.minutes) }
        publish(repo("cooling"))

        val worker = cluster.startWorker("burst-1", idleTimeout = 1.seconds)
        worker.awaitFinished()

        assertTrue(worker.runs().isEmpty())
        assertTrue(worker.deliveries.isEmpty(), "a cooled-down platform's jobs aren't even fetched")
    }

    @Test
    fun `a burst worker past its max lifetime finishes the job in flight and leaves the rest queued`() {
        publish(repo("first"))
        publish(repo("second"))

        val worker = cluster.startWorker(
            "burst-1",
            runTime = 2.seconds,
            idleTimeout = 1.minutes,
            maxLifetime = 1.seconds,
        )
        worker.awaitFinished()

        assertEquals(listOf(CliRun.State.COMPLETED), worker.runs().map { it.state })
        assertEquals(1L, cluster.pendingJobs(Platform.GITHUB))
    }

    @Test
    fun `a warm worker keeps its lease across idle periods and drops it on SIGTERM`() {
        val worker = cluster.startWorker("warm-1")
        awaitUntil("the warm lease") { cluster.liveWarmWorkers() == 1 }

        // Outlives the lease TTL (3s here) only if it's renewed
        Thread.sleep(4.seconds.inWholeMilliseconds)
        assertEquals(1, cluster.liveWarmWorkers())
        assertEquals(1L, worker.finished.count, "a warm worker doesn't finish on its own")

        worker.sigterm()
        assertEquals(0, cluster.liveWarmWorkers())
    }
}
