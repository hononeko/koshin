package dev.hononeko.koshin.e2e

import dev.hononeko.koshin.adapters.valkey.PlatformThrottleConfig
import dev.hononeko.koshin.core.model.CredentialScope
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.model.TriggerReason
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.UUID
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.TimeSource

/** Run-slot caps limiting concurrency per credential scope across concurrent workers. */
class RunSlotConcurrencyIntegrationTest {
    private val clusters = mutableListOf<E2eCluster>()

    private fun cluster(throttle: PlatformThrottleConfig) = E2eCluster(throttle = throttle).also {
        clusters += it
    }

    @AfterEach
    fun tearDown() {
        clusters.forEach { runCatching { it.close() } }
    }

    @Test
    fun `two workers on the same credential scope never run concurrently when maxRunsPerCredential is 1`() {
        val cluster = cluster(PlatformThrottleConfig(maxRunsPerCredential = 1))
        val repo1 = RepoRef(Platform.GITHUB, "hononeko/repo-1")
        val repo2 = RepoRef(Platform.GITHUB, "hononeko/repo-2")
        val scope = CredentialScope.Token(Platform.GITHUB)

        cluster.publish(RenovateJobEvent(repo1, UUID.randomUUID().toString(), TriggerReason.WEBHOOK))
        cluster.publish(RenovateJobEvent(repo2, UUID.randomUUID().toString(), TriggerReason.WEBHOOK))

        val worker1 = cluster.startWorker("worker-1").apply { cli.hold() }
        val worker2 = cluster.startWorker("worker-2")

        awaitUntil("worker-1 to start repo-1") { worker1.runs().isNotEmpty() }
        assertEquals(listOf(repo1), worker1.runs().map { it.repo })
        assertEquals(CliRun.State.RUNNING, worker1.runs().single().state)

        Thread.sleep(300.milliseconds.inWholeMilliseconds)
        assertTrue(worker2.runs().isEmpty(), "worker-2 cannot acquire a run slot while worker-1 holds it")

        worker1.cli.release()
        awaitUntil("worker-1 to finish") { worker1.runs().single().state == CliRun.State.COMPLETED }
        val worker1FinishedAt = TimeSource.Monotonic.markNow()

        awaitUntil("worker-2 to pick up repo-2") { worker2.runs().isNotEmpty() }
        cluster.awaitQuiescent(repo1, scope)
        cluster.awaitQuiescent(repo2, scope)

        assertEquals(listOf(repo2), worker2.runs().map { it.repo })
        assertEquals(CliRun.State.COMPLETED, worker2.runs().single().state)
        assertTrue(
            worker2.runs().single().startedAt >= worker1FinishedAt - 50.milliseconds,
            "worker-2 started after worker-1 released the slot",
        )
    }

    @Test
    fun `different credential scopes run concurrently even when maxRunsPerCredential is 1`() {
        val cluster = cluster(PlatformThrottleConfig(maxRunsPerCredential = 1))
        val repo1 = RepoRef(Platform.GITHUB, "tenant-1/repo")
        val repo2 = RepoRef(Platform.GITHUB, "tenant-2/repo")

        cluster.publish(
            RenovateJobEvent(repo1, UUID.randomUUID().toString(), TriggerReason.WEBHOOK, installationId = 100),
        )
        cluster.publish(
            RenovateJobEvent(repo2, UUID.randomUUID().toString(), TriggerReason.WEBHOOK, installationId = 200),
        )

        val worker1 = cluster.startWorker("worker-1").apply { cli.hold() }
        val worker2 = cluster.startWorker("worker-2").apply { cli.hold() }

        awaitUntil("both workers to be running concurrently") {
            worker1.runs().any { it.state == CliRun.State.RUNNING } &&
                worker2.runs().any { it.state == CliRun.State.RUNNING }
        }

        assertEquals(listOf(repo1), worker1.runs().map { it.repo })
        assertEquals(listOf(repo2), worker2.runs().map { it.repo })

        worker1.cli.release()
        worker2.cli.release()

        cluster.awaitQuiescent(repo1, CredentialScope.Installation(100))
        cluster.awaitQuiescent(repo2, CredentialScope.Installation(200))

        assertTrue(worker1.runs().all { it.state == CliRun.State.COMPLETED })
        assertTrue(worker2.runs().all { it.state == CliRun.State.COMPLETED })
    }
}
