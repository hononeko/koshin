package dev.hononeko.koshin.e2e

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.model.TriggerReason
import dev.hononeko.koshin.core.ports.ReleaseResult
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.seconds

/** Sweeper and manual trigger API ([POST /api/v1/trigger]) against real NATS and Valkey. */
class TriggerApiIntegrationTest {
    private val cluster = E2eCluster()
    private val repo = RepoRef(Platform.GITHUB, "hononeko/koshin")

    @AfterEach
    fun tearDown() {
        cluster.close()
    }

    @Test
    fun `unauthorized trigger requests return 401 and enqueue nothing`() {
        val proxy = cluster.startProxy()
        val payload = """{"repositories":[{"platform":"github","repository":"hononeko/koshin"}]}"""

        val responseNoAuth = proxy.triggerRaw(payload, token = null)
        assertEquals(401, responseNoAuth.statusCode())

        val responseBadAuth = proxy.triggerRaw(payload, token = "wrong-bearer-token")
        assertEquals(401, responseBadAuth.statusCode())

        assertEquals(0, cluster.publishedCount())
    }

    @Test
    fun `batch trigger enqueues repositories, coalesces duplicate queued requests and workers execute jobs`() {
        val proxy = cluster.startProxy()
        val repoA = RepoRef(Platform.GITHUB, "hononeko/repo-a")
        val repoB = RepoRef(Platform.GITHUB, "hononeko/repo-b")

        val res = proxy.trigger(listOf(repoA, repoB))
        assertEquals("accepted", res.status)
        assertEquals(2, res.enqueuedCount)
        assertEquals(0, res.coalescedCount)
        assertEquals(2, cluster.publishedCount())

        // repoA is already queued; a second trigger for it coalesces rather than publishing duplicate
        val dup = proxy.trigger(listOf(repoA))
        assertEquals(0, dup.enqueuedCount)
        assertEquals(1, dup.coalescedCount)
        assertEquals(2, cluster.publishedCount(), "no additional job published for queued repo")

        val worker = cluster.startWorker("worker-1")
        cluster.awaitQuiescent(repoA)
        cluster.awaitQuiescent(repoB)

        assertEquals(2, worker.runs().size)
        assertTrue(worker.runs().all { it.state == CliRun.State.COMPLETED })
        assertEquals(setOf(repoA, repoB), worker.runs().map { it.repo }.toSet())
        assertTrue(worker.deliveries.all { it.event.triggerReason == TriggerReason.SCHEDULE })
        assertTrue(worker.deliveries.all { it.settlement == Settlement.Ack })
    }

    @Test
    fun `triggering a running repository marks it dirty and schedules a single follow-up run`() {
        val proxy = cluster.startProxy()
        val worker = cluster.startWorker("worker-1", runTime = 2.seconds)

        val first = proxy.trigger(listOf(repo))
        assertEquals(1, first.enqueuedCount)
        assertEquals(0, first.coalescedCount)

        awaitUntil("the worker to start the first run") { worker.runs().isNotEmpty() }

        repeat(2) {
            val midRun = proxy.trigger(listOf(repo))
            assertEquals(0, midRun.enqueuedCount)
            assertEquals(1, midRun.coalescedCount)
        }
        assertTrue(cluster.isDirty(repo))
        assertEquals(1, cluster.publishedCount(), "proxy marks dirty instead of publishing")

        cluster.awaitQuiescent(repo)
        assertEquals(ReleaseResult.DIRTY, worker.coordination.releases.first().result)

        val runs = worker.runs()
        assertEquals(2, runs.size, "initial run + one follow-up")
        assertTrue(runs.all { it.state == CliRun.State.COMPLETED })

        val deliveries = worker.deliveries
        assertEquals(2, deliveries.size)
        assertEquals(TriggerReason.SCHEDULE, deliveries[0].event.triggerReason)
        assertEquals(TriggerReason.DIRTY_FOLLOWUP, deliveries[1].event.triggerReason)
    }

    private val JsonObject.enqueuedCount: Int
        get() = getValue("enqueued").jsonPrimitive.int

    private val JsonObject.coalescedCount: Int
        get() = getValue("coalesced").jsonPrimitive.int

    private val JsonObject.status: String
        get() = getValue("status").jsonPrimitive.content
}
