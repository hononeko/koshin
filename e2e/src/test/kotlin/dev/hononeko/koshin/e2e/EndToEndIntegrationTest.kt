package dev.hononeko.koshin.e2e

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.model.TriggerReason
import dev.hononeko.koshin.core.ports.AcquireResult
import dev.hononeko.koshin.core.ports.ReleaseResult
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * Proxy and workers against real NATS and Valkey, with the short timings from KSH-9:
 * lock TTL 3s, heartbeat 1s, AckWait 5s.
 */
class EndToEndIntegrationTest {
    private val cluster = E2eCluster()
    private val repo = RepoRef(Platform.GITHUB, "hononeko/koshin")

    @AfterEach
    fun tearDown() {
        cluster.close()
    }

    @Test
    fun `webhooks arriving mid-run coalesce into exactly one follow-up run`() {
        val proxy = cluster.startProxy()
        val workers = listOf(
            cluster.startWorker("worker-1", runTime = 2.seconds),
            cluster.startWorker("worker-2", runTime = 2.seconds),
        )

        assertTrue(proxy.push(repo).enqueued)
        val runner = awaitValue("the first run to start") { workers.firstOrNull { it.runs().isNotEmpty() } }
        repeat(4) { assertTrue(proxy.push(repo).enqueued) }

        cluster.awaitQuiescent(repo)
        val other = workers.single { it !== runner }
        assertEquals(4, other.coordination.acquires.count { it.result == AcquireResult.COALESCED })
        assertEquals(ReleaseResult.DIRTY, runner.coordination.releases.first().result)

        assertEquals(2, cluster.runs.size, "initial run + one follow-up")
        assertTrue(cluster.runs.all { it.state == CliRun.State.COMPLETED })
        val deliveries = workers.flatMap { it.deliveries }
        assertEquals(6, deliveries.size, "5 webhooks + 1 follow-up")
        assertTrue(deliveries.all { it.settlement == Settlement.Ack })
        assertEquals(1, deliveries.count { it.event.triggerReason == TriggerReason.DIRTY_FOLLOWUP })
    }

    @Test
    fun `a crashed worker's job is redelivered after its lock expires and completed by the other worker`() {
        val proxy = cluster.startProxy()
        val worker1 = cluster.startWorker("worker-1").apply { cli.hold() }
        assertTrue(proxy.push(repo).enqueued)
        awaitUntil("worker-1 to start the run") { worker1.runs().isNotEmpty() }

        val worker2 = cluster.startWorker("worker-2")
        assertTrue(proxy.push(repo).enqueued)
        awaitUntil("worker-2 to coalesce the second webhook") { worker2.coordination.acquires.isNotEmpty() }
        assertEquals(AcquireResult.COALESCED, worker2.coordination.acquires.single().result)

        worker1.crash()
        awaitUntil("the crashed worker's lock to expire") { cluster.lockHolder(repo) == null }
        val lockExpiredAt = TimeSource.Monotonic.markNow()

        val redelivery = awaitValue("the redelivery to worker-2") { worker2.deliveries.firstOrNull { it.attempt == 2 } }
        assertTrue(lockExpiredAt < redelivery.receivedAt, "lock must expire before AckWait redelivers the job")
        cluster.awaitQuiescent(repo)

        assertNull(worker1.deliveries.single().settlement, "a crashed worker settles nothing")
        assertEquals(Settlement.Ack, redelivery.settlement)
        assertEquals(
            listOf(AcquireResult.COALESCED, AcquireResult.ACQUIRED, AcquireResult.ACQUIRED),
            worker2.coordination.acquires.map { it.result },
        )
        assertEquals(
            listOf(ReleaseResult.DIRTY, ReleaseResult.CLEAN),
            worker2.coordination.releases.map { it.result },
            "the coalesced webhook's dirty flag turns into a follow-up",
        )
        assertEquals(listOf(CliRun.State.KILLED), worker1.runs().map { it.state })
        assertEquals(List(2) { CliRun.State.COMPLETED }, worker2.runs().map { it.state })
    }

    @Test
    fun `a redelivered job that hits a held lock is NAKed and completes once the lock frees`() {
        val proxy = cluster.startProxy()
        val holderToken = "worker-outside-the-suite"
        val held = runBlocking {
            cluster.coordination.acquireOrMarkDirty(repo, holderToken, 30.seconds, cluster.jobConfig.dirtyTtl)
        }
        assertEquals(AcquireResult.ACQUIRED, held)

        assertTrue(proxy.push(repo).enqueued)
        cluster.consumer().use { consumer ->
            runBlocking { checkNotNull(consumer.fetchNext(Platform.GITHUB, 5.seconds)).nack(Duration.ZERO) }
        }

        val worker = cluster.startWorker("worker-1")
        awaitUntil("the redelivery to be NAKed") { worker.deliveries.any { it.settlement is Settlement.Nak } }
        val whileHeld = worker.deliveries.toList()
        runBlocking { assertTrue(cluster.coordination.releaseLock(repo, holderToken)) }

        assertTrue(whileHeld.all { it.attempt >= 2 })
        assertTrue(whileHeld.none { it.settlement == Settlement.Ack }, "never ACKed while the lock is held")
        val collisionNak = Settlement.Nak(cluster.jobConfig.lockCollisionRetryDelay)
        assertTrue(whileHeld.mapNotNull { it.settlement }.all { it == collisionNak })

        cluster.awaitQuiescent(repo)
        val webhook = worker.deliveries.filter { it.event.triggerReason == TriggerReason.WEBHOOK }
        assertEquals(Settlement.Ack, webhook.last().settlement)
        assertTrue(webhook.dropLast(1).all { it.settlement is Settlement.Nak })
        assertTrue(worker.runs().isNotEmpty())
        assertTrue(worker.runs().all { it.state == CliRun.State.COMPLETED })
    }

    @Test
    fun `a stale holder's CLI is killed and its release keeps the new holder's lock`() {
        val proxy = cluster.startProxy()
        val worker1 = cluster.startWorker("worker-1").apply { cli.hold() }
        assertTrue(proxy.push(repo).enqueued)
        awaitUntil("worker-1 to start the run") { worker1.runs().isNotEmpty() }

        val worker2 = cluster.startWorker("worker-2").apply { cli.hold() }
        // Neither the lock nor the NATS ack deadline is renewed: the lock expires, then the job is redelivered.
        worker1.stall.begin()
        awaitUntil("worker-2 to take over the run") { worker2.runs().isNotEmpty() }
        val worker2Token = worker2.coordination.acquires.single { it.result == AcquireResult.ACQUIRED }.token
        assertEquals(worker2Token, cluster.lockHolder(repo))

        worker1.stall.end()
        awaitUntil("worker-1 to give up the lock") { worker1.coordination.unlocks.isNotEmpty() }
        assertEquals(CliRun.State.KILLED, worker1.runs().single().state)
        assertFalse(worker1.coordination.unlocks.single().released, "the stale release must not delete the lock")
        assertEquals(worker2Token, cluster.lockHolder(repo))
        assertTrue(worker1.coordination.releases.isEmpty())

        // worker-1 is back to polling; without this, a follow-up it picks up would block on the old hold.
        worker1.cli.release()
        worker2.cli.release()
        cluster.awaitQuiescent(repo)
        assertEquals(CliRun.State.COMPLETED, worker2.runs().first().state)
        assertEquals(Settlement.Ack, worker2.deliveries.first { it.attempt == 2 }.settlement)
    }

    @Test
    fun `renovate branch pushes and bot pushes enqueue nothing`() {
        val proxy = cluster.startProxy()
        cluster.startWorker("worker-1")

        assertFalse(proxy.push(repo, ref = "refs/heads/renovate/kotlin-2.x").enqueued)
        assertFalse(proxy.push(repo, sender = "renovate[bot]", senderType = "Bot").enqueued)
        assertEquals(0, cluster.publishedCount())

        // Control: the same pipeline does enqueue and run a human push.
        assertTrue(proxy.push(repo).enqueued)
        cluster.awaitQuiescent(repo)
        assertEquals(1, cluster.publishedCount())
        assertEquals(1, cluster.runs.size)
    }

    @Test
    fun `SIGTERM NAKs the in-flight job, keeps the dirty flag and the other worker completes it`() {
        val proxy = cluster.startProxy()
        val worker1 = cluster.startWorker("worker-1").apply { cli.hold() }
        assertTrue(proxy.push(repo).enqueued)
        awaitUntil("worker-1 to start the run") { worker1.runs().isNotEmpty() }

        val worker2 = cluster.startWorker("worker-2").apply { cli.hold() }
        assertTrue(proxy.push(repo).enqueued)
        awaitUntil("worker-2 to coalesce the second webhook") { worker2.coordination.acquires.isNotEmpty() }

        worker1.sigterm()
        assertEquals(Settlement.Nak(Duration.ZERO), worker1.deliveries.single().settlement)
        assertEquals(CliRun.State.KILLED, worker1.runs().single().state)
        assertTrue(cluster.isDirty(repo), "the dirty flag survives the shutdown")

        awaitUntil("worker-2 to pick up the NAKed job") { worker2.runs().isNotEmpty() }
        worker2.cli.release()
        cluster.awaitQuiescent(repo)

        assertEquals(Settlement.Ack, worker2.deliveries.single { it.attempt == 2 }.settlement)
        assertEquals(listOf(ReleaseResult.DIRTY, ReleaseResult.CLEAN), worker2.coordination.releases.map { it.result })
        assertEquals(List(2) { CliRun.State.COMPLETED }, worker2.runs().map { it.state })
    }

    private val JsonObject.enqueued: Boolean
        get() = getValue("enqueued").jsonPrimitive.boolean
}
