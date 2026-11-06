package dev.hononeko.koshin.e2e

import dev.hononeko.koshin.core.model.CredentialScope
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RepoRef
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/** MaxDeliver delivery exhaustion, DLQ forwarding, and self-healing of Valkey queued state. */
class DlqRecoveryIntegrationTest {
    private val cluster = E2eCluster(maxDeliver = 2L, exhaustedSweepInterval = 200.milliseconds)
    private val repo = RepoRef(Platform.GITHUB, "hononeko/koshin")

    @AfterEach
    fun tearDown() {
        cluster.close()
    }

    @Test
    fun `an exhausted delivery moved to DLQ by housekeeping clears queued mark so new webhooks enqueue`() {
        val proxy = cluster.startProxy()
        val scope = CredentialScope.Token(Platform.GITHUB)

        assertTrue(proxy.push(repo).enqueued)
        assertEquals(1, cluster.publishedCount())
        assertTrue(cluster.isQueued(repo))

        // Exhaust deliveries up to maxDeliver (2) by fetching and NAKing
        cluster.consumer().use { consumer ->
            repeat(cluster.natsConfig.maxDeliver.toInt()) {
                val delivery = checkNotNull(runBlocking { consumer.fetchNext(scope, 5.seconds) })
                assertEquals(repo, delivery.event.repo)
                runBlocking { delivery.nack(Duration.ZERO) }
            }
        }

        // Proxy housekeeping forwards the exhausted job to the DLQ and clears the queued mark
        val deadLetter = awaitValue("the exhausted job to reach the DLQ") { cluster.deadLetters().firstOrNull() }
        assertEquals(repo, deadLetter.repo)

        awaitUntil("the Valkey queued mark to be cleared") { !cluster.isQueued(repo) }

        // A new webhook for the same repository must now be accepted as enqueued, not stuck as coalesced
        val secondPush = proxy.push(repo)
        assertTrue(secondPush.enqueued, "new push webhook should be accepted after DLQ cleanup")
        assertEquals(2, cluster.publishedCount())

        val worker = cluster.startWorker("worker-1")
        cluster.awaitQuiescent(repo)

        assertEquals(1, worker.runs().size)
        assertEquals(CliRun.State.COMPLETED, worker.runs().single().state)
        assertEquals(Settlement.Ack, worker.deliveries.single().settlement)
    }

    private val JsonObject.enqueued: Boolean
        get() = getValue("enqueued").jsonPrimitive.boolean
}
