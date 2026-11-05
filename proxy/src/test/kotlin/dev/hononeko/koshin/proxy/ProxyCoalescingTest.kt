package dev.hononeko.koshin.proxy

import dev.hononeko.koshin.adapters.valkey.ValkeyConfig
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.ports.AcquireResult
import dev.hononeko.koshin.core.ports.EnqueueDecision
import dev.hononeko.koshin.core.ports.JobQueuePublisher
import dev.hononeko.koshin.core.ports.ReleaseResult
import dev.hononeko.koshin.core.ports.RepoCoordinationPort
import dev.hononeko.koshin.core.usecase.IngestConfig
import io.ktor.client.HttpClient
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.testing.testApplication
import io.lettuce.core.RedisConnectionException
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.HexFormat
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/** Pre-queue coalescing at the proxy, and failing open when Valkey is down or slow. */
class ProxyCoalescingTest {
    private val ghSecret = "gh-secret"
    private val glSecret = "gl-secret"
    private val triggerToken = "trigger-token"
    private val config = ProxyConfig(
        port = 0,
        platforms = setOf(Platform.GITHUB, Platform.GITLAB),
        githubWebhookSecret = ghSecret,
        gitlabWebhookSecret = glSecret,
        internalTriggerToken = triggerToken,
        publishRetryBaseDelay = 5.milliseconds,
        publishRetryBudget = 1.seconds,
        ingest = IngestConfig(coalesceTimeout = 100.milliseconds),
    )
    private val published = CopyOnWriteArrayList<RenovateJobEvent>()
    private val publisher = object : JobQueuePublisher {
        override suspend fun publish(event: RenovateJobEvent) {
            published += event
        }

        override suspend fun publishToDeadLetter(event: RenovateJobEvent) = error("not used")
    }

    /** Decides each enqueue with [decide]; the worker's methods aren't used by the proxy. */
    private class FakeCoordination(private val decide: suspend (RepoRef) -> EnqueueDecision) : RepoCoordinationPort {
        val cleared = CopyOnWriteArrayList<RepoRef>()

        override suspend fun enqueueOrCoalesce(repo: RepoRef, queuedTtl: Duration, dirtyTtl: Duration) = decide(repo)

        override suspend fun clearQueued(repo: RepoRef) {
            cleared += repo
        }

        override suspend fun acquireOrMarkDirty(repo: RepoRef, token: String, lockTtl: Duration, dirtyTtl: Duration) =
            AcquireResult.ACQUIRED

        override suspend fun extendLock(repo: RepoRef, token: String, ttl: Duration) = true

        override suspend fun releaseLockAndTakeDirty(repo: RepoRef, token: String, queuedTtl: Duration?) =
            ReleaseResult.CLEAN

        override suspend fun releaseLock(repo: RepoRef, token: String, queuedTtl: Duration?) = true
    }

    private suspend fun HttpClient.githubPush(repo: String): HttpResponse {
        val payload = """{"ref":"refs/heads/main","repository":{"full_name":"$repo","default_branch":"main"},""" +
            """"sender":{"login":"octocat","type":"User"}}"""
        val mac = Mac.getInstance("HmacSHA256").apply { init(SecretKeySpec(ghSecret.toByteArray(), "HmacSHA256")) }
        return post("/webhooks/github") {
            header("X-GitHub-Event", "push")
            header("X-GitHub-Delivery", "delivery-$repo")
            header("X-Hub-Signature-256", "sha256=" + HexFormat.of().formatHex(mac.doFinal(payload.toByteArray())))
            contentType(ContentType.Application.Json)
            setBody(payload)
        }
    }

    private suspend fun HttpClient.gitlabMerge(repo: String): HttpResponse = post("/webhooks/gitlab") {
        header("X-Gitlab-Token", glSecret)
        header("X-Gitlab-Event", "Merge Request Hook")
        contentType(ContentType.Application.Json)
        setBody(
            """{"object_kind":"merge_request","project":{"path_with_namespace":"$repo"},""" +
                """"object_attributes":{"action":"merge"}}""",
        )
    }

    private suspend fun HttpClient.trigger(vararg repos: String): HttpResponse = post("/api/v1/trigger") {
        header("Authorization", "Bearer $triggerToken")
        contentType(ContentType.Application.Json)
        setBody("""{"repositories":[${repos.joinToString(",") { """{"repository":"$it"}""" }}]}""")
    }

    private fun assertFailsOpen(coordination: RepoCoordinationPort) = testApplication {
        val observer = LoggingIngestObserver()
        application {
            proxyModule(config, publisher, coordination = coordination, ingestObserver = observer)
        }

        assertEquals(HttpStatusCode.Accepted, client.githubPush("hononeko/a").status)
        assertEquals(HttpStatusCode.Accepted, client.gitlabMerge("group/sub/b").status)
        val trigger = client.trigger("hononeko/c")
        assertEquals(HttpStatusCode.Accepted, trigger.status)
        assertEquals(1, ProxyJson.decodeFromString(TriggerResponse.serializer(), trigger.bodyAsText()).enqueued)

        assertEquals(
            listOf("github:hononeko/a", "gitlab:group/sub/b", "github:hononeko/c"),
            published.map { it.repo.toString() },
        )
        assertEquals(3, observer.failOpenCount)
    }

    @Test
    fun `webhooks and triggers fail open while Valkey isn't connected`() {
        ProxyValkey { awaitCancellation() }.use { assertFailsOpen(it.coordination) }
    }

    @Test
    fun `webhooks and triggers fail open when Valkey fails`() {
        assertFailsOpen(FakeCoordination { error("READONLY You can't write against a read only replica") })
    }

    @Test
    fun `webhooks and triggers fail open when Valkey is slower than COALESCE_TIMEOUT_MS`() {
        assertFailsOpen(
            FakeCoordination {
                delay(10.seconds)
                EnqueueDecision.ALREADY_QUEUED
            },
        )
    }

    @Test
    fun `webhooks for a queued or running repo are coalesced without publishing`() = testApplication {
        val decisions = mapOf(
            "hononeko/queued" to EnqueueDecision.ALREADY_QUEUED,
            "hononeko/running" to EnqueueDecision.MARKED_DIRTY,
        )
        application {
            proxyModule(config, publisher, coordination = FakeCoordination { decisions.getValue(it.repository) })
        }

        for ((repo, reason) in listOf("hononeko/queued" to "already queued", "hononeko/running" to "in progress")) {
            val response = client.githubPush(repo)
            assertEquals(HttpStatusCode.Accepted, response.status)
            val body = ProxyJson.decodeFromString(WebhookResponse.serializer(), response.bodyAsText())
            assertEquals("coalesced", body.status)
            assertFalse(body.enqueued)
            assertEquals("delivery-$repo", body.deliveryId)
            assertTrue(body.reason!!.contains(reason), body.reason)
        }
        assertTrue(published.isEmpty())
    }

    @Test
    fun `a trigger counts the repos it published and those it coalesced`() = testApplication {
        val decisions = mapOf(
            "hononeko/idle" to EnqueueDecision.ENQUEUE,
            "hononeko/queued" to EnqueueDecision.ALREADY_QUEUED,
            "hononeko/running" to EnqueueDecision.MARKED_DIRTY,
        )
        application {
            proxyModule(config, publisher, coordination = FakeCoordination { decisions.getValue(it.repository) })
        }

        val response = client.trigger("hononeko/idle", "hononeko/queued", "hononeko/running")

        assertEquals(HttpStatusCode.Accepted, response.status)
        assertEquals(
            TriggerResponse(status = "accepted", enqueued = 1, coalesced = 2),
            ProxyJson.decodeFromString(TriggerResponse.serializer(), response.bodyAsText()),
        )
        assertEquals(listOf("hononeko/idle"), published.map { it.repo.repository })
    }

    @Test
    fun `a failed publish clears the queued mark and answers 503`() = testApplication {
        val coordination = FakeCoordination { EnqueueDecision.ENQUEUE }
        val failing = object : JobQueuePublisher {
            override suspend fun publish(event: RenovateJobEvent) = error("NATS down")
            override suspend fun publishToDeadLetter(event: RenovateJobEvent) = error("not used")
        }
        application {
            proxyModule(config.copy(publishRetryBudget = Duration.ZERO), failing, coordination = coordination)
        }

        assertEquals(HttpStatusCode.ServiceUnavailable, client.githubPush("hononeko/a").status)
        assertEquals(listOf(RepoRef(Platform.GITHUB, "hononeko/a")), coordination.cleared)
    }

    @Test
    fun `ProxyValkey delegates once connected and clears dead-lettered repos in the background`() = runBlocking {
        val coordination = FakeCoordination { EnqueueDecision.ALREADY_QUEUED }
        var closed = false
        val valkey = ProxyValkey(retryDelay = 1.milliseconds) {
            ProxyValkey.Connection(coordination) { closed = true }
        }
        val repo = RepoRef(Platform.GITHUB, "hononeko/a")
        assertThrows<IllegalStateException> { valkey.coordination.enqueueOrCoalesce(repo, 1.seconds, 1.seconds) }

        valkey.startAsync().join()

        assertTrue(valkey.isConnected)
        assertEquals(EnqueueDecision.ALREADY_QUEUED, valkey.coordination.enqueueOrCoalesce(repo, 1.seconds, 1.seconds))
        valkey.clearQueuedLater(repo)
        val deadline = System.nanoTime() + 5.seconds.inWholeNanoseconds
        while (coordination.cleared.isEmpty() && System.nanoTime() < deadline) delay(10.milliseconds)
        assertEquals(listOf(repo), coordination.cleared)
        valkey.close()
        assertTrue(closed)
    }

    @Test
    fun `ProxyValkey retries a failed connection`() = runBlocking {
        var attempts = 0
        val valkey = ProxyValkey(retryDelay = 1.milliseconds) {
            if (++attempts < 3) error("connection refused")
            ProxyValkey.Connection(FakeCoordination { EnqueueDecision.ENQUEUE })
        }
        valkey.use { it.startAsync().join() }
        assertEquals(3, attempts)
    }

    @Test
    fun `ProxyValkey passes every coordination call through once connected`() = runBlocking {
        val coordination = FakeCoordination { EnqueueDecision.ENQUEUE }
        val valkey = ProxyValkey { ProxyValkey.Connection(coordination) }
        valkey.startAsync().join()
        val repo = RepoRef(Platform.GITHUB, "hononeko/a")
        with(valkey.coordination) {
            assertEquals(AcquireResult.ACQUIRED, acquireOrMarkDirty(repo, "t", 1.seconds, 1.seconds))
            assertTrue(extendLock(repo, "t", 1.seconds))
            assertEquals(ReleaseResult.CLEAN, releaseLockAndTakeDirty(repo, "t", null))
            assertTrue(releaseLock(repo, "t", 1.seconds))
            clearQueued(repo)
        }
        assertEquals(listOf(repo), coordination.cleared)
        valkey.close()
    }

    @Test
    fun `ProxyValkey gives up clearing a mark that fails or hangs`() = runBlocking {
        val calls = AtomicInteger()
        val failing = object : RepoCoordinationPort by FakeCoordination({ EnqueueDecision.ENQUEUE }) {
            override suspend fun clearQueued(repo: RepoRef) {
                if (calls.incrementAndGet() == 1) error("Valkey down") else awaitCancellation()
            }
        }
        val valkey = ProxyValkey(clearTimeout = 50.milliseconds) { ProxyValkey.Connection(failing) }
        valkey.startAsync().join()

        valkey.clearQueuedLater(RepoRef(Platform.GITHUB, "hononeko/a"))
        valkey.clearQueuedLater(RepoRef(Platform.GITHUB, "hononeko/b"))
        val deadline = System.nanoTime() + 5.seconds.inWholeNanoseconds
        while (calls.get() < 2 && System.nanoTime() < deadline) delay(10.milliseconds)
        delay(100.milliseconds)

        assertEquals(2, calls.get())
        valkey.close()
    }

    @Test
    fun `ProxyValkey fails to connect to an unreachable Valkey`() = runBlocking<Unit> {
        val config = ValkeyConfig(valkeyUrl = "redis://127.0.0.1:1", commandTimeout = 1.seconds)
        assertThrows<RedisConnectionException> { ProxyValkey.connect(config) }
    }

    @Test
    fun `the ingest observer counts fail-opens and reports a kept mark`() {
        val observer = LoggingIngestObserver()
        val repo = RepoRef(Platform.GITHUB, "hononeko/a")

        observer.failedOpen(repo, null)
        observer.failedOpen(repo, IllegalStateException("down"))
        observer.queuedMarkKept(repo, null)

        assertEquals(2, observer.failOpenCount)
    }
}
