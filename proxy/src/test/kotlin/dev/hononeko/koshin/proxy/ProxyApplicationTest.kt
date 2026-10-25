package dev.hononeko.koshin.proxy

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.model.TriggerReason
import dev.hononeko.koshin.core.ports.JobQueuePublisher
import dev.hononeko.koshin.core.ports.RepositoryDiscoveryPort
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.content.OutgoingContent
import io.ktor.http.contentType
import io.ktor.server.testing.testApplication
import io.ktor.utils.io.ByteWriteChannel
import io.ktor.utils.io.writeFully
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.HexFormat
import java.util.concurrent.CopyOnWriteArrayList
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class ProxyApplicationTest {

    private val ghSecret = "test-gh-webhook-secret"
    private val glSecret = "test-gl-webhook-secret"
    private val triggerToken = "test-internal-trigger-token"
    private val json = Json { ignoreUnknownKeys = true }

    private fun testConfig(
        platforms: Set<Platform> = setOf(Platform.GITHUB, Platform.GITLAB),
        maxRequestBodyBytes: Int = ProxyConfig.DEFAULT_MAX_REQUEST_BODY_BYTES,
        publishRetryBaseDelay: Duration = 5.milliseconds,
        publishRetryBudget: Duration = 5.seconds,
    ): ProxyConfig = ProxyConfig(
        port = 0,
        http2Enabled = true,
        platforms = platforms,
        githubWebhookSecret = ghSecret,
        gitlabWebhookSecret = glSecret,
        internalTriggerToken = triggerToken,
        publishRetryBaseDelay = publishRetryBaseDelay,
        publishRetryBudget = publishRetryBudget,
        maxRequestBodyBytes = maxRequestBodyBytes,
    )

    private fun signGitHub(payload: String, secret: String = ghSecret): String {
        val mac = Mac.getInstance("HmacSHA256")
        mac.init(SecretKeySpec(secret.toByteArray(Charsets.UTF_8), "HmacSHA256"))
        val digest = mac.doFinal(payload.toByteArray(Charsets.UTF_8))
        return "sha256=" + HexFormat.of().formatHex(digest)
    }

    private class RecordingPublisher : JobQueuePublisher {
        val published = CopyOnWriteArrayList<RenovateJobEvent>()
        val deadLettered = CopyOnWriteArrayList<RenovateJobEvent>()

        override suspend fun publish(event: RenovateJobEvent) {
            published.add(event)
        }

        override suspend fun publishToDeadLetter(event: RenovateJobEvent) {
            deadLettered.add(event)
        }
    }

    @Test
    fun `healthz returns 200 OK without NATS and readyz reflects readiness check`() = testApplication {
        var ready = false
        val publisher = RecordingPublisher()
        application {
            proxyModule(
                config = testConfig(),
                publisher = publisher,
                readinessCheck = { ready },
            )
        }

        val healthRes = client.get("/healthz")
        assertEquals(HttpStatusCode.OK, healthRes.status)
        assertTrue(healthRes.bodyAsText().contains("\"status\":\"ok\""))

        val notReadyRes = client.get("/readyz")
        assertEquals(HttpStatusCode.ServiceUnavailable, notReadyRes.status)

        ready = true
        val readyRes = client.get("/readyz")
        assertEquals(HttpStatusCode.OK, readyRes.status)
        assertTrue(readyRes.bodyAsText().contains("\"status\":\"ready\""))
    }

    @Test
    fun `GitHub webhook accepts valid HMAC and rejects invalid or missing HMAC with 403`() = testApplication {
        val publisher = RecordingPublisher()
        application {
            proxyModule(config = testConfig(), publisher = publisher)
        }

        val payload = """
            {
              "ref": "refs/heads/main",
              "repository": {
                "full_name": "hononeko/koshin",
                "default_branch": "main"
              },
              "sender": {
                "login": "octocat",
                "type": "User"
              }
            }
        """.trimIndent()

        val missingSigRes = client.post("/webhooks/github") {
            header("X-GitHub-Event", "push")
            header("X-GitHub-Delivery", "gh-deliv-100")
            contentType(ContentType.Application.Json)
            setBody(payload)
        }
        assertEquals(HttpStatusCode.Forbidden, missingSigRes.status)
        assertTrue(publisher.published.isEmpty())

        val badSigRes = client.post("/webhooks/github") {
            header("X-Hub-Signature-256", signGitHub(payload, "wrong-secret"))
            header("X-GitHub-Event", "push")
            header("X-GitHub-Delivery", "gh-deliv-100")
            contentType(ContentType.Application.Json)
            setBody(payload)
        }
        assertEquals(HttpStatusCode.Forbidden, badSigRes.status)
        assertTrue(publisher.published.isEmpty())

        val validRes = client.post("/webhooks/github") {
            header("X-Hub-Signature-256", signGitHub(payload))
            header("X-GitHub-Event", "push")
            header("X-GitHub-Delivery", "gh-deliv-100")
            contentType(ContentType.Application.Json)
            setBody(payload)
        }
        assertEquals(HttpStatusCode.Accepted, validRes.status)
        assertEquals(1, publisher.published.size)
        val event = publisher.published.single()
        assertEquals(RepoRef(Platform.GITHUB, "hononeko/koshin"), event.repo)
        assertEquals("gh-deliv-100", event.deliveryId)
        assertEquals(TriggerReason.WEBHOOK, event.triggerReason)
    }

    @Test
    fun `GitHub webhook returns 202 with enqueued false when filtered by self-trigger rules`() = testApplication {
        val publisher = RecordingPublisher()
        application {
            proxyModule(config = testConfig(), publisher = publisher)
        }

        val botPushPayload = """
            {
              "ref": "refs/heads/main",
              "repository": {
                "full_name": "hononeko/koshin",
                "default_branch": "main"
              },
              "sender": {
                "login": "renovate[bot]",
                "type": "Bot"
              }
            }
        """.trimIndent()

        val res = client.post("/webhooks/github") {
            header("X-Hub-Signature-256", signGitHub(botPushPayload))
            header("X-GitHub-Event", "push")
            header("X-GitHub-Delivery", "gh-deliv-bot")
            contentType(ContentType.Application.Json)
            setBody(botPushPayload)
        }
        assertEquals(HttpStatusCode.Accepted, res.status)
        assertTrue(res.bodyAsText().contains("\"enqueued\":false"))
        assertTrue(publisher.published.isEmpty())
    }

    @Test
    fun `GitLab webhook verifies X-Gitlab-Token in constant time and handles events`() = testApplication {
        val publisher = RecordingPublisher()
        application {
            proxyModule(config = testConfig(), publisher = publisher)
        }

        val payload = """
            {
              "object_kind": "merge_request",
              "project": {
                "path_with_namespace": "gitlab-org/subgroup/koshin"
              },
              "object_attributes": {
                "action": "merge"
              }
            }
        """.trimIndent()

        val badTokenRes = client.post("/webhooks/gitlab") {
            header("X-Gitlab-Token", "wrong-token")
            header("X-Gitlab-Event", "Merge Request Hook")
            contentType(ContentType.Application.Json)
            setBody(payload)
        }
        assertEquals(HttpStatusCode.Forbidden, badTokenRes.status)
        assertTrue(publisher.published.isEmpty())

        val validRes = client.post("/webhooks/gitlab") {
            header("X-Gitlab-Token", glSecret)
            header("X-Gitlab-Event", "Merge Request Hook")
            header("X-Gitlab-Event-UUID", "gl-uuid-42")
            contentType(ContentType.Application.Json)
            setBody(payload)
        }
        assertEquals(HttpStatusCode.Accepted, validRes.status)
        assertEquals(1, publisher.published.size)
        val event = publisher.published.single()
        assertEquals(RepoRef(Platform.GITLAB, "gitlab-org/subgroup/koshin"), event.repo)
        assertEquals("gl-uuid-42", event.deliveryId)
        assertEquals(TriggerReason.WEBHOOK, event.triggerReason)

        val ignoredPayload = """
            {
              "object_kind": "merge_request",
              "project": { "path_with_namespace": "gitlab-org/subgroup/koshin" },
              "object_attributes": { "action": "open" }
            }
        """.trimIndent()
        val ignoredRes = client.post("/webhooks/gitlab") {
            header("X-Gitlab-Token", glSecret)
            header("X-Gitlab-Event", "Merge Request Hook")
            contentType(ContentType.Application.Json)
            setBody(ignoredPayload)
        }
        assertEquals(HttpStatusCode.Accepted, ignoredRes.status)
        assertTrue(ignoredRes.bodyAsText().contains("\"enqueued\":false"))
        assertEquals(1, publisher.published.size)
    }

    @Test
    fun `disabled platforms return 403 Forbidden on webhook endpoints`() = testApplication {
        val publisher = RecordingPublisher()
        application {
            proxyModule(
                config = testConfig(platforms = setOf(Platform.GITHUB)),
                publisher = publisher,
            )
        }

        val gitlabRes = client.post("/webhooks/gitlab") {
            header("X-Gitlab-Token", glSecret)
            setBody("{}")
        }
        assertEquals(HttpStatusCode.Forbidden, gitlabRes.status)
    }

    @Test
    fun `disabled GitHub platform returns 403 Forbidden on GitHub webhook endpoint`() = testApplication {
        val publisher = RecordingPublisher()
        application {
            proxyModule(
                config = testConfig(platforms = setOf(Platform.GITLAB)),
                publisher = publisher,
            )
        }

        val githubRes = client.post("/webhooks/github") {
            header("X-Hub-Signature-256", signGitHub("{}"))
            setBody("{}")
        }
        assertEquals(HttpStatusCode.Forbidden, githubRes.status)
    }

    @Test
    fun `trigger API validates Bearer token, deduplicates, and queues batch repositories with fresh UUIDs`() =
        testApplication {
            val publisher = RecordingPublisher()
            application {
                proxyModule(config = testConfig(), publisher = publisher)
            }

            val unauthRes = client.post("/api/v1/trigger") {
                contentType(ContentType.Application.Json)
                setBody("""{"discover": true}""")
            }
            assertEquals(HttpStatusCode.Unauthorized, unauthRes.status)

            val wrongTokenRes = client.post("/api/v1/trigger") {
                header("Authorization", "Bearer wrong-token")
                contentType(ContentType.Application.Json)
                setBody("""{"discover": true}""")
            }
            assertEquals(HttpStatusCode.Unauthorized, wrongTokenRes.status)

            val batchBody = """
                {
                  "repositories": [
                    {"platform": "github", "repository": "hononeko/koshin"},
                    {"platform": "GITHUB", "repository": "hononeko/koshin"},
                    {"platform": "gitlab", "repository": "gitlab-org/sub/repo"}
                  ]
                }
            """.trimIndent()
            val batchRes = client.post("/api/v1/trigger") {
                header("Authorization", "Bearer $triggerToken")
                contentType(ContentType.Application.Json)
                setBody(batchBody)
            }
            assertEquals(HttpStatusCode.Accepted, batchRes.status)
            val batchRespObj = json.decodeFromString(TriggerResponse.serializer(), batchRes.bodyAsText())
            assertEquals(2, batchRespObj.enqueued)
            assertEquals(2, publisher.published.size)
            assertTrue(publisher.published.all { it.triggerReason == TriggerReason.SCHEDULE })
            assertNotEquals(publisher.published[0].deliveryId, publisher.published[1].deliveryId)
        }

    @Test
    fun `trigger API with discover true filters enabled platforms and handles discovery errors`() = testApplication {
        val publisher = RecordingPublisher()
        val discoveryPort = object : RepositoryDiscoveryPort {
            override suspend fun discover(): List<RepoRef> = listOf(
                RepoRef(Platform.GITHUB, "hononeko/discovered-1"),
                RepoRef(Platform.GITLAB, "gitlab-org/discovered-2"),
            )
        }

        application {
            proxyModule(
                config = testConfig(platforms = setOf(Platform.GITHUB)),
                publisher = publisher,
                discoveryPort = discoveryPort,
            )
        }

        // Only enabled platform (GITHUB) from discovery is enqueued
        val discoverRes = client.post("/api/v1/trigger") {
            header("Authorization", "Bearer $triggerToken")
            contentType(ContentType.Application.Json)
            setBody("""{"discover": true}""")
        }
        assertEquals(HttpStatusCode.Accepted, discoverRes.status)
        val discoverRespObj = json.decodeFromString(TriggerResponse.serializer(), discoverRes.bodyAsText())
        assertEquals(1, discoverRespObj.enqueued)
        assertEquals(listOf(RepoRef(Platform.GITHUB, "hononeko/discovered-1")), publisher.published.map { it.repo })

        // Explicit disabled platform in request -> 400 Bad Request
        val disabledPlatformRes = client.post("/api/v1/trigger") {
            header("Authorization", "Bearer $triggerToken")
            contentType(ContentType.Application.Json)
            setBody("""{"repositories":[{"platform":"gitlab","repository":"gitlab-org/repo"}]}""")
        }
        assertEquals(HttpStatusCode.BadRequest, disabledPlatformRes.status)

        // Empty trigger request -> 400 Bad Request
        val emptyTriggerRes = client.post("/api/v1/trigger") {
            header("Authorization", "Bearer $triggerToken")
            contentType(ContentType.Application.Json)
            setBody("""{"repositories":[],"discover":false}""")
        }
        assertEquals(HttpStatusCode.BadRequest, emptyTriggerRes.status)
    }

    @Test
    fun `trigger API returns 503 when discovery is unconfigured and 500 when discovery throws`() = testApplication {
        var shouldThrow = false
        val failingDiscovery = object : RepositoryDiscoveryPort {
            override suspend fun discover(): List<RepoRef> {
                if (shouldThrow) {
                    throw IOException("Failed to read sweep file")
                }
                return emptyList()
            }
        }

        val publisher = RecordingPublisher()
        application {
            proxyModule(
                config = testConfig(),
                publisher = publisher,
                discoveryPort = failingDiscovery,
            )
        }

        shouldThrow = true
        val errorRes = client.post("/api/v1/trigger") {
            header("Authorization", "Bearer $triggerToken")
            contentType(ContentType.Application.Json)
            setBody("""{"discover": true}""")
        }
        assertEquals(HttpStatusCode.InternalServerError, errorRes.status)
    }

    @Test
    fun `trigger API returns 503 when discoveryPort is null`() = testApplication {
        val publisher = RecordingPublisher()
        application {
            proxyModule(
                config = testConfig(),
                publisher = publisher,
                discoveryPort = null,
            )
        }

        val res = client.post("/api/v1/trigger") {
            header("Authorization", "Bearer $triggerToken")
            contentType(ContentType.Application.Json)
            setBody("""{"discover": true}""")
        }
        assertEquals(HttpStatusCode.ServiceUnavailable, res.status)
    }

    @Test
    fun `oversized request bodies return 413 Payload Too Large with Content-Length or chunked`() = testApplication {
        val publisher = RecordingPublisher()
        application {
            proxyModule(
                config = testConfig(maxRequestBodyBytes = 64),
                publisher = publisher,
            )
        }

        val oversizedPayload = "x".repeat(128)

        // Unauthenticated GitHub webhook with Content-Length > 64 -> 413 Payload Too Large
        val ghOversizedRes = client.post("/webhooks/github") {
            setBody(oversizedPayload)
        }
        assertEquals(HttpStatusCode.PayloadTooLarge, ghOversizedRes.status)

        // Chunked / streaming request without Content-Length exceeding maxRequestBodyBytes -> 413 Payload Too Large
        val chunkedBody = object : OutgoingContent.WriteChannelContent() {
            override val contentLength: Long? = null
            override suspend fun writeTo(channel: ByteWriteChannel) {
                val bytes = oversizedPayload.toByteArray(Charsets.UTF_8)
                channel.writeFully(bytes, 0, bytes.size)
            }
        }
        val ghChunkedRes = client.post("/webhooks/github") {
            setBody(chunkedBody)
        }
        assertEquals(HttpStatusCode.PayloadTooLarge, ghChunkedRes.status)

        // GitLab oversized -> 413
        val glOversizedRes = client.post("/webhooks/gitlab") {
            header("X-Gitlab-Token", glSecret)
            header(HttpHeaders.ContentLength, "200")
            setBody(oversizedPayload)
        }
        assertEquals(HttpStatusCode.PayloadTooLarge, glOversizedRes.status)

        // Trigger oversized -> 413
        val triggerOversizedRes = client.post("/api/v1/trigger") {
            header("Authorization", "Bearer $triggerToken")
            setBody(oversizedPayload)
        }
        assertEquals(HttpStatusCode.PayloadTooLarge, triggerOversizedRes.status)
    }

    @Test
    fun `returns 400 Bad Request on malformed JSON or invalid repository format`() = testApplication {
        val publisher = RecordingPublisher()
        application {
            proxyModule(config = testConfig(), publisher = publisher)
        }

        val malformedGh = "not-json"
        val ghMalformedRes = client.post("/webhooks/github") {
            header("X-Hub-Signature-256", signGitHub(malformedGh))
            header("X-GitHub-Event", "push")
            setBody(malformedGh)
        }
        assertEquals(HttpStatusCode.BadRequest, ghMalformedRes.status)

        val invalidRepoGh = """
            {
              "ref": "refs/heads/main",
              "repository": { "full_name": "invalid-single-segment", "default_branch": "main" },
              "sender": { "login": "octocat", "type": "User" }
            }
        """.trimIndent()
        val ghInvalidRepoRes = client.post("/webhooks/github") {
            header("X-Hub-Signature-256", signGitHub(invalidRepoGh))
            header("X-GitHub-Event", "push")
            setBody(invalidRepoGh)
        }
        assertEquals(HttpStatusCode.BadRequest, ghInvalidRepoRes.status)

        val glMalformedRes = client.post("/webhooks/gitlab") {
            header("X-Gitlab-Token", glSecret)
            header("X-Gitlab-Event", "Push Hook")
            setBody("not-json")
        }
        assertEquals(HttpStatusCode.BadRequest, glMalformedRes.status)

        val glInvalidRepo = """
            {
              "object_kind": "merge_request",
              "project": { "path_with_namespace": "invalid-single-segment" },
              "object_attributes": { "action": "merge" }
            }
        """.trimIndent()
        val glInvalidRepoRes = client.post("/webhooks/gitlab") {
            header("X-Gitlab-Token", glSecret)
            header("X-Gitlab-Event", "Merge Request Hook")
            setBody(glInvalidRepo)
        }
        assertEquals(HttpStatusCode.BadRequest, glInvalidRepoRes.status)

        val triggerMalformedRes = client.post("/api/v1/trigger") {
            header("Authorization", "Bearer $triggerToken")
            setBody("not-json")
        }
        assertEquals(HttpStatusCode.BadRequest, triggerMalformedRes.status)

        val triggerBadRepoRes = client.post("/api/v1/trigger") {
            header("Authorization", "Bearer $triggerToken")
            setBody("""{"repositories":[{"platform":"github","repository":"invalid"}]}""")
        }
        assertEquals(HttpStatusCode.BadRequest, triggerBadRepoRes.status)
    }

    @Test
    fun `retries transient publish failures and succeeds when broker recovers`() = testApplication {
        val flakyPublisher = mockk<JobQueuePublisher>()
        var callCount = 0
        coEvery { flakyPublisher.publish(any()) } answers {
            callCount++
            if (callCount <= 2) {
                throw IOException("Transient NATS disconnect")
            }
        }

        application {
            proxyModule(
                config = testConfig(publishRetryBaseDelay = 5.milliseconds, publishRetryBudget = 5.seconds),
                publisher = flakyPublisher,
            )
        }

        val payload = """
            {
              "action": "closed",
              "repository": { "full_name": "hononeko/koshin" }
            }
        """.trimIndent()

        val successRes = client.post("/webhooks/github") {
            header("X-Hub-Signature-256", signGitHub(payload))
            header("X-GitHub-Event", "pull_request")
            setBody(payload)
        }
        assertEquals(HttpStatusCode.Accepted, successRes.status)
        coVerify(exactly = 3) { flakyPublisher.publish(any()) }
    }

    @Test
    fun `returns 503 when publish retries are exhausted`() = testApplication {
        val failingPublisher = mockk<JobQueuePublisher>()
        coEvery { failingPublisher.publish(any()) } throws IOException("NATS cluster down")

        application {
            proxyModule(
                config = testConfig(publishRetryBaseDelay = 5.milliseconds, publishRetryBudget = 25.milliseconds),
                publisher = failingPublisher,
            )
        }

        val payload = """
            {
              "action": "closed",
              "repository": { "full_name": "hononeko/koshin" }
            }
        """.trimIndent()

        val failGhRes = client.post("/webhooks/github") {
            header("X-Hub-Signature-256", signGitHub(payload))
            header("X-GitHub-Event", "pull_request")
            setBody(payload)
        }
        assertEquals(HttpStatusCode.ServiceUnavailable, failGhRes.status)

        val glPayload = """
            {
              "object_kind": "merge_request",
              "project": { "path_with_namespace": "gitlab-org/koshin" },
              "object_attributes": { "action": "merge" }
            }
        """.trimIndent()
        val failGlRes = client.post("/webhooks/gitlab") {
            header("X-Gitlab-Token", glSecret)
            header("X-Gitlab-Event", "Merge Request Hook")
            setBody(glPayload)
        }
        assertEquals(HttpStatusCode.ServiceUnavailable, failGlRes.status)

        val failTriggerRes = client.post("/api/v1/trigger") {
            header("Authorization", "Bearer $triggerToken")
            setBody("""{"repositories":[{"platform":"github","repository":"hononeko/koshin"}]}""")
        }
        assertEquals(HttpStatusCode.ServiceUnavailable, failTriggerRes.status)
    }
}
