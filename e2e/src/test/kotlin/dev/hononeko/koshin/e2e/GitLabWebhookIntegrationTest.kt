package dev.hononeko.koshin.e2e

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.model.TriggerReason
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/** GitLab webhook ([POST /webhooks/gitlab]) pipeline through to worker execution. */
class GitLabWebhookIntegrationTest {
    private val cluster = E2eCluster()
    private val repo = RepoRef(Platform.GITLAB, "gitlab-org/project")

    @AfterEach
    fun tearDown() {
        cluster.close()
    }

    @Test
    fun `GitLab push webhook validates token and is executed end-to-end by multi-platform worker`() {
        val proxy = cluster.startProxy()
        val worker = cluster.startWorker("worker-1", platforms = setOf(Platform.GITHUB, Platform.GITLAB))

        val res = proxy.pushGitLab(repo)
        assertTrue(res.enqueued)
        assertEquals(1, cluster.publishedCount())
        assertTrue(cluster.isQueued(repo))

        cluster.awaitQuiescent(repo)

        assertEquals(1, worker.runs().size)
        val run = worker.runs().single()
        assertEquals(repo, run.repo)
        assertEquals(CliRun.State.COMPLETED, run.state)

        val delivery = worker.deliveries.single()
        assertEquals(Settlement.Ack, delivery.settlement)
        assertEquals(TriggerReason.WEBHOOK, delivery.event.triggerReason)
        assertEquals(Platform.GITLAB, delivery.event.repo.platform)
    }

    @Test
    fun `GitLab push webhook with invalid token returns 403 Forbidden`() {
        val proxy = cluster.startProxy()
        val payload = """
            {
              "object_kind": "push",
              "ref": "refs/heads/main",
              "user_username": "octocat",
              "project": {
                "path_with_namespace": "gitlab-org/project",
                "default_branch": "main"
              }
            }
        """.trimIndent()

        val badTokenRes = proxy.pushGitLabRaw(payload, token = "wrong-token")
        assertEquals(403, badTokenRes.statusCode())

        val noTokenRes = proxy.pushGitLabRaw(payload, token = null)
        assertEquals(403, noTokenRes.statusCode())

        assertEquals(0, cluster.publishedCount())
    }

    @Test
    fun `GitLab branch deletion and bot pushes are ignored without publishing`() {
        val proxy = cluster.startProxy()
        cluster.startWorker("worker-1", platforms = setOf(Platform.GITHUB, Platform.GITLAB))

        val deletePayload = """
            {
              "object_kind": "push",
              "ref": "refs/heads/main",
              "after": "0000000000000000000000000000000000000000",
              "user_username": "octocat",
              "project": {
                "path_with_namespace": "gitlab-org/project",
                "default_branch": "main"
              }
            }
        """.trimIndent()
        val delRes = proxy.pushGitLabRaw(deletePayload)
        assertEquals(202, delRes.statusCode())
        assertFalse(Json.parseToJsonElement(delRes.body()).jsonObject.enqueued)

        assertFalse(proxy.pushGitLab(repo, ref = "refs/heads/feature-branch").enqueued)
        assertFalse(proxy.pushGitLab(repo, ref = "refs/heads/renovate/kotlin-2.x").enqueued)
        assertFalse(proxy.pushGitLab(repo, username = "renovate[bot]").enqueued)

        assertEquals(0, cluster.publishedCount())
    }

    private val JsonObject.enqueued: Boolean
        get() = getValue("enqueued").jsonPrimitive.boolean
}
