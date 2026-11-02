package dev.hononeko.koshin.proxy

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.model.TriggerReason
import dev.hononeko.koshin.core.ports.InstallationPolicyPort
import dev.hononeko.koshin.core.ports.JobQueuePublisher
import dev.hononeko.koshin.core.ports.RepositoryDiscoveryPort
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
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.HexFormat
import java.util.concurrent.CopyOnWriteArrayList
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import kotlin.time.Duration.Companion.seconds

class GitHubAppWebhookTest {

    private val secret = "test-gh-webhook-secret"
    private val config = ProxyConfig(port = 0, githubWebhookSecret = secret, publishRetryBudget = 0.seconds)

    private class RecordingPublisher(private val failing: Boolean = false) : JobQueuePublisher {
        val published = CopyOnWriteArrayList<RenovateJobEvent>()

        override suspend fun publish(event: RenovateJobEvent) {
            if (failing) throw IOException("NATS down")
            published.add(event)
        }

        override suspend fun publishToDeadLetter(event: RenovateJobEvent) = Unit
    }

    private fun sign(payload: String): String {
        val mac = Mac.getInstance("HmacSHA256").apply { init(SecretKeySpec(secret.toByteArray(), "HmacSHA256")) }
        return "sha256=" + HexFormat.of().formatHex(mac.doFinal(payload.toByteArray()))
    }

    private suspend fun HttpClient.webhook(event: String, payload: String): HttpResponse = post("/webhooks/github") {
        header("X-Hub-Signature-256", sign(payload))
        header("X-GitHub-Event", event)
        header("X-GitHub-Delivery", "delivery-1")
        contentType(ContentType.Application.Json)
        setBody(payload)
    }

    @Test
    fun `installing the App runs every repository it now covers once`() = testApplication {
        val publisher = RecordingPublisher()
        application { proxyModule(config = config, publisher = publisher) }

        val created = """
            {"action":"created","installation":{"id":42},
             "repositories":[{"full_name":"org/a"},{"full_name":"org/b"}]}
        """.trimIndent()
        assertEquals(HttpStatusCode.Accepted, client.webhook("installation", created).status)
        val added = """{"action":"added","installation":{"id":42},"repositories_added":[{"full_name":"org/c"}]}"""
        assertEquals(HttpStatusCode.Accepted, client.webhook("installation_repositories", added).status)

        assertEquals(listOf("org/a", "org/b", "org/c"), publisher.published.map { it.repo.repository })
        assertTrue(publisher.published.all { it.triggerReason == TriggerReason.SCHEDULE })
        assertTrue(publisher.published.all { it.deliveryId == "delivery-1" && it.repo.platform == Platform.GITHUB })
    }

    @Test
    fun `removed repositories and other installation actions start nothing`() = testApplication {
        val publisher = RecordingPublisher()
        application { proxyModule(config = config, publisher = publisher) }

        val removed = """{"action":"removed","installation":{"id":42},"repositories_removed":[{"full_name":"org/a"}]}"""
        val responses = listOf(
            client.webhook("installation_repositories", removed),
            client.webhook("installation", """{"action":"new_permissions_accepted","installation":{"id":42}}"""),
            client.webhook("installation", """{"action":"created","installation":{"id":42},"repositories":[]}"""),
        )

        assertTrue(responses.all { it.status == HttpStatusCode.Accepted })
        assertTrue(responses.all { it.bodyAsText().contains("\"enqueued\":false") })
        assertTrue(publisher.published.isEmpty())
        assertEquals(HttpStatusCode.BadRequest, client.webhook("installation", "not json").status)
    }

    @Test
    fun `a deleted or suspended installation is reported, so its tokens are dropped`() = testApplication {
        val gone = CopyOnWriteArrayList<Long>()
        application {
            proxyModule(
                config = config,
                publisher = RecordingPublisher(),
                gitHubApp = ProxyGitHubApp(onInstallationGone = { gone += it }),
            )
        }

        client.webhook("installation", """{"action":"deleted","installation":{"id":42}}""")
        val res = client.webhook("installation", """{"action":"suspend","installation":{"id":7}}""")

        assertEquals(listOf(42L, 7L), gone)
        assertTrue(res.bodyAsText().contains("installation suspend"))
    }

    @Test
    fun `a failing queue answers 503 to an installation webhook`() = testApplication {
        application { proxyModule(config = config, publisher = RecordingPublisher(failing = true)) }
        val created = """{"action":"created","installation":{"id":42},"repositories":[{"full_name":"org/a"}]}"""

        assertEquals(HttpStatusCode.ServiceUnavailable, client.webhook("installation", created).status)
        val invalid = """{"action":"created","installation":{"id":42},"repositories":[{"full_name":"no-slash"}]}"""
        assertEquals(HttpStatusCode.BadRequest, client.webhook("installation", invalid).status)
    }

    @Test
    fun `pushes by the App's bot are ignored once its login is known`() = testApplication {
        val publisher = RecordingPublisher()
        var botLogin: String? = null
        application {
            proxyModule(
                config = config,
                publisher = publisher,
                gitHubApp = ProxyGitHubApp(botLogins = { listOfNotNull(botLogin) }),
            )
        }
        val push = """
            {"ref":"refs/heads/main","repository":{"full_name":"org/a","default_branch":"main"},
             "sender":{"login":"koshin[bot]","type":"Bot"}}
        """.trimIndent()

        client.webhook("push", push)
        assertEquals(1, publisher.published.size)
        botLogin = "koshin[bot]"
        assertTrue(client.webhook("push", push).bodyAsText().contains("self-triggered push"))
        assertEquals(1, publisher.published.size)
    }

    /** Only hononeko (ID 100) may use the App. */
    private val allowOnly100 = object : InstallationPolicyPort {
        override suspend fun isAllowed(accountId: Long, login: String) = accountId == 100L
    }

    private fun push(ownerId: Long, owner: String) = """
        {"ref":"refs/heads/main","repository":{"full_name":"$owner/repo","default_branch":"main",
         "owner":{"id":$ownerId,"login":"$owner"}},"sender":{"login":"someone","type":"User"}}
    """.trimIndent()

    @Test
    fun `webhooks from accounts that may not use the App enqueue nothing`() = testApplication {
        val publisher = RecordingPublisher()
        application {
            proxyModule(config = config, publisher = publisher, gitHubApp = ProxyGitHubApp(policy = allowOnly100))
        }

        val ignored = client.webhook("push", push(200, "stranger"))
        assertEquals(HttpStatusCode.Accepted, ignored.status)
        assertTrue(ignored.bodyAsText().contains("stranger (200) is not allowed"))
        client.webhook("push", push(100, "hononeko"))

        assertEquals(listOf("hononeko/repo"), publisher.published.map { it.repo.repository })
    }

    @Test
    fun `a new installation by an account that may not use the App gets the disallowed action, nothing else`() =
        testApplication {
            val publisher = RecordingPublisher()
            val actions = CopyOnWriteArrayList<String>()
            application {
                proxyModule(
                    config = config,
                    publisher = publisher,
                    gitHubApp = ProxyGitHubApp(
                        policy = allowOnly100,
                        onDisallowedInstallation = { id, account ->
                            actions += "$id:$account"
                            if (id == 9L) throw IOException("GitHub down")
                        },
                    ),
                )
            }
            fun installed(id: Long, action: String, field: String) = """
                {"action":"$action","installation":{"id":$id,"account":{"id":200,"login":"stranger"}},
                 "$field":[{"full_name":"stranger/repo"}]}
            """.trimIndent()

            client.webhook("installation", installed(7, "created", "repositories"))
            client.webhook("installation_repositories", installed(7, "added", "repositories_added"))
            val failing = client.webhook("installation", installed(9, "created", "repositories"))

            assertEquals(HttpStatusCode.Accepted, failing.status)
            assertEquals(listOf("7:stranger (200)", "9:stranger (200)"), actions)
            assertTrue(publisher.published.isEmpty())
        }

    @Test
    fun `triggers drop the repositories the App's policy rejects`() = testApplication {
        val publisher = RecordingPublisher()
        application {
            proxyModule(
                config = config.copy(internalTriggerToken = "trigger"),
                publisher = publisher,
                gitHubApp = ProxyGitHubApp(allowedRepositories = { repos -> repos.filter { it.repository != "x/b" } }),
            )
        }

        val res = client.post("/api/v1/trigger") {
            header("Authorization", "Bearer trigger")
            contentType(ContentType.Application.Json)
            setBody("""{"repositories":[{"repository":"x/a"},{"repository":"x/b"}]}""")
        }

        assertTrue(res.bodyAsText().contains("\"enqueued\":1"), res.bodyAsText())
        assertEquals(listOf("x/a"), publisher.published.map { it.repo.repository })
    }

    @Test
    fun `the bot login is looked up again until GitHub answers`() = runTest {
        var calls = 0
        val login = AppBotLogin(fetch = { if (++calls < 3) throw IOException("GitHub down") else "koshin[bot]" })

        assertNull(login.get())
        login.start(this).join()

        assertEquals("koshin[bot]", login.get())
        assertEquals(3, calls)
    }

    private fun discovery(vararg repos: RepoRef) = object : RepositoryDiscoveryPort {
        override suspend fun discover(): List<RepoRef> = repos.toList()
    }

    @Test
    fun `discovery combines its sources`() = runTest {
        val app = discovery(RepoRef(Platform.GITHUB, "org/a"), RepoRef(Platform.GITHUB, "org/b"))
        val file = discovery(RepoRef(Platform.GITHUB, "org/a"), RepoRef(Platform.GITLAB, "g/p"))

        assertEquals(
            listOf("org/a", "org/b", "g/p"),
            CompositeRepositoryDiscovery.of(listOf(app, file))!!.discover().map { it.repository },
        )
        assertSame(app, CompositeRepositoryDiscovery.of(listOf(app)))
        assertNull(CompositeRepositoryDiscovery.of(emptyList()))
    }
}
