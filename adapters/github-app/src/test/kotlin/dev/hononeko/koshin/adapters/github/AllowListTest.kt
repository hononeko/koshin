package dev.hononeko.koshin.adapters.github

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.ports.RepositoryRejectedException
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.MockRequestHandleScope
import io.ktor.client.engine.mock.respond
import io.ktor.client.request.HttpResponseData
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import java.io.IOException
import java.nio.file.Path
import java.security.KeyPairGenerator
import java.util.Base64
import kotlin.io.path.Path
import kotlin.io.path.writeText
import kotlin.time.Clock
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Instant

class AllowListTest {

    @TempDir
    lateinit var dir: Path

    private val now = Instant.parse("2026-11-02T12:00:00Z")
    private val clock = object : Clock {
        override fun now(): Instant = now
    }
    private val requests = mutableListOf<String>()

    private val hononeko = """{"id":1,"account":{"id":100,"login":"hononeko"}}"""
    private val stranger = """{"id":2,"account":{"id":200,"login":"stranger"}}"""

    /** Installation 1 is hononeko's (ID 100), installation 2 belongs to stranger (ID 200). */
    private val route: MockRequestHandleScope.(String) -> HttpResponseData = { call ->
        when (call) {
            "GET /app/installations?per_page=100" -> json("[$hononeko,$stranger]")

            "GET /repos/hononeko/koshin/installation" -> json("""{"id":1,"account":{"id":100,"login":"hononeko"}}""")

            "GET /repos/stranger/repo/installation", "GET /repos/stranger/other/installation" ->
                json("""{"id":2,"account":{"id":200,"login":"stranger"}}""")

            "GET /repos/elsewhere/repo/installation" -> respond("{}", HttpStatusCode.NotFound)

            "GET /repos/broken/repo/installation" -> respond("{}", HttpStatusCode.InternalServerError)

            "POST /app/installations/1/access_tokens" ->
                json("""{"token":"ghs_one","expires_at":"${now + 60.minutes}"}""")

            "GET /installation/repositories?per_page=100" -> json(
                """{"repositories":[{"full_name":"hononeko/koshin"}]}""",
            )

            "PUT /app/installations/2/suspended" -> respond("", HttpStatusCode.NoContent)

            "DELETE /app/installations/2" -> respond("", HttpStatusCode.NoContent)

            "GET /app" -> json("""{"id":1234,"slug":"koshin"}""")

            "GET /users/koshin%5Bbot%5D" -> json("""{"id":424242,"login":"koshin[bot]"}""")

            else -> error("No route for $call")
        }
    }

    private fun MockRequestHandleScope.json(body: String) =
        respond(body, HttpStatusCode.OK, headersOf("Content-Type", "application/json"))

    private fun app(allowed: List<String>, action: DisallowedAction = DisallowedAction.IGNORE): GitHubApp {
        val key = KeyPairGenerator.getInstance("RSA").apply { initialize(2048) }.generateKeyPair().private
        val keyFile = dir.resolve("key.pem").apply {
            writeText(
                "-----BEGIN PRIVATE KEY-----\n${Base64.getMimeEncoder().encodeToString(
                    key.encoded,
                )}\n-----END PRIVATE KEY-----\n",
            )
        }
        val engine = MockEngine { request ->
            val call = "${request.method.value} ${request.url.toString().removePrefix(GitHubAppConfig.DEFAULT_API_URL)}"
            requests += call
            route(call)
        }
        val http = gitHubHttpClient(engine, HttpRetryConfig(maxRetries = 0), clock, sleep = {})
        return GitHubApp(
            GitHubAppConfig("1234", keyFile, allowedAccounts = allowed, disallowedAction = action),
            http,
            clock,
        )
    }

    private fun allowList(vararg entries: String, lookup: suspend (String) -> UserInfo = { error("no lookup") }) =
        AccountAllowList(entries.toList(), lookup)

    @Test
    fun `accounts match by ID or, ignoring case, by login, and an empty list allows all`() = runTest {
        val list = allowList("12345678", "Hononeko")

        assertTrue(list.isAllowed(12345678, "renamed"))
        assertTrue(list.isAllowed(1, "hononeko"))
        assertTrue(list.isAllowed(1, "HONONEKO"))
        assertFalse(list.isAllowed(2, "stranger"))
        assertTrue(allowList().isAllowed(2, "stranger"))
        assertTrue(allowList().allowsAll)
    }

    @Test
    fun `logins are resolved to IDs at startup and matched by ID from then on`() = runTest {
        val list = allowList("hononeko", "ghost") { login ->
            if (login == "hononeko") UserInfo(100, "hononeko") else throw IOException("GitHub down")
        }

        list.resolveLogins()

        assertTrue(list.isAllowed(100, "hononeko-renamed"))
        assertFalse(list.isAllowed(999, "hononeko"), "someone else who took the freed login")
        assertTrue(list.isAllowed(5, "Ghost"), "unresolved logins still match by login")
        allowList().resolveLogins() // nothing to resolve
    }

    @Test
    fun `discovery skips installations by accounts that aren't allowed and suspends them once`() = runTest {
        val repos = app(listOf("100"), DisallowedAction.SUSPEND).discovery().discover()

        assertEquals(listOf(RepoRef(Platform.GITHUB, "hononeko/koshin")), repos)
        assertEquals(1, requests.count { it == "PUT /app/installations/2/suspended" })
        assertTrue(requests.none { it.startsWith("POST /app/installations/2/") })
    }

    @Test
    fun `uninstall deletes the installation, ignore leaves it alone`() = runTest {
        app(listOf("100"), DisallowedAction.UNINSTALL).discovery().discover()
        assertEquals(listOf("DELETE /app/installations/2"), requests.filter { it.contains("/app/installations/2") })

        requests.clear()
        app(listOf("100")).discovery().discover()
        assertTrue(requests.none { it.contains("/app/installations/2") })
    }

    @Test
    fun `no token is minted for an installation by an account that isn't allowed`() = runTest {
        val provider = app(listOf("hononeko")).credentialsProvider(45.minutes)

        assertThrows<RepositoryRejectedException> { provider.credentialsFor(RepoRef(Platform.GITHUB, "stranger/repo")) }
        assertEquals("ghs_one", provider.credentialsFor(RepoRef(Platform.GITHUB, "hononeko/koshin")).token)
        assertTrue(requests.none { it.startsWith("POST /app/installations/2/") })
    }

    @Test
    fun `trigger repositories by accounts that aren't allowed are dropped, with one lookup per owner`() = runTest {
        val repos = listOf(
            RepoRef(Platform.GITHUB, "hononeko/koshin"),
            RepoRef(Platform.GITHUB, "stranger/repo"),
            RepoRef(Platform.GITHUB, "stranger/other"),
            RepoRef(Platform.GITHUB, "elsewhere/repo"),
            RepoRef(Platform.GITHUB, "broken/repo"),
            RepoRef(Platform.GITLAB, "stranger/project"),
        )

        val kept = app(listOf("100")).withoutDisallowed(repos)

        assertEquals(
            listOf("hononeko/koshin", "elsewhere/repo", "broken/repo", "stranger/project"),
            kept.map { it.repository },
        )
        assertEquals(1, requests.count { it.startsWith("GET /repos/stranger/") })

        requests.clear()
        assertEquals(repos, app(emptyList()).withoutDisallowed(repos))
        assertTrue(requests.isEmpty())
    }

    @Test
    fun `an installation already gone counts as uninstalled`() = runTest {
        val engine = MockEngine { respond("", HttpStatusCode.NotFound) }
        val api = GitHubAppApi(
            gitHubHttpClient(engine, HttpRetryConfig(maxRetries = 0), clock, sleep = {}),
            GitHubAppConfig.DEFAULT_API_URL,
            GitHubAppJwt(
                "1234",
                KeyPairGenerator.getInstance("RSA").apply { initialize(2048) }.generateKeyPair().private
                    as java.security.interfaces.RSAPrivateKey,
                clock,
            ),
        )

        api.deleteInstallation(9)
        assertThrows<GitHubApiException> { api.suspendInstallation(9) }
    }

    @Test
    fun `the allow-list and disallowed action come from the environment`() {
        val config = GitHubAppConfig.fromEnv(
            mapOf(
                "GITHUB_APP_ID" to "1",
                "GITHUB_APP_PRIVATE_KEY_PATH" to "/k",
                "GITHUB_APP_ALLOWED_ACCOUNTS" to " hononeko, 12345678 ,,",
                "GITHUB_APP_DISALLOWED_ACTION" to "Suspend",
            ),
        )!!

        assertEquals(listOf("hononeko", "12345678"), config.allowedAccounts)
        assertEquals(DisallowedAction.SUSPEND, config.disallowedAction)
        assertEquals(DisallowedAction.IGNORE, GitHubAppConfig("1", Path("/k")).disallowedAction)
        assertThrows<IllegalArgumentException> { DisallowedAction.fromString("delete") }
    }
}
