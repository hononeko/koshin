package dev.hononeko.koshin.adapters.github

import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RepoRef
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.MockRequestHandleScope
import io.ktor.client.engine.mock.respond
import io.ktor.client.request.HttpRequestData
import io.ktor.client.request.HttpResponseData
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.security.KeyPair
import java.security.KeyPairGenerator
import java.security.interfaces.RSAPrivateCrtKey
import java.security.interfaces.RSAPublicKey
import java.util.Base64
import kotlin.io.path.Path
import kotlin.io.path.writeText
import kotlin.time.Clock
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant
import kotlin.time.toJavaInstant

class GitHubAppTest {

    @TempDir
    lateinit var dir: Path

    private var now = Instant.parse("2026-11-02T12:00:00Z")
    private val clock = object : Clock {
        override fun now(): Instant = now
    }

    /** Requests seen, as `METHOD /path?query` without the API base. */
    private val requests = mutableListOf<String>()
    private var route: MockRequestHandleScope.(String) -> HttpResponseData = { error("No route for $it") }

    private fun app(
        apiUrl: String = GitHubAppConfig.DEFAULT_API_URL,
        pem: String = pkcs8Pem,
        allowed: List<String> = emptyList(),
        action: DisallowedAction = DisallowedAction.IGNORE,
    ): GitHubApp {
        val keyFile = dir.resolve("key.pem").apply { writeText(pem) }
        val base = apiUrl.trimEnd('/')
        val engine = MockEngine { request ->
            val call = "${request.method.value} ${request.url.toString().removePrefix(base)}"
            requests += call
            lastRequest = request
            route(call)
        }
        val http = gitHubHttpClient(engine, HttpRetryConfig(maxRetries = 0), clock, sleep = {})
        return GitHubApp(GitHubAppConfig("1234", keyFile, apiUrl, allowed, action), http, clock)
    }

    private var lastRequest: HttpRequestData? = null

    private fun MockRequestHandleScope.json(body: String, vararg headers: Pair<String, String>): HttpResponseData {
        val all = headers.toList() + ("Content-Type" to "application/json")
        return respond(body, HttpStatusCode.OK, headersOf(*all.map { it.first to listOf(it.second) }.toTypedArray()))
    }

    private fun MockRequestHandleScope.status(code: Int) = respond(
        """{"message":"x"}""",
        HttpStatusCode.fromValue(code),
    )

    private fun tokenJson(token: String, expiresAt: Instant = now + 60.minutes) =
        """{"token":"$token","expires_at":"$expiresAt"}"""

    /** A repo on installation 7 (then [moved] after a 404), its tokens numbered by mint, and the bot `koshin[bot]`. */
    private var installationOfRepo = 7L
    private var mints = 0
    private val gone = mutableSetOf<Long>()

    private val defaultRoute: MockRequestHandleScope.(String) -> HttpResponseData = { call ->
        when {
            call == "GET /repos/hononeko/koshin/installation" -> json("""{"id":$installationOfRepo}""")

            call.startsWith("POST /app/installations/") -> {
                val id = call.removePrefix("POST /app/installations/").substringBefore('/').toLong()
                if (id in gone) status(404) else json(tokenJson("ghs_${id}_${++mints}"))
            }

            call == "GET /app" -> json("""{"id":1234,"slug":"koshin"}""")

            call == "GET /users/koshin%5Bbot%5D" -> json("""{"id":424242,"login":"koshin[bot]"}""")

            else -> error("No route for $call")
        }
    }

    private val repo = RepoRef(Platform.GITHUB, "hononeko/koshin")

    @Test
    fun `the App JWT is RS256, issued by the App id and valid for under 10 minutes`() {
        val jwt = JWT.decode(GitHubAppJwt.fromPem("1234", pkcs1Pem, clock).create())

        Algorithm.RSA256(keyPair.public as RSAPublicKey, null).verify(jwt)
        assertEquals("RS256", jwt.algorithm)
        assertEquals("1234", jwt.issuer)
        assertEquals((now - 60.seconds).toJavaInstant(), jwt.issuedAtAsInstant)
        assertEquals((now + 9.minutes).toJavaInstant(), jwt.expiresAtAsInstant)
        assertTrue(jwt.expiresAtAsInstant.epochSecond - jwt.issuedAtAsInstant.epochSecond <= 600)
    }

    @Test
    fun `PKCS#1 keys, as GitHub hands them out, read like PKCS#8 ones`() {
        val fromPkcs1 = PemKeys.rsaPrivateKey(pkcs1Pem) as RSAPrivateCrtKey
        val fromPkcs8 = PemKeys.rsaPrivateKey(pkcs8Pem) as RSAPrivateCrtKey
        assertEquals(fromPkcs8.modulus, fromPkcs1.modulus)
        assertEquals(fromPkcs8.privateExponent, fromPkcs1.privateExponent)
        assertThrows<IllegalArgumentException> { PemKeys.rsaPrivateKey(pem("CERTIFICATE", byteArrayOf(1))) }
        assertThrows<IllegalArgumentException> { PemKeys.rsaPrivateKey("not a key") }
    }

    @Test
    fun `credentials carry the installation token and the bot identity`() = runTest {
        route = defaultRoute
        val creds = app().credentialsProvider(45.minutes).credentialsFor(repo)

        assertEquals("ghs_7_1", creds.token)
        assertNull(creds.endpoint)
        assertEquals("koshin[bot]", creds.username)
        assertEquals("koshin[bot] <424242+koshin[bot]@users.noreply.github.com>", creds.gitAuthor)
        assertEquals("Bearer ghs_7_1", lastRequest!!.headers[HttpHeaders.Authorization])
        assertEquals(GitHubAppApi.API_VERSION, lastRequest!!.headers["X-GitHub-Api-Version"])
    }

    @Test
    fun `the installation id is cached across calls and looked up again after a 404`() = runTest {
        route = defaultRoute
        val provider = app().credentialsProvider(45.minutes)
        provider.credentialsFor(repo)
        now += 20.minutes // the token now has 40 minutes left: a new one is minted, from the cached installation
        assertEquals("ghs_7_2", provider.credentialsFor(repo).token)
        assertEquals(1, requests.count { it == "GET /repos/hononeko/koshin/installation" })

        // Uninstalled from 7 and installed again as 8
        gone += 7
        installationOfRepo = 8
        now += 20.minutes
        assertEquals("ghs_8_3", provider.credentialsFor(repo).token)
        assertEquals(2, requests.count { it == "GET /repos/hononeko/koshin/installation" })
    }

    @Test
    fun `a token is reused while it has CLI_TIMEOUT_MINUTES + 5 left, by the expires_at GitHub returns`() = runTest {
        route = { call ->
            if (call.startsWith("POST /app/installations/7/")) {
                json(tokenJson("ghs_${++mints}", expiresAt = now + 55.minutes))
            } else {
                defaultRoute(call)
            }
        }
        val provider = app().credentialsProvider(45.minutes)
        assertEquals("ghs_1", provider.credentialsFor(repo).token)

        now += 5.minutes // exactly 50 minutes left
        assertEquals("ghs_1", provider.credentialsFor(repo).token)
        now += 1.seconds
        assertEquals("ghs_2", provider.credentialsFor(repo).token)
        assertEquals(2, requests.count { it.startsWith("POST /app/installations/7/access_tokens") })
    }

    @Test
    fun `a repo the App isn't installed on fails the run, and a CLI timeout no token covers fails at startup`() =
        runTest {
            route = { call -> if (call.endsWith("/installation")) status(404) else defaultRoute(call) }
            val app = app()

            val error = assertThrows<GitHubApiException> { app.credentialsProvider(45.minutes).credentialsFor(repo) }
            assertEquals(404, error.status)
            assertThrows<IllegalArgumentException> { app.credentialsProvider(55.minutes) }
            assertThrows<IllegalArgumentException> {
                app.credentialsProvider(45.minutes).credentialsFor(RepoRef(Platform.GITLAB, "group/project"))
            }
        }

    @Test
    fun `other GitHub errors aren't taken for a stale installation`() = runTest {
        route = defaultRoute
        val provider = app().credentialsProvider(45.minutes)
        provider.credentialsFor(repo)
        now += 20.minutes
        route = { call -> if (call.startsWith("POST")) status(500) else defaultRoute(call) }

        val error = assertThrows<GitHubApiException> { provider.credentialsFor(repo) }
        assertEquals(500, error.status)
        assertEquals(1, requests.count { it == "GET /repos/hononeko/koshin/installation" })
    }

    @Test
    fun `on GHES the endpoint and the noreply domain follow GITHUB_API_URL`() = runTest {
        route = defaultRoute
        val creds = app(apiUrl = "https://ghe.example.com/api/v3/").credentialsProvider(45.minutes).credentialsFor(repo)

        assertEquals("https://ghe.example.com/api/v3", creds.endpoint)
        assertEquals("koshin[bot] <424242+koshin[bot]@users.noreply.ghe.example.com>", creds.gitAuthor)
        assertEquals("https://ghe.example.com/api/v3/users/koshin%5Bbot%5D", lastRequest!!.url.toString())
    }

    private val page2 = "https://api.github.com/installation/repositories?per_page=100&page=2"

    private fun bearer() = lastRequest!!.headers[HttpHeaders.Authorization]!!.removePrefix("Bearer ")

    @Test
    fun `discovery pages through installations and repositories, skipping archived, disabled and suspended`() =
        runTest {
            route = { call ->
                when (call) {
                    "GET /app/installations?per_page=100" -> json(
                        """[{"id":1},{"id":2,"suspended_at":"2026-10-01T00:00:00Z"}]""",
                        "Link" to """<https://api.github.com/app/installations?per_page=100&page=2>; rel="next"""",
                    )

                    "GET /app/installations?per_page=100&page=2" -> json("""[{"id":3}]""")

                    "POST /app/installations/1/access_tokens" -> json(tokenJson("ghs_one"))

                    "POST /app/installations/3/access_tokens" -> json(tokenJson("ghs_three"))

                    "GET /installation/repositories?per_page=100" -> if (bearer() == "ghs_one") {
                        json(
                            """{"repositories":[{"full_name":"org/a"},{"full_name":"org/old","archived":true}]}""",
                            "Link" to """<$page2>; rel="next", <$page2>; rel="last"""",
                        )
                    } else {
                        json("""{"repositories":[{"full_name":"user/c"}]}""")
                    }

                    "GET /installation/repositories?per_page=100&page=2" ->
                        json("""{"repositories":[{"full_name":"org/b"},{"full_name":"org/off","disabled":true}]}""")

                    else -> error("No route for $call")
                }
            }

            val repos = app().discovery().discover()

            assertEquals(listOf("org/a", "org/b", "user/c"), repos.map { it.repository })
            assertTrue(repos.all { it.platform == Platform.GITHUB })
            assertTrue(requests.none { it.startsWith("POST /app/installations/2/") })
        }

    @Test
    fun `discovery skips an installation it can't get a token for`() = runTest {
        route = { call ->
            when (call) {
                "GET /app/installations?per_page=100" -> json("""[{"id":1},{"id":2}]""")
                "POST /app/installations/1/access_tokens" -> status(403)
                "POST /app/installations/2/access_tokens" -> json(tokenJson("ghs_two"))
                "GET /installation/repositories?per_page=100" -> json("""{"repositories":[{"full_name":"org/b"}]}""")
                else -> error("No route for $call")
            }
        }

        assertEquals(listOf("org/b"), app().discovery().discover().map { it.repository })
    }

    @Test
    fun `discovery won't send a token to a pagination link on another host`() = runTest {
        route = { call ->
            when (call) {
                "GET /app/installations?per_page=100" -> json(
                    """[{"id":1}]""",
                    "Link" to """<https://evil.example.com/app/installations?page=2>; rel="next"""",
                )

                else -> error("No route for $call")
            }
        }

        val error = assertThrows<java.io.IOException> { app().discovery().discover() }
        assertTrue(error.message!!.contains("evil.example.com"), error.message)
        assertEquals(1, requests.size)
    }

    @Test
    fun `the bot login is the App slug`() = runTest {
        route = defaultRoute
        assertEquals("koshin[bot]", app().use { it.botLogin() })
    }

    @Test
    fun `config comes from the environment, or is absent without an App`() {
        assertNull(GitHubAppConfig.fromEnv(emptyMap()))
        val config = GitHubAppConfig.fromEnv(
            mapOf("GITHUB_APP_ID" to "42", "GITHUB_APP_PRIVATE_KEY_PATH" to "/etc/key.pem"),
        )!!
        assertEquals(GitHubAppConfig("42", Path("/etc/key.pem")), config)
        assertNull(config.renovateEndpoint)
        assertThrows<IllegalArgumentException> { GitHubAppConfig.fromEnv(mapOf("GITHUB_APP_ID" to "42")) }
        assertThrows<IllegalArgumentException> {
            GitHubAppConfig.fromEnv(
                mapOf("GITHUB_APP_ID" to "42", "GITHUB_APP_PRIVATE_KEY_PATH" to "/k", "GITHUB_API_URL" to "ghe"),
            )
        }
        assertThrows<IllegalArgumentException> { GitHubAppConfig("42", Path("/k"), "https://") }
    }

    private companion object {
        val keyPair: KeyPair = KeyPairGenerator.getInstance("RSA").apply { initialize(2048) }.generateKeyPair()

        val pkcs8Pem = pem("PRIVATE KEY", keyPair.private.encoded)

        /** Java only writes PKCS#8: its PKCS#1 key is the OCTET STRING at the end (offset 26 for a 2048-bit key). */
        val pkcs1Pem: String = keyPair.private.encoded.let { pkcs8 ->
            check(pkcs8[22] == 0x04.toByte()) { "Unexpected PKCS#8 layout" }
            pem("RSA PRIVATE KEY", pkcs8.copyOfRange(26, pkcs8.size))
        }

        fun pem(type: String, der: ByteArray): String {
            val body = Base64.getMimeEncoder(64, "\n".toByteArray()).encodeToString(der)
            return "-----BEGIN $type-----\n$body\n-----END $type-----\n"
        }
    }
}
