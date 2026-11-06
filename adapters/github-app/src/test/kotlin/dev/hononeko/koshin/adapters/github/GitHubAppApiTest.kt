package dev.hononeko.koshin.adapters.github

import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.IOException
import kotlin.time.Instant

class GitHubAppApiTest {

    @Test
    fun `AccountInfo name and toString formatting`() {
        val user = AccountInfo(1L, login = "octocat")
        assertEquals("octocat", user.name)
        assertEquals("octocat (1)", user.toString())

        val enterprise = AccountInfo(2L, slug = "my-ent")
        assertEquals("my-ent", enterprise.name)
        assertEquals("my-ent (2)", enterprise.toString())

        val anonymous = AccountInfo(3L)
        assertEquals("3", anonymous.name)
        assertEquals("3 (3)", anonymous.toString())
    }

    @Test
    fun `InstallationToken redacts token in toString`() {
        val token = InstallationToken("secret-token-value", Instant.fromEpochSeconds(1_700_000_000))
        assertFalse(token.toString().contains("secret-token-value"))
        assertTrue(token.toString().contains("token=***"))
    }

    @Test
    fun `paginate follows valid links and rejects external redirect URLs`() = runTest {
        val jwt = io.mockk.mockk<GitHubAppJwt>()
        io.mockk.every { jwt.create() } returns "mock-jwt"
        val engine = MockEngine { request ->
            when (request.url.encodedPathAndQuery) {
                "/app/installations?per_page=100" -> {
                    val nextLink = """<https://api.github.com/app/installations?page=2>; rel="next""""
                    respond(
                        """[{"id":1}]""",
                        HttpStatusCode.OK,
                        headersOf(
                            HttpHeaders.Link to listOf(nextLink),
                            HttpHeaders.ContentType to listOf("application/json"),
                        ),
                    )
                }

                "/app/installations?page=2" -> {
                    respond(
                        """[{"id":2}]""",
                        HttpStatusCode.OK,
                        headersOf(HttpHeaders.ContentType to listOf("application/json")),
                    )
                }

                else -> respond("", HttpStatusCode.NotFound)
            }
        }
        val client = HttpClient(engine)
        val api = GitHubAppApi(client, "https://api.github.com", jwt)

        val installations = api.installations()
        assertEquals(2, installations.size)
        assertEquals(listOf(1L, 2L), installations.map { it.id })
    }

    @Test
    fun `paginate rejects next link to untrusted external host`() = runTest {
        val jwt = io.mockk.mockk<GitHubAppJwt>()
        io.mockk.every { jwt.create() } returns "mock-jwt"
        val engine = MockEngine {
            respond(
                """[{"id":1}]""",
                HttpStatusCode.OK,
                headersOf(
                    HttpHeaders.Link to listOf("""<https://evil-host.com/steal-token>; rel="next""""),
                    HttpHeaders.ContentType to listOf("application/json"),
                ),
            )
        }
        val client = HttpClient(engine)
        val api = GitHubAppApi(client, "https://api.github.com", jwt)

        assertThrows<IOException> {
            api.installations()
        }
    }

    @Test
    fun `repositoryInstallation validates repository format`() = runTest {
        val jwt = io.mockk.mockk<GitHubAppJwt>()
        io.mockk.every { jwt.create() } returns "mock-jwt"
        val client = HttpClient(MockEngine { respond("[]", HttpStatusCode.OK) })
        val api = GitHubAppApi(client, "https://api.github.com", jwt)

        assertThrows<IllegalArgumentException> {
            api.repositoryInstallation("single-segment")
        }
        assertThrows<IllegalArgumentException> {
            api.repositoryInstallation("owner/repo/extra")
        }
        assertThrows<IllegalArgumentException> {
            api.repositoryInstallation(" /repo")
        }
    }
}
