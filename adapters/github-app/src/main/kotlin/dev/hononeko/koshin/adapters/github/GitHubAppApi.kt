package dev.hononeko.koshin.adapters.github

import io.ktor.client.HttpClient
import io.ktor.client.request.HttpRequestBuilder
import io.ktor.client.request.delete
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.put
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.encodeURLPathPart
import io.ktor.http.isSuccess
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.io.IOException
import kotlin.time.Instant

/** A GitHub API call that failed with [status]. */
class GitHubApiException(val status: Int, message: String) : IOException(message)

data class InstallationToken(val token: String, val expiresAt: Instant) {
    override fun toString(): String = "InstallationToken(token=***, expiresAt=$expiresAt)"
}

@Serializable
internal data class AppInfo(val id: Long, val slug: String)

/** A user, an organization, or (`slug`, no `login`) an enterprise. */
@Serializable
data class AccountInfo(val id: Long, val login: String? = null, val slug: String? = null) {
    val name: String get() = login ?: slug ?: id.toString()

    override fun toString(): String = "$name ($id)"
}

@Serializable
internal data class InstallationInfo(
    val id: Long,
    val account: AccountInfo? = null,
    @SerialName("suspended_at") val suspendedAt: String? = null,
)

@Serializable
internal data class RepositoryInfo(
    @SerialName("full_name") val fullName: String,
    val archived: Boolean = false,
    val disabled: Boolean = false,
)

@Serializable
data class UserInfo(val id: Long, val login: String)

@Serializable
private data class AccessTokenResponse(val token: String, @SerialName("expires_at") val expiresAt: String)

@Serializable
private data class InstallationRepositories(val repositories: List<RepositoryInfo> = emptyList())

/** The GitHub REST calls koshin makes as an App, against [apiBase] (github.com or GHES). */
internal class GitHubAppApi(private val http: HttpClient, private val apiBase: String, private val jwt: GitHubAppJwt) {

    suspend fun app(): AppInfo = decode(get("/app", appAuth()))

    /** The installation that covers [repository] (`owner/name`), or `null` when the App isn't installed there. */
    suspend fun repositoryInstallation(repository: String): InstallationInfo? {
        val response = get("/repos/${repositoryPath(repository)}/installation", appAuth(), allowNotFound = true)
        return if (response.status == HttpStatusCode.NotFound) null else decode<InstallationInfo>(response)
    }

    suspend fun suspendInstallation(installationId: Long) {
        val path = "/app/installations/$installationId/suspended"
        http.put(apiBase + path) { headers(appAuth()) }.orThrow("PUT $path")
    }

    /** Uninstalls; an installation that's already gone counts as done. */
    suspend fun deleteInstallation(installationId: Long) {
        val path = "/app/installations/$installationId"
        val response = http.delete(apiBase + path) { headers(appAuth()) }
        if (response.status != HttpStatusCode.NotFound) response.orThrow("DELETE $path")
    }

    suspend fun createInstallationToken(installationId: Long): InstallationToken {
        val path = "/app/installations/$installationId/access_tokens"
        val response = http.post(apiBase + path) { headers(appAuth()) }.orThrow("POST $path")
        val body = decode<AccessTokenResponse>(response)
        return InstallationToken(body.token, Instant.parse(body.expiresAt))
    }

    suspend fun installations(): List<InstallationInfo> =
        paginate("/app/installations?per_page=$PAGE_SIZE", appAuth()) { json.decodeFromString(it) }

    suspend fun installationRepositories(token: String): List<RepositoryInfo> =
        paginate("/installation/repositories?per_page=$PAGE_SIZE", token) {
            json.decodeFromString<InstallationRepositories>(it).repositories
        }

    /** Looks up a user or an organization, without authentication when [token] is `null`. */
    suspend fun user(login: String, token: String? = null): UserInfo =
        decode(get("/users/${login.encodeURLPathPart()}", token))

    private fun appAuth(): String = jwt.create()

    private suspend fun get(path: String, bearer: String?, allowNotFound: Boolean = false): HttpResponse {
        val response = http.get(apiBase + path) { headers(bearer) }
        val notFound = allowNotFound && response.status == HttpStatusCode.NotFound
        return if (notFound) response else response.orThrow("GET $path")
    }

    /** Follows `Link: <…>; rel="next"` until the last page. */
    private suspend fun <T> paginate(path: String, bearer: String, page: (String) -> List<T>): List<T> {
        val items = mutableListOf<T>()
        var url: String? = apiBase + path
        while (url != null) {
            val response = http.get(url) { headers(bearer) }.orThrow("GET $path")
            items += page(response.bodyAsText())
            url = response.headers[HttpHeaders.Link]?.let { NEXT_LINK.find(it)?.groupValues?.get(1) }
            // The token goes along: never to another host, e.g. one a proxy in front of GHES rewrote the links to.
            if (url != null && !url.startsWith("$apiBase/")) {
                throw IOException("Refusing to follow a pagination link outside $apiBase: $url")
            }
        }
        return items
    }

    private fun HttpRequestBuilder.headers(bearer: String?) {
        header(HttpHeaders.Accept, "application/vnd.github+json")
        header("X-GitHub-Api-Version", API_VERSION)
        bearer?.let { header(HttpHeaders.Authorization, "Bearer $it") }
    }

    private suspend fun HttpResponse.orThrow(call: String): HttpResponse {
        if (!status.isSuccess()) {
            val detail = bodyAsText().take(MAX_ERROR)
            throw GitHubApiException(status.value, "$call failed with HTTP ${status.value}: $detail")
        }
        return this
    }

    private suspend inline fun <reified T> decode(response: HttpResponse): T = json.decodeFromString(
        response.bodyAsText(),
    )

    private fun repositoryPath(repository: String): String {
        val parts = repository.split('/')
        require(parts.size == 2 && parts.all { it.isNotBlank() }) { "Expected owner/name: $repository" }
        return parts.joinToString("/") { it.encodeURLPathPart() }
    }

    companion object {
        const val API_VERSION = "2022-11-28"
        private const val PAGE_SIZE = 100
        private const val MAX_ERROR = 200
        private val NEXT_LINK = Regex("""<([^>]+)>;\s*rel="next"""")
        private val json = Json { ignoreUnknownKeys = true }
    }
}
