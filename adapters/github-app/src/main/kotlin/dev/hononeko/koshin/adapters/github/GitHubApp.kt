package dev.hononeko.koshin.adapters.github

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.PlatformCredentials
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.ports.PlatformCredentialsPort
import dev.hononeko.koshin.core.ports.RepositoryDiscoveryPort
import io.ktor.client.HttpClient
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import kotlin.io.path.readText
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes

/** koshin as a GitHub App (KSH-14): installation tokens for Renovate runs, and the repositories to sweep. */
class GitHubApp internal constructor(
    private val config: GitHubAppConfig,
    internal val api: GitHubAppApi,
    private val http: HttpClient,
    clock: Clock,
) : AutoCloseable {

    constructor(config: GitHubAppConfig, http: HttpClient = gitHubHttpClient(), clock: Clock = Clock.System) : this(
        config,
        GitHubAppApi(http, config.apiBase, GitHubAppJwt.fromPem(config.appId, config.privateKeyPath.readText(), clock)),
        http,
        clock,
    )

    /** Installation tokens, shared by everything this process does as the App. */
    val tokens = InstallationTokens(api, clock)

    /** The App's bot login, `<slug>[bot]`: the author of everything Renovate pushes. */
    suspend fun botLogin(): String = "${api.app().slug}[bot]"

    fun credentialsProvider(cliTimeout: Duration) = GitHubAppCredentialsProvider(api, tokens, config, cliTimeout)

    fun discovery() = GitHubAppRepositoryDiscovery(api, tokens)

    override fun close() = http.close()
}

/** Installation tokens cached in memory per installation (never in Valkey), minted again once too short-lived. */
class InstallationTokens internal constructor(private val api: GitHubAppApi, private val clock: Clock) {
    private val tokens = ConcurrentHashMap<Long, InstallationToken>()
    private val mint = Mutex()

    /** A token for [installationId] valid for at least [minRemaining], from the cache if it still is. */
    suspend fun token(installationId: Long, minRemaining: Duration): InstallationToken =
        cached(installationId, minRemaining) ?: mint.withLock {
            cached(installationId, minRemaining)
                ?: api.createInstallationToken(installationId).also { tokens[installationId] = it }
        }

    fun evict(installationId: Long) {
        tokens.remove(installationId)
    }

    private fun cached(installationId: Long, minRemaining: Duration): InstallationToken? =
        tokens[installationId]?.takeIf { it.expiresAt - clock.now() >= minRemaining }
}

/**
 * Credentials for GitHub runs as the App: an installation token for the repo's installation, plus the bot identity
 * (`RENOVATE_USERNAME`, `RENOVATE_GIT_AUTHOR`). Installation tokens last 60 minutes, so one is handed to a run only
 * while it has at least `CLI_TIMEOUT_MINUTES + 5` minutes left, and can't expire mid-run.
 */
class GitHubAppCredentialsProvider internal constructor(
    private val api: GitHubAppApi,
    private val tokens: InstallationTokens,
    private val config: GitHubAppConfig,
    cliTimeout: Duration,
) : PlatformCredentialsPort {

    private val logger = LoggerFactory.getLogger(GitHubAppCredentialsProvider::class.java)
    private val minRemaining = cliTimeout + RUN_MARGIN
    private val installations = ConcurrentHashMap<String, Long>()
    private val identityLock = Mutex()

    @Volatile
    private var identity: BotIdentity? = null

    init {
        require(minRemaining < TOKEN_LIFETIME) {
            "CLI_TIMEOUT_MINUTES + 5 must be below 60 with a GitHub App: installation tokens last 60 minutes " +
                "(CLI timeout: $cliTimeout)"
        }
    }

    override suspend fun credentialsFor(repo: RepoRef): PlatformCredentials {
        require(repo.platform == Platform.GITHUB) { "The GitHub App only covers GitHub repositories: $repo" }
        val token = tokenFor(repo.repository)
        val bot = identity ?: resolveIdentity(token.token)
        return PlatformCredentials(token.token, config.renovateEndpoint, bot.username, bot.gitAuthor)
    }

    /** Forgets [repository]'s installation, e.g. after it was removed from the App. */
    fun evictRepository(repository: String) {
        installations.remove(repository)
    }

    private suspend fun tokenFor(repository: String): InstallationToken {
        installations[repository]?.let { cached ->
            try {
                return tokens.token(cached, minRemaining)
            } catch (e: GitHubApiException) {
                if (e.status !in STALE_INSTALLATION) throw e
                // Uninstalled, or the repo moved: look it up again.
                logger.info(
                    "Installation {} of {} is gone (HTTP {}); looking it up again",
                    cached,
                    repository,
                    e.status,
                )
                installations.remove(repository, cached)
                tokens.evict(cached)
            }
        }
        val installation = api.repositoryInstallation(repository)
            ?: throw GitHubApiException(NOT_FOUND, "The GitHub App isn't installed on $repository")
        installations[repository] = installation
        return tokens.token(installation, minRemaining)
    }

    private suspend fun resolveIdentity(token: String): BotIdentity = identityLock.withLock {
        identity ?: run {
            val login = "${api.app().slug}[bot]"
            val userId = api.user(login, token).id
            BotIdentity(login, "$login <$userId+$login@${config.noreplyDomain}>").also { identity = it }
        }
    }

    private data class BotIdentity(val username: String, val gitAuthor: String)

    companion object {
        val TOKEN_LIFETIME: Duration = 60.minutes
        val RUN_MARGIN: Duration = 5.minutes
        private const val NOT_FOUND = 404
        private val STALE_INSTALLATION = setOf(401, NOT_FOUND)
    }
}

/** Every repository the App is installed on, skipping archived and disabled ones and suspended installations. */
class GitHubAppRepositoryDiscovery internal constructor(
    private val api: GitHubAppApi,
    private val tokens: InstallationTokens,
) : RepositoryDiscoveryPort {

    private val logger = LoggerFactory.getLogger(GitHubAppRepositoryDiscovery::class.java)

    override suspend fun discover(): List<RepoRef> = api.installations()
        .filter { it.suspendedAt == null }
        .flatMap { installation ->
            try {
                val token = tokens.token(installation.id, DISCOVERY_MIN_REMAINING)
                api.installationRepositories(token.token)
                    .filterNot { it.archived || it.disabled }
                    .map { RepoRef(Platform.GITHUB, it.fullName) }
            } catch (e: CancellationException) {
                throw e
            } catch (e: GitHubApiException) {
                // One installation failing (e.g. uninstalled meanwhile) mustn't stop the sweep of the others.
                logger.warn("Skipping installation {} in discovery: {}", installation.id, e.message)
                tokens.evict(installation.id)
                emptyList()
            }
        }

    companion object {
        private val DISCOVERY_MIN_REMAINING: Duration = 5.minutes
    }
}
