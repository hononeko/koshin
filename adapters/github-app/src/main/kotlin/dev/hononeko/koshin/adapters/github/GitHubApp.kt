package dev.hononeko.koshin.adapters.github

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.PlatformCredentials
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.ports.InstallationPolicyPort
import dev.hononeko.koshin.core.ports.PlatformCredentialsPort
import dev.hononeko.koshin.core.ports.RepositoryDiscoveryPort
import dev.hononeko.koshin.core.ports.RepositoryRejectedException
import io.ktor.client.HttpClient
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import java.io.IOException
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

    private val logger = LoggerFactory.getLogger(GitHubApp::class.java)

    /** Installation tokens, shared by everything this process does as the App. */
    val tokens = InstallationTokens(api, clock)

    /** Which accounts may use the App (KSH-17); call [AccountAllowList.resolveLogins] at startup. */
    val allowList = AccountAllowList(config.allowedAccounts) { login -> api.user(login) }

    /** The App's bot login, `<slug>[bot]`: the author of everything Renovate pushes. */
    suspend fun botLogin(): String = "${api.app().slug}[bot]"

    fun credentialsProvider(cliTimeout: Duration) =
        GitHubAppCredentialsProvider(api, tokens, config, allowList, cliTimeout)

    fun discovery() = GitHubAppRepositoryDiscovery(api, tokens, allowList, ::applyDisallowedAction)

    /** Applies `GITHUB_APP_DISALLOWED_ACTION` to an installation by an account that isn't allowed. */
    suspend fun applyDisallowedAction(installationId: Long, account: AccountInfo?) {
        when (config.disallowedAction) {
            DisallowedAction.IGNORE -> return
            DisallowedAction.SUSPEND -> api.suspendInstallation(installationId)
            DisallowedAction.UNINSTALL -> api.deleteInstallation(installationId)
        }
        tokens.evict(installationId)
        logger.info(
            "Applied {} to installation {} of {}, which isn't allowed",
            config.disallowedAction.name.lowercase(),
            installationId,
            account,
        )
    }

    /**
     * [repos] without the GitHub ones whose owner's installation belongs to an account that isn't allowed: one lookup
     * per owner, and only with an allow-list. A repo the App isn't installed on, or whose lookup fails, is kept: the
     * worker checks again.
     */
    suspend fun withoutDisallowed(repos: List<RepoRef>): List<RepoRef> {
        if (allowList.allowsAll) return repos
        val allowedOwners = repos.filter { it.platform == Platform.GITHUB }
            .groupBy { it.repository.substringBefore('/') }
            .mapValues { (_, ownerRepos) -> ownerAllowed(ownerRepos.first().repository) }
        return repos.filter { repo ->
            val owner = repo.repository.substringBefore('/')
            repo.platform != Platform.GITHUB || allowedOwners[owner] != false
        }
    }

    private suspend fun ownerAllowed(repository: String): Boolean = try {
        val account = api.repositoryInstallation(repository)?.account
        account == null || allowList.isAllowed(account.id, account.login.orEmpty()).also { allowed ->
            if (!allowed) logger.info("Dropping repositories of {}: the account isn't allowed", account)
        }
    } catch (e: CancellationException) {
        throw e
    } catch (e: IOException) {
        logger.warn("Could not look up the installation of {}; keeping it: {}", repository, e.message)
        true
    }

    override fun close() = http.close()
}

/**
 * `GITHUB_APP_ALLOWED_ACCOUNTS` (KSH-17): the accounts whose installations koshin serves, as numeric IDs or logins.
 * Logins are resolved to IDs at startup ([resolveLogins]) and matched by ID from then on, since a login can be renamed
 * and a freed one registered by someone else. A login that couldn't be resolved is matched by login, ignoring case.
 * An empty list allows every account.
 */
class AccountAllowList(entries: List<String>, private val lookup: suspend (String) -> UserInfo) :
    InstallationPolicyPort {

    private val logger = LoggerFactory.getLogger(AccountAllowList::class.java)
    private val ids: MutableSet<Long> = ConcurrentHashMap.newKeySet()
    private val logins: MutableSet<String> = ConcurrentHashMap.newKeySet()

    val allowsAll: Boolean = entries.isEmpty()

    init {
        entries.map { it.trim() }.filter { it.isNotEmpty() }.forEach { entry ->
            entry.toLongOrNull()?.let { ids += it } ?: logins.add(entry.lowercase())
        }
    }

    /** Resolves configured logins to IDs (`GET /users/{login}`), logging the mapping; failures keep the login. */
    suspend fun resolveLogins() {
        if (allowsAll) {
            logger.warn("GITHUB_APP_ALLOWED_ACCOUNTS is unset: any account that installs the GitHub App can use it")
            return
        }
        logins.toList().forEach { login ->
            try {
                val user = lookup(login)
                ids += user.id
                logins.remove(login)
                logger.info("Allowed GitHub account {} has ID {}; matching by ID", login, user.id)
            } catch (e: CancellationException) {
                throw e
            } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
                logger.warn("Could not resolve allowed GitHub account {}; matching it by login: {}", login, e.message)
            }
        }
    }

    override suspend fun isAllowed(accountId: Long, login: String): Boolean =
        allowsAll || accountId in ids || login.lowercase() in logins
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
    private val policy: InstallationPolicyPort,
    cliTimeout: Duration,
) : PlatformCredentialsPort {

    private val logger = LoggerFactory.getLogger(GitHubAppCredentialsProvider::class.java)
    private val minRemaining = cliTimeout + RUN_MARGIN
    private val installations = ConcurrentHashMap<String, InstallationInfo>()
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
                return tokens.token(allowed(repository, cached), minRemaining)
            } catch (e: GitHubApiException) {
                if (e.status !in STALE_INSTALLATION) throw e
                // Uninstalled, or the repo moved: look it up again.
                logger.info(
                    "Installation {} of {} is gone (HTTP {}); looking it up again",
                    cached.id,
                    repository,
                    e.status,
                )
                installations.remove(repository, cached)
                tokens.evict(cached.id)
            }
        }
        val installation = api.repositoryInstallation(repository)
            ?: throw GitHubApiException(NOT_FOUND, "The GitHub App isn't installed on $repository")
        installations[repository] = installation
        return tokens.token(allowed(repository, installation), minRemaining)
    }

    /** The installation's ID, unless its account isn't allowed: then no token is minted for it, ever. */
    private suspend fun allowed(repository: String, installation: InstallationInfo): Long {
        val account = installation.account
        if (account != null && !policy.isAllowed(account.id, account.login.orEmpty())) {
            logger.warn("Not running {}: its installation belongs to {}, which isn't allowed", repository, account)
            throw RepositoryRejectedException("$repository is installed by $account, which isn't allowed")
        }
        return installation.id
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
    private val policy: InstallationPolicyPort,
    private val onDisallowed: suspend (Long, AccountInfo?) -> Unit,
) : RepositoryDiscoveryPort {

    private val logger = LoggerFactory.getLogger(GitHubAppRepositoryDiscovery::class.java)

    override suspend fun discover(): List<RepoRef> = api.installations()
        .filter { it.suspendedAt == null && isAllowed(it) }
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

    /** An installation by an account that isn't allowed is skipped, and gets `GITHUB_APP_DISALLOWED_ACTION`. */
    private suspend fun isAllowed(installation: InstallationInfo): Boolean {
        val account = installation.account
        if (account == null || policy.isAllowed(account.id, account.login.orEmpty())) return true
        logger.info("Skipping installation {} of {} in discovery: the account isn't allowed", installation.id, account)
        try {
            onDisallowed(installation.id, account)
        } catch (e: CancellationException) {
            throw e
        } catch (e: IOException) {
            logger.warn("Could not apply the disallowed action to installation {}: {}", installation.id, e.message)
        }
        return false
    }

    companion object {
        private val DISCOVERY_MIN_REMAINING: Duration = 5.minutes
    }
}
