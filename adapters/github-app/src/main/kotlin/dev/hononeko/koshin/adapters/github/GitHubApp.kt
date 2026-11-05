package dev.hononeko.koshin.adapters.github

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.PlatformCredentials
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.model.RepoTarget
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
import kotlin.time.Instant

/** koshin as a GitHub App: installation tokens for Renovate runs, and the repositories to sweep. */
class GitHubApp internal constructor(
    private val config: GitHubAppConfig,
    internal val api: GitHubAppApi,
    private val http: HttpClient,
    private val clock: Clock,
) : AutoCloseable {

    constructor(config: GitHubAppConfig, http: HttpClient = gitHubHttpClient(), clock: Clock = Clock.System) : this(
        config,
        GitHubAppApi(http, config.apiBase, GitHubAppJwt.fromPem(config.appId, config.privateKeyPath.readText(), clock)),
        http,
        clock,
    )

    private val logger = LoggerFactory.getLogger(GitHubApp::class.java)

    /** The installation on each account (repository owner) that has one, as looked up for [resolve]. */
    private val ownerInstallations = ConcurrentHashMap<String, CachedInstallation>()

    private class CachedInstallation(val installation: InstallationInfo, val expiresAt: Instant)

    /** Installation tokens, shared by everything this process does as the App. */
    val tokens = InstallationTokens(api, clock)

    /** Which accounts may use the App; call [AccountAllowList.resolveLogins] at startup. */
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
     * [targets] with the App installation covering each GitHub repository, so its job is queued in that
     * installation's scope, and without those whose installation belongs to an account that isn't allowed.
     * One lookup per owner, cached for an hour. A target that names its installation already (from discovery, which
     * checks the account) is kept as it is; one the App isn't installed on, or whose lookup fails, is kept without
     * one: the worker checks again.
     */
    suspend fun resolve(targets: List<RepoTarget>): List<RepoTarget> {
        val installations = targets.filter(::isUnresolved)
            .groupBy { owner(it.repo) }
            .mapValues { (owner, ownerTargets) -> installationOf(owner, ownerTargets.map { it.repo.repository }) }
        return targets.mapNotNull { target ->
            val installation = if (isUnresolved(target)) installations[owner(target.repo)] else null
            when {
                installation == null -> target

                !allowList.allows(installation.account) -> {
                    logger.info("Dropping {}: its account {} isn't allowed", target.repo, installation.account)
                    null
                }

                else -> target.copy(installationId = installation.id)
            }
        }
    }

    /** Forgets [installationId], e.g. once it's uninstalled. */
    fun forgetInstallation(installationId: Long) {
        tokens.evict(installationId)
        ownerInstallations.values.removeIf { it.installation.id == installationId }
    }

    /** The installation on [owner], whose [repositories] these are; `null` if there's none or a lookup failed. */
    private suspend fun installationOf(owner: String, repositories: List<String>): InstallationInfo? =
        ownerInstallations[owner]?.takeIf { it.expiresAt > clock.now() }?.installation
            ?: lookUp(repositories)?.also { installation ->
                ownerInstallations[owner] = CachedInstallation(installation, clock.now() + INSTALLATION_CACHE_TTL)
            }

    /**
     * The installation of the first of [repositories] the App can reach. With "selected repositories" it may not reach
     * them all, but an account has one installation of the App, so it's theirs too. Stops at a failed lookup.
     */
    private suspend fun lookUp(repositories: List<String>): InstallationInfo? {
        var found: InstallationInfo? = null
        var failed = false
        val remaining = repositories.iterator()
        while (found == null && !failed && remaining.hasNext()) {
            val repository = remaining.next()
            try {
                found = api.repositoryInstallation(repository)
            } catch (e: CancellationException) {
                throw e
            } catch (e: IOException) {
                logger.warn("Could not look up the installation of {}; queueing without it: {}", repository, e.message)
                failed = true
            }
        }
        return found
    }

    private fun owner(repo: RepoRef) = repo.repository.substringBefore('/').lowercase()

    private fun isUnresolved(target: RepoTarget) =
        target.repo.platform == Platform.GITHUB && target.installationId == null

    override fun close() = http.close()

    private companion object {
        val INSTALLATION_CACHE_TTL: Duration = 60.minutes
    }
}

/**
 * `GITHUB_APP_ALLOWED_ACCOUNTS`: the accounts whose installations koshin serves, as numeric IDs or logins.
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

    /** Fails closed: with an allow-list, an installation whose account GitHub didn't name isn't allowed. */
    suspend fun allows(account: AccountInfo?): Boolean =
        allowsAll || (account != null && isAllowed(account.id, account.login ?: account.slug.orEmpty()))
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
 * Credentials for GitHub runs as the App: an installation token for the repo's installation, with its ID (whose caches
 * the run gets), plus the bot identity (`RENOVATE_USERNAME`, `RENOVATE_GIT_AUTHOR`). Installation tokens last
 * 60 minutes, so one is handed to a run only while it has at least `CLI_TIMEOUT_MINUTES + 5` minutes left, and can't
 * expire mid-run.
 */
class GitHubAppCredentialsProvider internal constructor(
    private val api: GitHubAppApi,
    private val tokens: InstallationTokens,
    private val config: GitHubAppConfig,
    private val policy: AccountAllowList,
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
        val (installationId, token) = tokenFor(repo.repository)
        val bot = identity ?: resolveIdentity(token.token)
        return PlatformCredentials(
            token = token.token,
            endpoint = config.renovateEndpoint,
            username = bot.username,
            gitAuthor = bot.gitAuthor,
            installationId = installationId,
        )
    }

    /** Forgets [repository]'s installation, e.g. after it was removed from the App. */
    fun evictRepository(repository: String) {
        installations.remove(repository)
    }

    /** The repository's installation, and a token for it. */
    private suspend fun tokenFor(repository: String): Pair<Long, InstallationToken> {
        installations[repository]?.let { cached ->
            try {
                val installationId = allowed(repository, cached)
                return installationId to tokens.token(installationId, minRemaining)
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
        val installationId = allowed(repository, installation)
        return installationId to tokens.token(installationId, minRemaining)
    }

    /** The installation's ID, unless its account isn't allowed: then no token is minted for it, ever. */
    private suspend fun allowed(repository: String, installation: InstallationInfo): Long {
        val account = installation.account
        if (!policy.allows(account)) {
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

/**
 * Every repository the App is installed on, with its installation, skipping archived and disabled ones and
 * suspended installations.
 */
class GitHubAppRepositoryDiscovery internal constructor(
    private val api: GitHubAppApi,
    private val tokens: InstallationTokens,
    private val policy: AccountAllowList,
    private val onDisallowed: suspend (Long, AccountInfo?) -> Unit,
) : RepositoryDiscoveryPort {

    private val logger = LoggerFactory.getLogger(GitHubAppRepositoryDiscovery::class.java)

    override suspend fun discover(): List<RepoTarget> = api.installations()
        .filter { it.suspendedAt == null && isAllowed(it) }
        .flatMap { installation ->
            try {
                val token = tokens.token(installation.id, DISCOVERY_MIN_REMAINING)
                api.installationRepositories(token.token)
                    .filterNot { it.archived || it.disabled }
                    .map { RepoTarget(RepoRef(Platform.GITHUB, it.fullName), installation.id) }
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
        if (policy.allows(account)) return true
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
