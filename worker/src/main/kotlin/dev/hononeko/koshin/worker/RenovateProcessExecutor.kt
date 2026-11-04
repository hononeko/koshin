package dev.hononeko.koshin.worker

import dev.hononeko.koshin.core.model.JobExecutionResult
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.PlatformCredentials
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.ports.CliExecutorPort
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import org.slf4j.LoggerFactory
import java.net.URI
import java.nio.file.Path
import kotlin.time.Duration

class RenovateProcessExecutor(
    private val baseDir: Path,
    private val binarySource: String = WorkerSettings.DEFAULT_RENOVATE_BINARY_SOURCE,
    private val redisUrl: String? = null,
    private val command: List<String> = listOf("renovate"),
    private val inheritedEnv: Map<String, String> = System.getenv(),
    private val launcher: RenovateLauncher = LocalRenovateLauncher(),
    private val requestLimits: RenovateRequestLimits = RenovateRequestLimits(),
    private val rateLimitPattern: Regex = Regex(RateLimitDetector.DEFAULT_ABORT_PATTERN),
    private val repoCache: RepoCacheSettings = RepoCacheSettings(),
    private val cacheDir: Path = baseDir.resolve(WorkerSettings.RENOVATE_CACHE_SUBDIR),
    private val redactor: SecretRedactor = SecretRedactor.shared,
) : CliExecutorPort {

    private val logger = LoggerFactory.getLogger(RenovateProcessExecutor::class.java)
    private val outputLogger = RenovateOutputLogger(redactor = redactor)

    // With a private key, not even the commands an operator allows for `postUpgradeTasks` run next to it
    private val guardrails = UNTRUSTED_CODE_GUARDRAILS + CACHE_ISOLATION_GUARDRAILS +
        if (PRIVATE_KEY_PATH_ENV.any { !inheritedEnv[it].isNullOrBlank() }) KEY_HOLDER_GUARDRAILS else emptyMap()

    /** An operator's `RENOVATE_REDIS_PREFIX` shares the package cache between credential scopes. */
    private val sharedRedisPrefix = inheritedEnv[REDIS_PREFIX_ENV]?.takeIf { it.isNotBlank() }

    init {
        require(command.isNotEmpty()) { "command cannot be empty" }
        guardrails.forEach { (key, pinned) ->
            inheritedEnv[key]?.takeIf { it != pinned }?.let { value ->
                val reason = if (key in KEY_HOLDER_GUARDRAILS) " while Renovate has a private key" else ""
                val ticket = if (key in CACHE_ISOLATION_GUARDRAILS) "KSH-26" else "KSH-25"
                logger.warn("Ignoring {}={}: koshin pins it to {}{} ({})", key, value, pinned, reason, ticket)
            }
        }
        sharedRedisPrefix?.let {
            logger.warn("{}={}: every credential scope shares Renovate's package cache (KSH-26)", REDIS_PREFIX_ENV, it)
        }
    }

    override suspend fun execute(repo: RepoRef, creds: PlatformCredentials, timeout: Duration): JobExecutionResult {
        require(!repo.repository.startsWith("-")) { "Repository cannot start with '-': ${repo.repository}" }

        // The run's token is scrubbed from its output and every other log line while it runs (KSH-26)
        redactor.track(creds.token).use { _ ->
            val rateLimit = RateLimitDetector(repo.platform, rateLimitPattern)
            val env = buildEnvironment(repo, creds)
            val run = launcher.launch(command + repo.repository, env, baseDir) { stream, line ->
                if (stream == OutputStreamName.STDOUT) rateLimit.inspect(line)
                outputLogger.log(line, stream.label)
            }
            logger.info("Started renovate for {}", repo)

            try {
                // Returns once the output has been read too: the rate-limit verdict is in the last lines.
                val exitCode = withTimeoutOrNull(timeout) { run.awaitExit() }
                if (exitCode == null) {
                    logger.warn("Renovate for {} exceeded {}; killing process tree", repo, timeout)
                    return JobExecutionResult.TimedOut
                }
                logger.info("Renovate for {} exited with code {}", repo, exitCode)
                return rateLimit.result()
                    ?.also { logger.warn("Renovate for {} was rate-limited (retry after {})", repo, it.retryAfter) }
                    ?: if (exitCode == 0) JobExecutionResult.Success else JobExecutionResult.Failed(exitCode)
            } finally {
                withContext(NonCancellable) { run.destroy() }
            }
        }
    }

    fun buildEnvironment(repo: RepoRef, creds: PlatformCredentials): Map<String, String> = buildMap {
        // On github.com, lookups there use the run's own token, never an operator's (KSH-26)
        val gitHubCom = isGitHubCom(repo, creds)
        putAll(inheritedEnv.filterKeys { !isStripped(it) && !(gitHubCom && it in GITHUB_COM_TOKEN_ENV) })
        put("LOG_FORMAT", "json")
        put("RENOVATE_PLATFORM", repo.platform.name.lowercase())
        put("RENOVATE_TOKEN", creds.token)
        creds.endpoint?.let { put("RENOVATE_ENDPOINT", it) }
        creds.username?.let { put("RENOVATE_USERNAME", it) }
        creds.gitAuthor?.let { put("RENOVATE_GIT_AUTHOR", it) }
        put("RENOVATE_BASE_DIR", baseDir.toString())
        put("RENOVATE_BINARY_SOURCE", binarySource)
        redisUrl?.let { put("RENOVATE_REDIS_URL", it) }
        put(REDIS_PREFIX_ENV, sharedRedisPrefix ?: "koshin:${creds.scope(repo.platform)}:")
        putAll(repoCache.renovateEnv(cacheDir))
        hostRules(repo, creds, inheritedEnv["RENOVATE_HOST_RULES"])?.let { put("RENOVATE_HOST_RULES", it) }
        // Last: nothing inherited turns them back on
        putAll(guardrails)
    }

    /** Appends a rule with the request limits for the platform's API host to any inherited `RENOVATE_HOST_RULES`. */
    private fun hostRules(repo: RepoRef, creds: PlatformCredentials, inherited: String?): String? {
        if (requestLimits.isEmpty()) return inherited
        val existing = if (inherited.isNullOrBlank()) {
            JsonArray(emptyList())
        } else {
            try {
                Json.parseToJsonElement(inherited) as? JsonArray
            } catch (_: SerializationException) {
                null
            }
        }
        if (existing == null) {
            logger.warn("Inherited RENOVATE_HOST_RULES is not a JSON array; request limits are not applied")
        }
        return existing?.let { JsonArray(it + requestLimitRule(repo, creds)).toString() } ?: inherited
    }

    private fun requestLimitRule(repo: RepoRef, creds: PlatformCredentials) = buildJsonObject {
        put("matchHost", URI(creds.endpoint ?: defaultEndpoint(repo.platform)).host)
        requestLimits.maxRequestsPerSecond?.let { put("maxRequestsPerSecond", it) }
        requestLimits.concurrentRequestLimit?.let { put("concurrentRequestLimit", it) }
    }

    companion object {
        /**
         * Renovate turns either into a `github.com` host rule, which would take lookups there off a github.com run's
         * own installation token. Passed on to GitLab and GHES runs only (KSH-26).
         */
        val GITHUB_COM_TOKEN_ENV = setOf("GITHUB_COM_TOKEN", "RENOVATE_GITHUB_COM_TOKEN")

        /**
         * The package cache (Valkey, or files under `cacheDir`) outlives a run and is shared by every tenant: nothing
         * private goes in, neither packages from private registries nor resolved presets such as an org's
         * `renovate-config` (KSH-26).
         */
        val CACHE_ISOLATION_GUARDRAILS = mapOf(
            "RENOVATE_CACHE_PRIVATE_PACKAGES" to "false",
            "RENOVATE_PRESET_CACHE_PERSISTENCE" to "false",
        )

        /** Renovate's package cache keys start with it: `koshin:<scope>:` unless an operator sets one to share. */
        private const val REDIS_PREFIX_ENV = "RENOVATE_REDIS_PREFIX"

        private const val GITHUB_COM_API_HOST = "api.github.com"

        /**
         * The OpenPGP private keys themselves, which would sit in the environment of Renovate and everything it starts
         * (and take precedence over `RENOVATE_PRIVATE_KEY_PATH`): Renovate reads them from the files instead (KSH-25).
         */
        val RAW_PRIVATE_KEY_ENV = setOf("RENOVATE_PRIVATE_KEY", "RENOVATE_PRIVATE_KEY_OLD")

        /**
         * Renovate holds the private key while it runs, so no repository gets to run code in it: no install scripts or
         * plugins, none of the commands Renovate would run implicitly, no custom Cargo `git` registries (KSH-25).
         */
        val UNTRUSTED_CODE_GUARDRAILS = mapOf(
            "RENOVATE_ALLOW_SCRIPTS" to "false",
            "RENOVATE_ALLOWED_UNSAFE_EXECUTIONS" to "[]",
            "RENOVATE_ALLOW_PLUGINS" to "false",
            "RENOVATE_ALLOW_CUSTOM_CRATE_GIT_REGISTRIES" to "false",
        )

        private val PRIVATE_KEY_PATH_ENV = listOf("RENOVATE_PRIVATE_KEY_PATH", "RENOVATE_PRIVATE_KEY_PATH_OLD")

        /**
         * With a private key, `postUpgradeTasks` are off too: whatever an operator's `allowedCommands` let a repository
         * run would run where it could read the key, and decrypt every tenant's secrets (KSH-25).
         */
        val KEY_HOLDER_GUARDRAILS = mapOf(
            "RENOVATE_ALLOWED_COMMANDS" to "[]",
            "RENOVATE_ALLOW_SHELL_EXECUTOR_FOR_POST_UPGRADE_COMMANDS" to "false",
        )

        /** Worker settings and secrets that must not leak into Renovate (or repo code it runs). */
        private val STRIPPED_ENV = setOf(
            "GITHUB_TOKEN",
            "GITLAB_TOKEN",
            "RENOVATE_ENDPOINT",
            "INTERNAL_TRIGGER_TOKEN",
            "RUNNER_SOCKET",
            // koshin settings that only look like Renovate options; they reach Renovate as RENOVATE_HOST_RULES.
            "RENOVATE_MAX_REQUESTS_PER_SECOND",
            "RENOVATE_CONCURRENT_REQUEST_LIMIT",
            // koshin's own log levels; Renovate's is RENOVATE_LOG_LEVEL, which it reads before LOG_LEVEL
            "LOG_LEVEL",
            "ROOT_LOG_LEVEL",
            // Legacy aliases of RENOVATE_S3_*, which koshin sets itself; Renovate lets an alias override them.
            "RENOVATE_X_S3_ENDPOINT",
            "RENOVATE_X_S3_PATH_STYLE",
        ) + RAW_PRIVATE_KEY_ENV
        private val STRIPPED_PREFIXES = listOf("NATS_", "VALKEY_", "GITHUB_APP_")
        private const val STRIPPED_SUFFIX = "_WEBHOOK_SECRET"

        /** GitHub itself, not GHES; an endpoint without a host it can parse counts as github.com, so nothing leaks. */
        private fun isGitHubCom(repo: RepoRef, creds: PlatformCredentials): Boolean {
            val host = creds.endpoint?.let { endpoint -> runCatching { URI(endpoint).host }.getOrNull() }
            return repo.platform == Platform.GITHUB && (host == null || host.equals(GITHUB_COM_API_HOST, true))
        }

        private fun defaultEndpoint(platform: Platform): String = when (platform) {
            Platform.GITHUB -> "https://api.github.com/"
            Platform.GITLAB -> "https://gitlab.com/api/v4/"
        }

        fun isStripped(key: String): Boolean = key in STRIPPED_ENV ||
            STRIPPED_PREFIXES.any { key.startsWith(it) } ||
            key.endsWith(STRIPPED_SUFFIX)
    }
}

/** Renovate `hostRules` limits applied to the platform's API host on every run; unset values are left to Renovate. */
data class RenovateRequestLimits(val maxRequestsPerSecond: Double? = null, val concurrentRequestLimit: Int? = null) {
    init {
        require(maxRequestsPerSecond == null || maxRequestsPerSecond > 0) {
            "RENOVATE_MAX_REQUESTS_PER_SECOND must be > 0: $maxRequestsPerSecond"
        }
        require(concurrentRequestLimit == null || concurrentRequestLimit > 0) {
            "RENOVATE_CONCURRENT_REQUEST_LIMIT must be > 0: $concurrentRequestLimit"
        }
    }

    fun isEmpty(): Boolean = maxRequestsPerSecond == null && concurrentRequestLimit == null
}
