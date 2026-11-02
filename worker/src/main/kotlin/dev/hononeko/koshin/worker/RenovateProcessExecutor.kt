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
) : CliExecutorPort {

    private val logger = LoggerFactory.getLogger(RenovateProcessExecutor::class.java)
    private val outputLogger = RenovateOutputLogger()

    init {
        require(command.isNotEmpty()) { "command cannot be empty" }
    }

    override suspend fun execute(repo: RepoRef, creds: PlatformCredentials, timeout: Duration): JobExecutionResult {
        require(!repo.repository.startsWith("-")) { "Repository cannot start with '-': ${repo.repository}" }

        val rateLimit = RateLimitDetector(repo.platform, rateLimitPattern)
        val run = launcher.launch(command + repo.repository, buildEnvironment(repo, creds), baseDir) { stream, line ->
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

    fun buildEnvironment(repo: RepoRef, creds: PlatformCredentials): Map<String, String> = buildMap {
        putAll(inheritedEnv.filterKeys { !isStripped(it) })
        put("LOG_FORMAT", "json")
        put("RENOVATE_PLATFORM", repo.platform.name.lowercase())
        put("RENOVATE_TOKEN", creds.token)
        creds.endpoint?.let { put("RENOVATE_ENDPOINT", it) }
        creds.username?.let { put("RENOVATE_USERNAME", it) }
        creds.gitAuthor?.let { put("RENOVATE_GIT_AUTHOR", it) }
        put("RENOVATE_BASE_DIR", baseDir.toString())
        put("RENOVATE_BINARY_SOURCE", binarySource)
        redisUrl?.let { put("RENOVATE_REDIS_URL", it) }
        putAll(repoCache.renovateEnv(cacheDir))
        hostRules(repo, creds, inheritedEnv["RENOVATE_HOST_RULES"])?.let { put("RENOVATE_HOST_RULES", it) }
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
        )
        private val STRIPPED_PREFIXES = listOf("NATS_", "VALKEY_", "GITHUB_APP_")
        private const val STRIPPED_SUFFIX = "_WEBHOOK_SECRET"

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
