package dev.hononeko.koshin.worker

import dev.hononeko.koshin.core.model.JobExecutionResult
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.PlatformCredentials
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.ports.CliExecutorPort
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import kotlinx.coroutines.slf4j.MDCContext
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import org.slf4j.LoggerFactory
import java.io.IOException
import java.io.InputStream
import java.net.URI
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import kotlin.io.path.createDirectories
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class RenovateProcessExecutor(
    private val baseDir: Path,
    private val binarySource: String = WorkerSettings.DEFAULT_RENOVATE_BINARY_SOURCE,
    private val redisUrl: String? = null,
    private val command: List<String> = listOf("renovate"),
    private val inheritedEnv: Map<String, String> = System.getenv(),
    private val ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
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

        val process = withContext(ioDispatcher) {
            baseDir.createDirectories()
            ProcessBuilder(command + repo.repository)
                .directory(baseDir.toFile())
                .apply {
                    environment().clear()
                    environment().putAll(buildEnvironment(repo, creds))
                }
                .start()
                .also { it.outputStream.close() }
        }
        logger.info("Started renovate for {} (pid {})", repo, process.pid())

        val rateLimit = RateLimitDetector(repo.platform, rateLimitPattern)
        // Not children of the caller: a blocked read must never hold up cancellation. They keep the job's MDC.
        val pumps = CoroutineScope(ioDispatcher + MDCContext()).launch {
            launch { pump(process.inputStream, "stdout", rateLimit::inspect) }
            launch { pump(process.errorStream, "stderr") }
        }

        try {
            val exited = withTimeoutOrNull(timeout) { process.onExit().await() }
            if (exited == null) {
                logger.warn("Renovate for {} exceeded {}; killing process tree", repo, timeout)
                return JobExecutionResult.TimedOut
            }
            val exitCode = process.exitValue()
            logger.info("Renovate for {} exited with code {}", repo, exitCode)
            // The rate-limit verdict is in the last lines, so read them all before deciding.
            awaitOutput(pumps, repo)
            return rateLimit.result()
                ?.also { logger.warn("Renovate for {} was rate-limited (retry after {})", repo, it.retryAfter) }
                ?: if (exitCode == 0) JobExecutionResult.Success else JobExecutionResult.Failed(exitCode)
        } finally {
            withContext(NonCancellable + ioDispatcher) {
                killProcessTree(process)
                awaitOutput(pumps, repo)
            }
        }
    }

    private suspend fun awaitOutput(pumps: Job, repo: RepoRef) {
        if (pumps.isCompleted) return
        withTimeoutOrNull(OUTPUT_DRAIN_TIMEOUT) { pumps.join() }
            ?: logger.warn("Output of renovate for {} still open after exit; abandoning", repo)
    }

    fun buildEnvironment(repo: RepoRef, creds: PlatformCredentials): Map<String, String> = buildMap {
        putAll(inheritedEnv.filterKeys { !isStripped(it) })
        put("LOG_FORMAT", "json")
        put("RENOVATE_PLATFORM", repo.platform.name.lowercase())
        put("RENOVATE_TOKEN", creds.token)
        creds.endpoint?.let { put("RENOVATE_ENDPOINT", it) }
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

    private fun pump(stream: InputStream, name: String, inspect: (String) -> Unit = {}) {
        try {
            stream.bufferedReader().useLines { lines ->
                lines.forEach { line ->
                    inspect(line)
                    outputLogger.log(line, name)
                }
            }
        } catch (@Suppress("SwallowedException") _: IOException) {
            // Stream closed while the process was being killed
        }
    }

    private fun killProcessTree(process: Process) {
        // Snapshot first: once the root dies its children are reparented and no longer reachable.
        val tree = listOf(process.toHandle()) + process.descendants().toList()
        tree.forEach { it.destroyForcibly() }
        // Wait for the whole tree, so nothing still writes to the workspace while it is deleted.
        val deadline = System.nanoTime() + KILL_WAIT.inWholeNanoseconds
        tree.forEach { handle ->
            val remaining = (deadline - System.nanoTime()).coerceAtLeast(0)
            runCatching { handle.onExit().get(remaining, TimeUnit.NANOSECONDS) }
        }
    }

    companion object {
        private val OUTPUT_DRAIN_TIMEOUT = 2.seconds
        private val KILL_WAIT = 5.seconds

        /** Worker settings and secrets that must not leak into Renovate (or repo code it runs). */
        private val STRIPPED_ENV = setOf(
            "GITHUB_TOKEN",
            "GITLAB_TOKEN",
            "RENOVATE_ENDPOINT",
            "INTERNAL_TRIGGER_TOKEN",
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
