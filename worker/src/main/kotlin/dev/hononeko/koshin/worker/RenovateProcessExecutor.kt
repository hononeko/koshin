package dev.hononeko.koshin.worker

import dev.hononeko.koshin.core.model.JobExecutionResult
import dev.hononeko.koshin.core.model.PlatformCredentials
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.ports.CliExecutorPort
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import java.io.IOException
import java.io.InputStream
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
) : CliExecutorPort {

    private val logger = LoggerFactory.getLogger(RenovateProcessExecutor::class.java)
    private val outputLogger = LoggerFactory.getLogger("renovate")

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

        // Not children of the caller: a blocked read must never hold up cancellation.
        val pumps = CoroutineScope(ioDispatcher).launch {
            launch { pump(process.inputStream, repo, "stdout") }
            launch { pump(process.errorStream, repo, "stderr") }
        }

        try {
            val exited = withTimeoutOrNull(timeout) { process.onExit().await() }
            if (exited == null) {
                logger.warn("Renovate for {} exceeded {}; killing process tree", repo, timeout)
                return JobExecutionResult.TimedOut
            }
            val exitCode = process.exitValue()
            logger.info("Renovate for {} exited with code {}", repo, exitCode)
            return if (exitCode == 0) JobExecutionResult.Success else JobExecutionResult.Failed(exitCode)
        } finally {
            withContext(NonCancellable + ioDispatcher) {
                killProcessTree(process)
                withTimeoutOrNull(OUTPUT_DRAIN_TIMEOUT) { pumps.join() }
                    ?: logger.warn("Output of renovate for {} still open after exit; abandoning", repo)
            }
        }
    }

    fun buildEnvironment(repo: RepoRef, creds: PlatformCredentials): Map<String, String> = buildMap {
        putAll(inheritedEnv.filterKeys { it !in STRIPPED_ENV })
        put("LOG_FORMAT", "json")
        put("RENOVATE_PLATFORM", repo.platform.name.lowercase())
        put("RENOVATE_TOKEN", creds.token)
        creds.endpoint?.let { put("RENOVATE_ENDPOINT", it) }
        put("RENOVATE_BASE_DIR", baseDir.toString())
        put("RENOVATE_BINARYSOURCE", binarySource)
        redisUrl?.let { put("RENOVATE_REDIS_URL", it) }
        put("RENOVATE_REPOSITORY_CACHE", "enabled")
    }

    private fun pump(stream: InputStream, repo: RepoRef, name: String) {
        try {
            stream.bufferedReader().useLines { lines ->
                lines.forEach { line ->
                    outputLogger.atInfo()
                        .addKeyValue("repo", repo.toString())
                        .addKeyValue("stream", name)
                        .log(line)
                }
            }
        } catch (@Suppress("SwallowedException") _: IOException) {
            // Stream closed while the process was being killed
        }
    }

    private fun killProcessTree(process: Process) {
        // Snapshot first: once the root dies its children are reparented and no longer reachable.
        val descendants = process.descendants().toList()
        process.destroyForcibly()
        descendants.forEach { it.destroyForcibly() }
        runCatching { process.onExit().get(KILL_WAIT.inWholeMilliseconds, TimeUnit.MILLISECONDS) }
    }

    companion object {
        private val OUTPUT_DRAIN_TIMEOUT = 2.seconds
        private val KILL_WAIT = 5.seconds

        /** Worker secrets that must not leak into the Renovate process (or anything it spawns). */
        val STRIPPED_ENV = setOf(
            "GITHUB_TOKEN",
            "GITLAB_TOKEN",
            "RENOVATE_ENDPOINT",
            "VALKEY_URL",
            "GITHUB_APP_PRIVATE_KEY_PATH",
            "GITHUB_WEBHOOK_SECRET",
            "GITLAB_WEBHOOK_SECRET",
            "INTERNAL_TRIGGER_TOKEN",
        )
    }
}
