package dev.hononeko.koshin.worker

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import dev.hononeko.koshin.core.model.JobExecutionResult
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.PlatformCredentials
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.model.TriggerReason
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import org.slf4j.LoggerFactory
import java.nio.file.Path
import kotlin.io.path.exists
import kotlin.io.path.readLines
import kotlin.io.path.readText
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class RenovateProcessExecutorTest {

    @TempDir
    lateinit var baseDir: Path

    private val repo = RepoRef(Platform.GITHUB, "hononeko/koshin")
    private val creds = PlatformCredentials("ghp_secret", "https://ghe.example.com/api/v3")
    private val env = mapOf(
        "PATH" to System.getenv("PATH"),
        "GITHUB_TOKEN" to "worker-pat",
        "VALKEY_URL" to "redis://:pw@valkey:6379",
        "NATS_URL" to "nats://koshin:pw@nats:4222",
        "GITHUB_APP_PRIVATE_KEY_PATH" to "/secrets/key.pem",
        "GITLAB_WEBHOOK_SECRET" to "hook",
        "RENOVATE_ENDPOINT" to "https://wrong.example.com",
        "RENOVATE_ONBOARDING" to "false",
    )

    private fun executor(
        script: String,
        inheritedEnv: Map<String, String> = env,
        requestLimits: RenovateRequestLimits = RenovateRequestLimits(),
    ) = RenovateProcessExecutor(
        baseDir = baseDir,
        redisUrl = "redis://cache:6379",
        command = listOf("sh", "-c", script, "renovate"),
        inheritedEnv = inheritedEnv,
        requestLimits = requestLimits,
    )

    /** Spawns a child that spawns a grandchild, records both PIDs, then hangs. */
    private val hangingTree = """
        sh -c 'sleep 300 & echo ${'$'}! > "${'$'}RENOVATE_BASE_DIR/grandchild.pid"; wait' &
        echo ${'$'}! > "${'$'}RENOVATE_BASE_DIR/child.pid"
        wait
    """.trimIndent()

    private suspend fun awaitFile(name: String): Path {
        val file = baseDir.resolve(name)
        val deadline = System.nanoTime() + 10.seconds.inWholeNanoseconds
        while (!(file.exists() && file.readText().isNotBlank())) {
            check(System.nanoTime() < deadline) { "Timed out waiting for $file" }
            delay(20)
        }
        return file
    }

    private suspend fun pidFrom(name: String): Long = awaitFile(name).readLines().first().trim().toLong()

    private fun assertDead(pid: Long) {
        val deadline = System.nanoTime() + 10.seconds.inWholeNanoseconds
        while (ProcessHandle.of(pid).filter { it.isAlive }.isPresent) {
            check(System.nanoTime() < deadline) { "Process $pid is still alive" }
            Thread.sleep(20)
        }
    }

    @Test
    fun `exit code 0 maps to Success and the repository is passed as the argument`() = runBlocking {
        val result = executor("""echo "${'$'}1" > "${'$'}RENOVATE_BASE_DIR/arg.txt"""").execute(repo, creds, 1.minutes)
        assertEquals(JobExecutionResult.Success, result)
        assertEquals("hononeko/koshin", baseDir.resolve("arg.txt").readText().trim())
    }

    @Test
    fun `non-zero exit maps to Failed`() = runBlocking {
        assertEquals(
            JobExecutionResult.Failed(3),
            executor("echo out; echo err >&2; exit 3").execute(repo, creds, 1.minutes),
        )
    }

    @Test
    fun `process gets per-run Renovate environment without worker secrets`() = runBlocking {
        executor("""env > "${'$'}RENOVATE_BASE_DIR/env.txt"""").execute(repo, creds, 1.minutes)
        val seen = baseDir.resolve("env.txt").readLines()
            .filter { it.contains('=') }
            .associate { it.substringBefore('=') to it.substringAfter('=') }

        assertEquals("json", seen["LOG_FORMAT"])
        assertEquals("github", seen["RENOVATE_PLATFORM"])
        assertEquals("ghp_secret", seen["RENOVATE_TOKEN"])
        assertEquals("https://ghe.example.com/api/v3", seen["RENOVATE_ENDPOINT"])
        assertEquals(baseDir.toString(), seen["RENOVATE_BASE_DIR"])
        assertEquals("global", seen["RENOVATE_BINARY_SOURCE"])
        assertEquals("redis://cache:6379", seen["RENOVATE_REDIS_URL"])
        assertEquals("enabled", seen["RENOVATE_REPOSITORY_CACHE"])
        assertEquals("false", seen["RENOVATE_ONBOARDING"])
        assertNull(seen["GITHUB_TOKEN"])
        assertNull(seen["VALKEY_URL"])
        assertNull(seen["NATS_URL"])
        assertNull(seen["GITHUB_APP_PRIVATE_KEY_PATH"])
        assertNull(seen["GITLAB_WEBHOOK_SECRET"])
    }

    @Test
    fun `inherited RENOVATE_ENDPOINT is dropped when credentials have no endpoint`() {
        val built = executor("true").buildEnvironment(
            RepoRef(Platform.GITLAB, "group/project"),
            PlatformCredentials("glpat"),
        )
        assertEquals("gitlab", built["RENOVATE_PLATFORM"])
        assertFalse(built.containsKey("RENOVATE_ENDPOINT"))
    }

    @Test
    fun `timeout returns TimedOut and kills the whole process tree`() = runBlocking {
        val result = async { executor(hangingTree).execute(repo, creds, 2.seconds) }
        val child = pidFrom("child.pid")
        val grandchild = pidFrom("grandchild.pid")

        assertEquals(JobExecutionResult.TimedOut, result.await())
        assertDead(child)
        assertDead(grandchild)
    }

    @Test
    fun `cancellation kills the whole process tree and propagates`() = runBlocking {
        val result = async { executor(hangingTree).execute(repo, creds, 10.minutes) }
        val child = pidFrom("child.pid")
        val grandchild = pidFrom("grandchild.pid")

        result.cancelAndJoin()

        assertTrue(result.isCancelled)
        assertDead(child)
        assertDead(grandchild)
    }

    @Test
    fun `rejects repositories that look like CLI options`() = runBlocking {
        assertThrows<IllegalArgumentException> {
            executor("true").execute(RepoRef(Platform.GITLAB, "-g/project"), creds, 1.minutes)
        }
        Unit
    }

    @Test
    fun `a rate-limit abort maps to RateLimited even though Renovate exits 0`() = runBlocking {
        val script = """
            echo '{"level":40,"msg":"GitHub failure: secondary rate limit","err":{"response":{"statusCode":403,"headers":{"retry-after":"60"}}}}'
            echo '{"level":40,"msg":"Rate limit exceeded - aborting"}'
            echo '{"level":30,"msg":"Repository finished","result":"rate-limit-exceeded"}'
            exit 0
        """.trimIndent()
        assertEquals(JobExecutionResult.RateLimited(60.seconds), executor(script).execute(repo, creds, 1.minutes))
    }

    @Test
    fun `request limits become a host rule for the platform API host`() {
        val limits = RenovateRequestLimits(maxRequestsPerSecond = 2.5, concurrentRequestLimit = 4)
        val built = executor("true", requestLimits = limits).buildEnvironment(repo, creds)
        assertEquals(
            """[{"matchHost":"ghe.example.com","maxRequestsPerSecond":2.5,"concurrentRequestLimit":4}]""",
            built["RENOVATE_HOST_RULES"],
        )

        val gitlab = executor("true", requestLimits = RenovateRequestLimits(concurrentRequestLimit = 2))
            .buildEnvironment(RepoRef(Platform.GITLAB, "group/project"), PlatformCredentials("glpat"))
        assertEquals("""[{"matchHost":"gitlab.com","concurrentRequestLimit":2}]""", gitlab["RENOVATE_HOST_RULES"])
    }

    @Test
    fun `request limits are appended to inherited host rules`() {
        val inherited = env + ("RENOVATE_HOST_RULES" to """[{"matchHost":"registry.npmjs.org","token":"npm"}]""")
        val limits = RenovateRequestLimits(maxRequestsPerSecond = 1.0)
        val built = executor("true", inherited, limits).buildEnvironment(repo, creds)
        assertEquals(
            """[{"matchHost":"registry.npmjs.org","token":"npm"},""" +
                """{"matchHost":"ghe.example.com","maxRequestsPerSecond":1.0}]""",
            built["RENOVATE_HOST_RULES"],
        )

        val invalid = env + ("RENOVATE_HOST_RULES" to "not json")
        assertEquals("not json", executor("true", invalid, limits).buildEnvironment(repo, creds)["RENOVATE_HOST_RULES"])
    }

    @Test
    fun `without request limits no host rule is added and koshin-only settings are stripped`() {
        val inherited = env + mapOf(
            "RENOVATE_MAX_REQUESTS_PER_SECOND" to "2",
            "RENOVATE_CONCURRENT_REQUEST_LIMIT" to "4",
        )
        val built = executor("true", inherited).buildEnvironment(repo, creds)
        assertFalse(built.containsKey("RENOVATE_HOST_RULES"))
        assertFalse(built.containsKey("RENOVATE_MAX_REQUESTS_PER_SECOND"))
        assertFalse(built.containsKey("RENOVATE_CONCURRENT_REQUEST_LIMIT"))
    }

    @Test
    fun `renovate output carries the job's MDC and Renovate's level`() = runBlocking {
        val logger = LoggerFactory.getLogger(RenovateOutputLogger.LOGGER_NAME) as ch.qos.logback.classic.Logger
        val appender = ListAppender<ILoggingEvent>().apply { start() }
        logger.addAppender(appender)
        logger.level = Level.TRACE
        val event = RenovateJobEvent(repo, "delivery-7", TriggerReason.SCHEDULE)
        try {
            MdcJobContext.within(event, 1) {
                executor("""echo '{"level":50,"msg":"Repository failed","repository":"hononeko/koshin"}'""")
                    .execute(repo, creds, 1.minutes)
            }
        } finally {
            logger.detachAppender(appender)
            logger.level = null
        }

        val line = appender.list.single()
        assertEquals(Level.ERROR, line.level)
        assertEquals("Repository failed", line.formattedMessage)
        assertEquals("github:hononeko/koshin", line.mdcPropertyMap["repo"])
        assertEquals("delivery-7", line.mdcPropertyMap["deliveryId"])
    }

    @Test
    fun `koshin's log levels stay out of Renovate's environment`() {
        val built = executor("true", env + mapOf("LOG_LEVEL" to "debug", "RENOVATE_LOG_LEVEL" to "warn"))
            .buildEnvironment(repo, creds)
        assertFalse(built.containsKey("LOG_LEVEL"))
        assertEquals("warn", built["RENOVATE_LOG_LEVEL"])
    }
}
