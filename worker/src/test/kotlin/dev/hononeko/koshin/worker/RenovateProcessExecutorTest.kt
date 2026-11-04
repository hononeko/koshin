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
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import org.slf4j.LoggerFactory
import java.nio.file.Path
import kotlin.io.path.exists
import kotlin.io.path.readLines
import kotlin.io.path.readText
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.nanoseconds
import kotlin.time.Duration.Companion.seconds

class RenovateProcessExecutorTest {

    @TempDir
    lateinit var baseDir: Path

    private val repo = RepoRef(Platform.GITHUB, "hononeko/koshin")
    private val gitlabRepo = RepoRef(Platform.GITLAB, "group/project")
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
        redactor: SecretRedactor = SecretRedactor(),
    ) = RenovateProcessExecutor(
        baseDir = baseDir,
        redisUrl = "redis://cache:6379",
        command = listOf("sh", "-c", script, "renovate"),
        inheritedEnv = inheritedEnv,
        requestLimits = requestLimits,
        redactor = redactor,
    )

    /** The WARN messages [block] logs from [RenovateProcessExecutor]. */
    private fun <T> warnings(block: () -> T): Pair<T, List<String>> {
        val logger = LoggerFactory.getLogger(RenovateProcessExecutor::class.java) as ch.qos.logback.classic.Logger
        val appender = ListAppender<ILoggingEvent>().apply { start() }
        logger.addAppender(appender)
        val result = try {
            block()
        } finally {
            logger.detachAppender(appender)
        }
        return result to appender.list.filter { it.level == Level.WARN }.map { it.formattedMessage }
    }

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
    fun `a GitHub App run gets the bot identity`() {
        val built = executor("true").buildEnvironment(
            repo,
            PlatformCredentials("ghs_app", username = "koshin[bot]", gitAuthor = "koshin[bot] <1+koshin[bot]@x>"),
        )
        assertEquals("ghs_app", built["RENOVATE_TOKEN"])
        assertEquals("koshin[bot]", built["RENOVATE_USERNAME"])
        assertEquals("koshin[bot] <1+koshin[bot]@x>", built["RENOVATE_GIT_AUTHOR"])
        assertFalse(executor("true").buildEnvironment(repo, creds).containsKey("RENOVATE_USERNAME"))
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

    /** Daemonizes (`setsid`) a process, records its PID, then does [then]. */
    private fun daemonThen(then: String) = """
        setsid sh -c 'echo ${'$'}${'$'} > "${'$'}RENOVATE_BASE_DIR/daemon.pid"; exec sleep 300' > /dev/null 2>&1 &
        while [ ! -s "${'$'}RENOVATE_BASE_DIR/daemon.pid" ]; do sleep 0.05; done
        $then
    """.trimIndent()

    /** Neither running nor a zombie: no `/proc` entry left (KSH-23). */
    private fun assertReaped(pid: Long) {
        val deadline = System.nanoTime() + 10.seconds.inWholeNanoseconds
        while (Path.of("/proc/$pid").exists()) {
            check(System.nanoTime() < deadline) { "Process $pid is still there: ${procState(pid)}" }
            Thread.sleep(20)
        }
    }

    private fun procState(pid: Long) = runCatching { Path.of("/proc/$pid/stat").readText() }.getOrDefault("gone")

    @Test
    fun `a daemon the run leaves behind is killed and reaped when Renovate exits`() = runBlocking {
        assumeTrue(Posix.becomeChildSubreaper(), "child subreapers are Linux-only")
        val started = System.nanoTime()

        val result = executor(daemonThen("exit 0")).execute(repo, creds, 1.minutes)

        assertEquals(JobExecutionResult.Success, result)
        assertReaped(pidFrom("daemon.pid"))
        val elapsed = (System.nanoTime() - started).nanoseconds
        assertTrue(elapsed < 4.seconds, "no wait for a zombie to exit: $elapsed")
    }

    @Test
    fun `a daemon the run leaves behind is killed and reaped on a timeout`() = runBlocking {
        assumeTrue(Posix.becomeChildSubreaper(), "child subreapers are Linux-only")

        val result = executor(daemonThen("sleep 300")).execute(repo, creds, 2.seconds)

        assertEquals(JobExecutionResult.TimedOut, result)
        assertReaped(pidFrom("daemon.pid"))
    }

    @Test
    fun `a daemon the run leaves behind is killed and reaped on a cancellation`() = runBlocking {
        assumeTrue(Posix.becomeChildSubreaper(), "child subreapers are Linux-only")
        val result = async { executor(daemonThen("sleep 300")).execute(repo, creds, 10.minutes) }
        val daemon = pidFrom("daemon.pid")

        result.cancelAndJoin()

        assertTrue(result.isCancelled)
        assertReaped(daemon)
    }

    @Test
    fun `reaping a process that isn't this JVM's child is a no-op`() {
        assertTrue(Posix.reap(ProcessHandle.current().pid()))
        assertTrue(Posix.reap(Long.MAX_VALUE / 2))
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

    @Test
    fun `the private key paths reach Renovate, the raw private keys don't`() {
        val inherited = env + mapOf(
            "RENOVATE_PRIVATE_KEY_PATH" to "/etc/renovate-encryption/private.asc",
            "RENOVATE_PRIVATE_KEY_PATH_OLD" to "/etc/renovate-encryption/private-old.asc",
            "RENOVATE_PRIVATE_KEY" to "-----BEGIN PGP PRIVATE KEY BLOCK-----",
            "RENOVATE_PRIVATE_KEY_OLD" to "-----BEGIN PGP PRIVATE KEY BLOCK-----",
        )

        val built = executor("true", inherited).buildEnvironment(repo, creds)

        assertEquals("/etc/renovate-encryption/private.asc", built["RENOVATE_PRIVATE_KEY_PATH"])
        assertEquals("/etc/renovate-encryption/private-old.asc", built["RENOVATE_PRIVATE_KEY_PATH_OLD"])
        assertFalse(built.containsKey("RENOVATE_PRIVATE_KEY"))
        assertFalse(built.containsKey("RENOVATE_PRIVATE_KEY_OLD"))
    }

    @Test
    fun `repositories can't run code in Renovate, whatever the worker's environment says`() {
        val logger = LoggerFactory.getLogger(RenovateProcessExecutor::class.java) as ch.qos.logback.classic.Logger
        val appender = ListAppender<ILoggingEvent>().apply { start() }
        logger.addAppender(appender)
        val inherited = env + mapOf(
            "RENOVATE_ALLOW_SCRIPTS" to "true",
            "RENOVATE_ALLOWED_UNSAFE_EXECUTIONS" to "[\"gradleWrapper\"]",
            "RENOVATE_ALLOW_PLUGINS" to "false",
        )

        val built = try {
            executor("true", inherited).buildEnvironment(repo, creds)
        } finally {
            logger.detachAppender(appender)
        }

        assertEquals("false", built["RENOVATE_ALLOW_SCRIPTS"])
        assertEquals("[]", built["RENOVATE_ALLOWED_UNSAFE_EXECUTIONS"])
        assertEquals("false", built["RENOVATE_ALLOW_PLUGINS"])
        assertEquals("false", built["RENOVATE_ALLOW_CUSTOM_CRATE_GIT_REGISTRIES"])
        assertEquals(
            listOf(
                "Ignoring RENOVATE_ALLOW_SCRIPTS=true: koshin pins it to false (KSH-25)",
                "Ignoring RENOVATE_ALLOWED_UNSAFE_EXECUTIONS=[\"gradleWrapper\"]: koshin pins it to [] (KSH-25)",
            ),
            appender.list.filter { it.level == Level.WARN }.map { it.formattedMessage },
        )
    }

    @Test
    fun `with a private key, postUpgradeTasks run nothing`() {
        val logger = LoggerFactory.getLogger(RenovateProcessExecutor::class.java) as ch.qos.logback.classic.Logger
        val appender = ListAppender<ILoggingEvent>().apply { start() }
        logger.addAppender(appender)
        val allowed = env + mapOf(
            "RENOVATE_ALLOWED_COMMANDS" to "[\"^npm run .*\"]",
            "RENOVATE_ALLOW_SHELL_EXECUTOR_FOR_POST_UPGRADE_COMMANDS" to "true",
        )

        val keys = listOf(
            "RENOVATE_PRIVATE_KEY_PATH" to "/k/private.asc",
            "RENOVATE_PRIVATE_KEY_PATH_OLD" to "/k/old.asc",
        )
        val built = try {
            keys.map { key -> executor("true", allowed + key).buildEnvironment(repo, creds) }
        } finally {
            logger.detachAppender(appender)
        }

        built.forEach { renovateEnv ->
            assertEquals("[]", renovateEnv["RENOVATE_ALLOWED_COMMANDS"])
            assertEquals("false", renovateEnv["RENOVATE_ALLOW_SHELL_EXECUTOR_FOR_POST_UPGRADE_COMMANDS"])
        }
        assertEquals(
            2,
            appender.list.count {
                it.formattedMessage == "Ignoring RENOVATE_ALLOWED_COMMANDS=[\"^npm run .*\"]: koshin pins it to [] " +
                    "while Renovate has a private key (KSH-25)"
            },
        )
    }

    @Test
    fun `without a private key, the commands an operator allows for postUpgradeTasks are passed on`() {
        val inherited = env + mapOf(
            "RENOVATE_ALLOWED_COMMANDS" to "[\"^npm run .*\"]",
            "RENOVATE_PRIVATE_KEY_PATH" to " ",
        )

        val built = executor("true", inherited).buildEnvironment(repo, creds)

        assertEquals("[\"^npm run .*\"]", built["RENOVATE_ALLOWED_COMMANDS"])
        assertFalse(built.containsKey("RENOVATE_ALLOW_SHELL_EXECUTOR_FOR_POST_UPGRADE_COMMANDS"))
    }

    @Test
    fun `github_com runs never get an operator's github_com token, GitLab and GHES runs do (KSH-26)`() {
        val inherited = env + mapOf("GITHUB_COM_TOKEN" to "ghp_operator", "RENOVATE_GITHUB_COM_TOKEN" to "ghp_other")
        val executor = executor("true", inherited)

        // No endpoint is github.com; so is one that can't be parsed, to be safe
        listOf(null, "https://api.github.com", "https://API.github.com/", "not a uri").forEach { endpoint ->
            val built = executor.buildEnvironment(repo, PlatformCredentials("ghs_run", endpoint, installationId = 7))
            assertNull(built["GITHUB_COM_TOKEN"], "endpoint $endpoint")
            assertNull(built["RENOVATE_GITHUB_COM_TOKEN"], "endpoint $endpoint")
            assertEquals("ghs_run", built["RENOVATE_TOKEN"])
        }
        val gitlab = executor.buildEnvironment(gitlabRepo, PlatformCredentials("glpat-x"))
        val ghes = executor.buildEnvironment(repo, creds)
        listOf(gitlab, ghes).forEach { built ->
            assertEquals("ghp_operator", built["GITHUB_COM_TOKEN"])
            assertEquals("ghp_other", built["RENOVATE_GITHUB_COM_TOKEN"])
        }
    }

    @Test
    fun `nothing private goes into the package cache, which is keyed per credential scope (KSH-26)`() {
        val inherited = env + mapOf(
            "RENOVATE_CACHE_PRIVATE_PACKAGES" to "true",
            "RENOVATE_PRESET_CACHE_PERSISTENCE" to "false",
        )

        val (executor, warned) = warnings { executor("true", inherited) }
        val installation = executor.buildEnvironment(repo, PlatformCredentials("ghs_run", installationId = 42))
        val gitlab = executor.buildEnvironment(gitlabRepo, PlatformCredentials("glpat-x"))

        assertEquals("false", installation["RENOVATE_CACHE_PRIVATE_PACKAGES"])
        assertEquals("false", installation["RENOVATE_PRESET_CACHE_PERSISTENCE"])
        assertEquals("koshin:github:inst-42:", installation["RENOVATE_REDIS_PREFIX"])
        assertEquals("koshin:github:pat:", executor.buildEnvironment(repo, creds)["RENOVATE_REDIS_PREFIX"])
        assertEquals("koshin:gitlab:pat:", gitlab["RENOVATE_REDIS_PREFIX"])
        assertEquals("false", gitlab["RENOVATE_CACHE_PRIVATE_PACKAGES"])
        assertEquals(listOf("Ignoring RENOVATE_CACHE_PRIVATE_PACKAGES=true: koshin pins it to false (KSH-26)"), warned)
    }

    @Test
    fun `an operator's RENOVATE_REDIS_PREFIX shares the package cache between scopes`() {
        val (executor, warned) = warnings { executor("true", env + ("RENOVATE_REDIS_PREFIX" to "renovate:")) }

        val built = executor.buildEnvironment(repo, PlatformCredentials("ghs_run", installationId = 42))

        assertEquals("renovate:", built["RENOVATE_REDIS_PREFIX"])
        assertEquals(
            listOf("RENOVATE_REDIS_PREFIX=renovate:: every credential scope shares Renovate's package cache (KSH-26)"),
            warned,
        )
    }

    @Test
    fun `the run's token is scrubbed from its output while it runs (KSH-26)`() = runBlocking {
        val logger = LoggerFactory.getLogger(RenovateOutputLogger.LOGGER_NAME) as ch.qos.logback.classic.Logger
        val appender = ListAppender<ILoggingEvent>().apply { start() }
        logger.addAppender(appender)
        logger.level = Level.TRACE
        val redactor = SecretRedactor()
        val token = "gitlab-group-token-1234"
        val script = """
            echo "fatal: remote oauth2:${'$'}RENOVATE_TOKEN@gitlab.example.com/group/project.git" >&2
            echo '{"level":30,"msg":"token '"${'$'}RENOVATE_TOKEN"'"}'
        """.trimIndent()
        try {
            executor(script, redactor = redactor)
                .execute(RepoRef(Platform.GITLAB, "group/project"), PlatformCredentials(token), 1.minutes)
        } finally {
            logger.detachAppender(appender)
            logger.level = null
        }

        assertEquals(
            listOf(
                "fatal: remote oauth2:**redacted**@gitlab.example.com/group/project.git",
                "token **redacted**",
            ),
            appender.list.map { it.formattedMessage }.sorted(),
        )
        assertFalse(redactor.containsSecret(token), "only tracked while the run lasts")
    }
}
