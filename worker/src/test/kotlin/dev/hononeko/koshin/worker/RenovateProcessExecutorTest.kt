package dev.hononeko.koshin.worker

import dev.hononeko.koshin.core.model.JobExecutionResult
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.PlatformCredentials
import dev.hononeko.koshin.core.model.RepoRef
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

    private fun executor(script: String) = RenovateProcessExecutor(
        baseDir = baseDir,
        redisUrl = "redis://cache:6379",
        command = listOf("sh", "-c", script, "renovate"),
        inheritedEnv = env,
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
}
