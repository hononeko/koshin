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
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import org.slf4j.LoggerFactory
import java.io.IOException
import java.net.StandardProtocolFamily
import java.net.UnixDomainSocketAddress
import java.nio.channels.Channels
import java.nio.channels.SocketChannel
import java.nio.file.Path
import java.util.stream.Stream
import kotlin.io.path.exists
import kotlin.io.path.readLines
import kotlin.io.path.readText
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class RenovateRunnerTest {

    @TempDir
    lateinit var dir: Path

    private val repo = RepoRef(Platform.GITHUB, "hononeko/koshin")
    private val creds = PlatformCredentials("ghp_secret")
    private val socket: Path get() = dir.resolve("runner.sock")
    private val workDir: Path get() = dir.resolve("work")
    private var runner: RenovateRunner? = null
    private var serving: Thread? = null

    @AfterEach
    fun stopRunner() {
        runner?.close()
        serving?.join(10_000)
    }

    private fun startRunner(killer: ProcessKiller = ProcessKiller.TREE) {
        val runner = RenovateRunner(socket, LocalRenovateLauncher(killer)).also { runner = it }
        serving = Thread { runner.serve() }.apply {
            isDaemon = true
            start()
        }
        val deadline = System.nanoTime() + 10.seconds.inWholeNanoseconds
        while (!socket.exists()) {
            check(System.nanoTime() < deadline) { "Runner never bound $socket" }
            Thread.sleep(10)
        }
    }

    private fun executor(script: String, env: Map<String, String> = mapOf("PATH" to System.getenv("PATH"))) =
        RenovateProcessExecutor(
            baseDir = workDir,
            command = listOf("sh", "-c", script, "renovate"),
            inheritedEnv = env,
            launcher = RunnerRenovateLauncher(socket),
        )

    /** Spawns a child that spawns a grandchild, records both PIDs, then hangs. */
    private val hangingTree = """
        sh -c 'sleep 300 & echo ${'$'}! > "${'$'}RENOVATE_BASE_DIR/grandchild.pid"; wait' &
        echo ${'$'}! > "${'$'}RENOVATE_BASE_DIR/child.pid"
        wait
    """.trimIndent()

    private suspend fun pidFrom(file: Path): Long {
        val deadline = System.nanoTime() + 10.seconds.inWholeNanoseconds
        while (!(file.exists() && file.readText().isNotBlank())) {
            check(System.nanoTime() < deadline) { "Timed out waiting for $file" }
            delay(20)
        }
        return file.readLines().first().trim().toLong()
    }

    private fun assertDead(pid: Long) {
        val deadline = System.nanoTime() + 10.seconds.inWholeNanoseconds
        while (ProcessHandle.of(pid).filter { it.isAlive }.isPresent) {
            check(System.nanoTime() < deadline) { "Process $pid is still alive" }
            Thread.sleep(20)
        }
    }

    @Test
    fun `a run gets exactly the requested environment and reports its exit code`() = runBlocking {
        startRunner()
        val script = """
            env > "${'$'}RENOVATE_BASE_DIR/env.txt"
            echo "${'$'}1" > "${'$'}RENOVATE_BASE_DIR/arg.txt"
            exit 3
        """.trimIndent()

        assertEquals(JobExecutionResult.Failed(3), executor(script).execute(repo, creds, 1.minutes))

        val seen = workDir.resolve("env.txt").readLines()
            .filter { it.contains('=') }
            .associate { it.substringBefore('=') to it.substringAfter('=') }
        assertEquals("ghp_secret", seen["RENOVATE_TOKEN"])
        assertEquals("github", seen["RENOVATE_PLATFORM"])
        // The runner's own environment never reaches Renovate
        assertNotNull(System.getenv("HOME"))
        assertNull(seen["HOME"])
        assertEquals("hononeko/koshin", workDir.resolve("arg.txt").readText().trim())
    }

    @Test
    fun `output reaches the worker's log with the job's MDC before the exit code`() = runBlocking {
        startRunner()
        val logger = LoggerFactory.getLogger(RenovateOutputLogger.LOGGER_NAME) as ch.qos.logback.classic.Logger
        val appender = ListAppender<ILoggingEvent>().apply { start() }
        logger.addAppender(appender)
        logger.level = Level.TRACE
        val script = """
            echo '{"level":50,"msg":"Repository failed"}'
            echo 'git: something odd' >&2
            echo '{"level":40,"msg":"Rate limit exceeded - aborting"}'
        """.trimIndent()
        var result: JobExecutionResult? = null
        try {
            MdcJobContext.within(RenovateJobEvent(repo, "delivery-9", TriggerReason.SCHEDULE), 1) {
                result = executor(script).execute(repo, creds, 1.minutes)
            }
        } finally {
            logger.detachAppender(appender)
            logger.level = null
        }

        assertEquals(JobExecutionResult.RateLimited(null), result)
        val failed = appender.list.single { it.formattedMessage == "Repository failed" }
        assertEquals(Level.ERROR, failed.level)
        assertEquals("delivery-9", failed.mdcPropertyMap["deliveryId"])
        val stderr = appender.list.single { it.formattedMessage == "git: something odd" }
        assertEquals(Level.WARN, stderr.level)
    }

    @Test
    fun `a timeout kills the whole process tree in the runner`() = runBlocking {
        startRunner()
        val result = async { executor(hangingTree).execute(repo, creds, 2.seconds) }
        val child = pidFrom(workDir.resolve("child.pid"))
        val grandchild = pidFrom(workDir.resolve("grandchild.pid"))

        assertEquals(JobExecutionResult.TimedOut, result.await())
        assertDead(child)
        assertDead(grandchild)
    }

    @Test
    fun `cancellation kills the run in the runner and propagates`() = runBlocking {
        startRunner()
        val result = async { executor(hangingTree).execute(repo, creds, 10.minutes) }
        val child = pidFrom(workDir.resolve("child.pid"))
        val grandchild = pidFrom(workDir.resolve("grandchild.pid"))

        result.cancelAndJoin()

        assertTrue(result.isCancelled)
        assertDead(child)
        assertDead(grandchild)
    }

    @Test
    fun `a worker that goes away gets its run killed`() = runBlocking {
        startRunner()
        val request = RunRequest(
            command = listOf("sh", "-c", hangingTree),
            env = mapOf("PATH" to System.getenv("PATH"), "RENOVATE_BASE_DIR" to workDir.toString()),
            directory = workDir.toString(),
        )
        val channel = SocketChannel.open(UnixDomainSocketAddress.of(socket))
        Channels.newOutputStream(channel).apply { write((Json.encodeToString(request) + "\n").toByteArray()) }.flush()
        val child = pidFrom(workDir.resolve("child.pid"))
        val grandchild = pidFrom(workDir.resolve("grandchild.pid"))

        channel.close()

        assertDead(child)
        assertDead(grandchild)
    }

    @Test
    fun `processes that escaped the tree are swept before the exit code goes out`() = runBlocking {
        val daemonPid = dir.resolve("daemon.pid")
        startRunner(ProcessKiller.sweep { pidsIn(daemonPid) })
        val script = """( sh -c 'echo ${'$'}${'$'} > "$daemonPid"; exec sleep 300' & ); sleep 0.2; exit 0"""

        assertEquals(JobExecutionResult.Success, executor(script).execute(repo, creds, 1.minutes))

        assertDead(pidFrom(daemonPid))
    }

    @Test
    fun `the socket is bound afresh after every run`() = runBlocking {
        startRunner()
        // A run replacing the socket: without a fresh bind, the next run couldn't reach the runner.
        val hijack = executor("""rm -f "${'$'}SOCK"; : > "${'$'}SOCK"""", mapOf("SOCK" to socket.toString()))
        assertEquals(JobExecutionResult.Success, hijack.execute(repo, creds, 1.minutes))

        assertEquals(JobExecutionResult.Success, executor("exit 0").execute(repo, creds, 1.minutes))
    }

    @Test
    fun `a command that can't start fails the run`() = runBlocking {
        startRunner()
        val executor = RenovateProcessExecutor(
            baseDir = workDir,
            command = listOf(dir.resolve("missing-renovate").toString()),
            inheritedEnv = emptyMap(),
            launcher = RunnerRenovateLauncher(socket),
        )

        val error = assertThrows<IOException> { executor.execute(repo, creds, 1.minutes) }
        assertTrue(error.message!!.contains("could not start"), error.message)
        // The runner keeps serving
        assertEquals(JobExecutionResult.Success, executor("exit 0").execute(repo, creds, 1.minutes))
    }

    @Test
    fun `an invalid request is answered with a failure`() {
        startRunner()
        SocketChannel.open(StandardProtocolFamily.UNIX).use { channel ->
            channel.connect(UnixDomainSocketAddress.of(socket))
            Channels.newOutputStream(channel).apply { write("""{"command":[]}""".toByteArray() + '\n'.code.toByte()) }
                .flush()
            val reply = Channels.newInputStream(channel).bufferedReader().readLine()
            assertTrue(Json.decodeFromString<RunnerMessage>(reply) is RunnerMessage.Failed, reply)
        }
    }

    @Test
    fun `without a runner the run fails to start`() = runBlocking {
        val error = assertThrows<IOException> { executor("exit 0").execute(repo, creds, 1.minutes) }
        assertTrue(error.message!!.contains("Renovate runner"), error.message)
    }

    @Test
    fun `only the sweep reaches a daemonized process`() {
        val daemonPid = dir.resolve("daemon.pid")
        val script = """( sh -c 'echo ${'$'}${'$'} > "$daemonPid"; exec sleep 300' & ); sleep 0.2"""
        val process = ProcessBuilder("sh", "-c", script).start()
        process.waitFor()
        val pid = runBlocking { pidFrom(daemonPid) }

        ProcessKiller.TREE.kill(process)
        assertTrue(ProcessHandle.of(pid).filter { it.isAlive }.isPresent, "a daemon escapes the tree")

        ProcessKiller.sweep { pidsIn(daemonPid) }.kill(process)
        assertDead(pid)
    }

    private fun pidsIn(file: Path): Stream<ProcessHandle> =
        if (file.exists()) ProcessHandle.of(file.readText().trim().toLong()).stream() else Stream.empty()
}
