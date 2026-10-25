package dev.hononeko.koshin.worker

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.PlatformCredentials
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.model.TriggerReason
import dev.hononeko.koshin.core.ports.AcquireResult
import dev.hononeko.koshin.core.ports.PlatformThrottlePort
import dev.hononeko.koshin.core.usecase.ProcessRenovateJobUseCase
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import kotlin.io.path.createDirectories
import kotlin.io.path.exists
import kotlin.io.path.readText
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class WorkerShutdownTest {

    @TempDir
    lateinit var baseDir: Path

    private val repo = RepoRef(Platform.GITHUB, "hononeko/koshin")

    private fun awaitPid(file: Path): Long {
        val deadline = System.nanoTime() + 10.seconds.inWholeNanoseconds
        while (!(file.exists() && file.readText().isNotBlank())) {
            check(System.nanoTime() < deadline) { "Timed out waiting for $file" }
            Thread.sleep(20)
        }
        return file.readText().trim().toLong()
    }

    private fun isAlive(pid: Long): Boolean = ProcessHandle.of(pid).filter { it.isAlive }.isPresent

    private fun awaitDead(pid: Long) {
        val deadline = System.nanoTime() + 10.seconds.inWholeNanoseconds
        while (isAlive(pid)) {
            check(System.nanoTime() < deadline) { "Process $pid is still alive" }
            Thread.sleep(20)
        }
    }

    @Test
    fun `SIGTERM NAKs the in-flight delivery, releases the lock keeping the dirty flag, and kills the CLI`() {
        val delivery = RecordingDelivery(RenovateJobEvent(repo, "delivery-1", TriggerReason.WEBHOOK))
        val coordination = InMemoryCoordination()
        val publisher = RecordingPublisher()
        val throttle = mockk<PlatformThrottlePort>(relaxed = true)
        coEvery { throttle.cooldownRemaining(any()) } returns null
        coEvery { throttle.tryAcquireRunSlot(any(), any(), any()) } returns true
        coEvery { throttle.extendRunSlot(any(), any(), any()) } returns true

        val cleaner = WorkspaceCleaner(baseDir)
        val workspace = cleaner.workspaceFor(repo)
        val pidDir = baseDir.resolve("pids").createDirectories()
        // Simulates Renovate: clones into the workspace, then hangs with a child process.
        val script = """
            mkdir -p "$workspace" && touch "$workspace/package.json"
            sleep 300 & echo ${'$'}! > "$pidDir/child.pid"
            echo ${'$'}${'$'} > "$pidDir/cli.pid"
            wait
        """.trimIndent()
        val executor = RenovateProcessExecutor(
            baseDir = baseDir,
            command = listOf("sh", "-c", script, "renovate"),
            inheritedEnv = mapOf("PATH" to System.getenv("PATH")),
        )

        val useCase = ProcessRenovateJobUseCase(
            queueConsumer = QueueConsumer(delivery),
            queuePublisher = publisher,
            coordinationPort = coordination,
            throttlePort = throttle,
            cachePort = NoOpRepoCache,
            credentialsPort = StaticCredentialsProvider(mapOf(Platform.GITHUB to PlatformCredentials("token"))),
            cliExecutor = executor,
            workspaceCleaner = cleaner,
        )
        val daemon = WorkerDaemon(setOf(Platform.GITHUB)) { useCase }
        val server = createWorkerServer(
            settings = WorkerSettings(port = 0, renovateBaseDir = baseDir),
            onStopping = { daemon.stop() },
        ).start(wait = false)
        daemon.start()

        val cliPid = awaitPid(pidDir.resolve("cli.pid"))
        val childPid = awaitPid(pidDir.resolve("child.pid"))
        assertTrue(workspace.exists())

        // A webhook for the same repo arrives mid-run and is coalesced into the dirty flag.
        val coalesced = runBlocking { coordination.acquireOrMarkDirty(repo, "other-worker", 45.seconds, 60.minutes) }
        assertEquals(AcquireResult.COALESCED, coalesced)

        // Same path as SIGTERM: Ktor's shutdown hook calls stop(), which raises ApplicationStopping.
        server.stop(100, 1000)

        assertEquals(listOf(Duration.ZERO), delivery.nacks)
        assertTrue(delivery.acks.isEmpty())
        assertFalse(coordination.locks.containsKey(repo), "lock should be released")
        assertTrue(repo in coordination.dirty, "dirty flag should be kept")
        assertTrue(publisher.published.isEmpty(), "no follow-up is published on shutdown")
        coVerify(exactly = 1) { throttle.releaseRunSlot(Platform.GITHUB, any()) }
        assertFalse(workspace.exists(), "workspace should be cleaned up")
        awaitDead(cliPid)
        awaitDead(childPid)
    }
}
