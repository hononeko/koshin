package dev.hononeko.koshin.worker

import dev.hononeko.koshin.core.model.CredentialScope
import dev.hononeko.koshin.core.model.JobExecutionResult
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.PlatformCredentials
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.model.TriggerReason
import dev.hononeko.koshin.core.ports.CliExecutorPort
import dev.hononeko.koshin.core.ports.PlatformThrottlePort
import dev.hononeko.koshin.core.ports.RepoCachePort
import dev.hononeko.koshin.core.usecase.ProcessRenovateJobUseCase
import io.mockk.coEvery
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.createDirectories
import kotlin.io.path.exists
import kotlin.io.path.isDirectory
import kotlin.io.path.writeText
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes

/** A run through the job use case leaves nothing in Renovate's cache dir for the next tenant. */
class RunCleanupTest {

    @TempDir
    lateinit var baseDir: Path

    private val repo = RepoRef(Platform.GITHUB, "hononeko/koshin")
    private val cacheDir by lazy { baseDir.resolve("cache") }
    private val cleaner by lazy { WorkspaceCleaner(baseDir) }
    private val repoCacheFile by lazy { cacheDir.resolve("renovate/repository/github/hononeko/koshin.json") }

    /** Plays Renovate: clones the repo and fills the cache dir, then ends with [result]. */
    private inner class Renovate(private val result: JobExecutionResult) : CliExecutorPort {
        override suspend fun execute(repo: RepoRef, creds: PlatformCredentials, timeout: Duration): JobExecutionResult {
            cleaner.workspaceFor(repo).resolve(".git").createDirectories()
            repoCacheFile.parent.createDirectories()
            repoCacheFile.writeText("""{"repository":"hononeko/koshin"}""")
            cacheDir.resolve("renovate/http-cache").createDirectories().resolve("org-preset").writeText("private")
            cacheDir.resolve("others/go").createDirectories().resolve("mod").writeText("module")
            return result
        }
    }

    /** Records whether Renovate's cache file was still there to upload. */
    private inner class RecordingRepoCache : RepoCachePort {
        val persisted = mutableListOf<Boolean>()

        override suspend fun restore(repo: RepoRef, scope: CredentialScope): Boolean = false

        override suspend fun persist(repo: RepoRef, scope: CredentialScope) {
            persisted += repoCacheFile.exists()
        }
    }

    private val throttle = mockk<PlatformThrottlePort>(relaxed = true) {
        coEvery { extendRunSlot(any(), any(), any()) } returns true
        coEvery { registerRateLimit(any(), any()) } returns 1.minutes
    }

    private fun filesUnder(dir: Path): List<Path> = Files.walk(dir).use { paths ->
        paths.filter { !it.isDirectory() }.toList()
    }

    @Test
    fun `the cache dir is empty after Success, Failed, TimedOut and RateLimited runs`() = runBlocking {
        val outcomes = listOf(
            JobExecutionResult.Success,
            JobExecutionResult.Failed(1),
            JobExecutionResult.TimedOut,
            JobExecutionResult.RateLimited(null),
        )
        outcomes.forEach { outcome ->
            val repoCache = RecordingRepoCache()
            val delivery = RecordingDelivery(RenovateJobEvent(repo, "delivery-$outcome", TriggerReason.WEBHOOK))
            val useCase = ProcessRenovateJobUseCase(
                queueConsumer = QueueConsumer(),
                queueDepth = QueueConsumer(),
                queuePublisher = RecordingPublisher(),
                coordinationPort = InMemoryCoordination(),
                throttlePort = throttle,
                cachePort = repoCache,
                credentialsPort = StaticCredentialsProvider(mapOf(Platform.GITHUB to PlatformCredentials("ghp_x"))),
                cliExecutor = Renovate(outcome),
                workspaceCleaner = cleaner,
            )

            useCase.processDelivery(delivery)

            assertEquals(1, delivery.acks.size, "$outcome")
            // A successful run's cache is uploaded before it's wiped
            assertEquals(if (outcome == JobExecutionResult.Success) listOf(true) else emptyList(), repoCache.persisted)
            assertEquals(emptyList<Path>(), filesUnder(baseDir), "$outcome")
            assertTrue(cacheDir.isDirectory(), "the cache dir itself is kept")
        }
    }
}
