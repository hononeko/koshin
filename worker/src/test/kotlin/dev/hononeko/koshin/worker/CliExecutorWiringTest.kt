package dev.hononeko.koshin.worker

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.PlatformCredentials
import dev.hononeko.koshin.core.model.RepoRef
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.IOException
import kotlin.time.Duration.Companion.minutes

class CliExecutorWiringTest {

    @Test
    fun `executor gets the repo cache settings and cache dir`() {
        val settings = WorkerSettings.fromEnv(
            mapOf("RENOVATE_BASE_DIR" to "/work", "RENOVATE_CACHE_DIR" to "/cache"),
        )

        val executor = cliExecutor(settings)

        assertTrue(executor is RenovateProcessExecutor)
        val env = (executor as RenovateProcessExecutor)
            .buildEnvironment(RepoRef(Platform.GITHUB, "hononeko/koshin"), PlatformCredentials("token"))
        assertEquals("local", env["RENOVATE_REPOSITORY_CACHE_TYPE"])
        assertEquals("/cache", env["RENOVATE_CACHE_DIR"])
        assertEquals("/work", env["RENOVATE_BASE_DIR"])
    }

    @Test
    fun `RUNNER_SOCKET sends runs to the runner`() = runBlocking {
        val settings = WorkerSettings.fromEnv(mapOf("RUNNER_SOCKET" to "/nonexistent/koshin/runner.sock"))

        val repo = RepoRef(Platform.GITHUB, "hononeko/koshin")
        val executor = cliExecutor(settings)

        val error = assertThrows<IOException> { executor.execute(repo, PlatformCredentials("t"), 1.minutes) }
        assertTrue(error.message!!.contains("/nonexistent/koshin/runner.sock"), error.message)
    }

    @Test
    fun `pod deletion cost wraps the executor when enabled`() {
        val settings = WorkerSettings.fromEnv(
            mapOf("POD_DELETION_COST_ENABLED" to "true", "POD_NAME" to "worker-0", "POD_NAMESPACE" to "koshin"),
        )

        assertTrue(cliExecutor(settings) is PodDeletionCostCliExecutor)
    }
}
