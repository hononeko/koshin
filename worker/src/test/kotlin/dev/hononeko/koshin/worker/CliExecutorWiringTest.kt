package dev.hononeko.koshin.worker

import dev.hononeko.koshin.adapters.github.GitHubApp
import dev.hononeko.koshin.adapters.github.GitHubAppConfig
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.PlatformCredentials
import dev.hononeko.koshin.core.model.RepoRef
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import java.io.IOException
import java.nio.file.Path
import java.security.KeyPairGenerator
import java.util.Base64
import kotlin.io.path.writeText
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
    fun `GitHub runs use the App when it's configured`(@TempDir dir: Path) {
        val key = KeyPairGenerator.getInstance("RSA").apply { initialize(2048) }.generateKeyPair().private
        val keyFile = dir.resolve("key.pem")
        val body = Base64.getMimeEncoder().encodeToString(key.encoded)
        keyFile.writeText("-----BEGIN PRIVATE KEY-----\n$body\n-----END PRIVATE KEY-----\n")

        GitHubApp(GitHubAppConfig("1234", keyFile)).use { app ->
            assertTrue(workerCredentials(WorkerSettings(), app).isConfigured(Platform.GITHUB))
        }
        assertFalse(workerCredentials(WorkerSettings(), null).isConfigured(Platform.GITHUB))
        assertTrue(workerCredentials(WorkerSettings(githubToken = "ghp"), null).isConfigured(Platform.GITHUB))
    }
}
