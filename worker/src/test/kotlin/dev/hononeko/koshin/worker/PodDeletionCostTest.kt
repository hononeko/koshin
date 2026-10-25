package dev.hononeko.koshin.worker

import com.sun.net.httpserver.HttpServer
import dev.hononeko.koshin.core.model.JobExecutionResult
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.PlatformCredentials
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.ports.CliExecutorPort
import io.mockk.coEvery
import io.mockk.coVerifyOrder
import io.mockk.mockk
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import java.io.IOException
import java.net.InetSocketAddress
import java.net.URI
import java.net.http.HttpClient
import java.nio.file.Path
import kotlin.io.path.writeText
import kotlin.time.Duration.Companion.minutes

class PodDeletionCostTest {

    @TempDir
    lateinit var dir: Path

    private val repo = RepoRef(Platform.GITHUB, "hononeko/koshin")
    private val creds = PlatformCredentials("token")

    private data class Captured(
        val method: String,
        val path: String,
        val auth: String?,
        val type: String?,
        val body: String,
    )

    private fun <T> withApiServer(status: Int, block: (URI, MutableList<Captured>) -> T): T {
        val captured = mutableListOf<Captured>()
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server.createContext("/") { exchange ->
            captured += Captured(
                method = exchange.requestMethod,
                path = exchange.requestURI.rawPath,
                auth = exchange.requestHeaders.getFirst("Authorization"),
                type = exchange.requestHeaders.getFirst("Content-Type"),
                body = exchange.requestBody.readAllBytes().decodeToString(),
            )
            exchange.sendResponseHeaders(status, -1)
            exchange.close()
        }
        server.start()
        return try {
            block(URI("http://127.0.0.1:${server.address.port}"), captured)
        } finally {
            server.stop(0)
        }
    }

    private fun updater(apiServer: URI): KubernetesPodDeletionCostUpdater {
        val token = dir.resolve("token").apply { writeText("sa-token\n") }
        return KubernetesPodDeletionCostUpdater("worker-0", "koshin", apiServer, token, HttpClient.newHttpClient())
    }

    @Test
    fun `patches the pod annotation with a merge patch`() = withApiServer(200) { uri, captured ->
        runTest { updater(uri).update(1000) }

        val request = captured.single()
        assertEquals("PATCH", request.method)
        assertEquals("/api/v1/namespaces/koshin/pods/worker-0", request.path)
        assertEquals("Bearer sa-token", request.auth)
        assertEquals("application/merge-patch+json", request.type)
        assertEquals(
            """{"metadata":{"annotations":{"controller.kubernetes.io/pod-deletion-cost":"1000"}}}""",
            request.body,
        )
    }

    @Test
    fun `non-2xx responses throw`() = withApiServer(403) { uri, _ ->
        runTest { assertThrows<IOException> { updater(uri).update(0) } }
        Unit
    }

    @Test
    fun `executor marks the pod busy during the run and idle afterwards`() = runTest {
        val costs = mutableListOf<Int>()
        val delegate = mockk<CliExecutorPort>()
        coEvery { delegate.execute(repo, creds, 1.minutes) } returns JobExecutionResult.Success

        val result = PodDeletionCostCliExecutor(delegate, { costs += it }).execute(repo, creds, 1.minutes)

        assertEquals(JobExecutionResult.Success, result)
        assertEquals(listOf(1000, 0), costs)
    }

    @Test
    fun `executor fails soft when patching fails and resets even if the run throws`() = runTest {
        val updater = mockk<PodDeletionCostUpdater>()
        coEvery { updater.update(any()) } throws IOException("forbidden")
        val delegate = mockk<CliExecutorPort>()
        coEvery { delegate.execute(any(), any(), any()) } returns JobExecutionResult.Failed(1) andThenThrows
            IllegalStateException("boom")
        val executor = PodDeletionCostCliExecutor(delegate, updater)

        assertEquals(JobExecutionResult.Failed(1), executor.execute(repo, creds, 1.minutes))
        assertThrows<IllegalStateException> { executor.execute(repo, creds, 1.minutes) }
        coVerifyOrder {
            updater.update(1000)
            updater.update(0)
            updater.update(1000)
            updater.update(0)
        }
    }
}
