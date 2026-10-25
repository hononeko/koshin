package dev.hononeko.koshin.worker

import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpStatusCode
import io.ktor.server.testing.testApplication
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.IOException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import kotlin.time.Duration.Companion.milliseconds

class WorkerApplicationTest {

    @Test
    fun `healthz is 200 and readyz is 503 while NATS and Valkey are unreachable`() {
        val backends = WorkerBackends(
            connectNats = { throw IOException("nats down") },
            connectValkey = { throw IOException("valkey down") },
            retryDelay = 10.milliseconds,
        )
        backends.startAsync()
        try {
            testApplication {
                application { workerModule(backends::isReady) }

                val health = client.get("/healthz")
                assertEquals(HttpStatusCode.OK, health.status)
                assertTrue(health.bodyAsText().contains("\"ok\""))
                assertEquals(HttpStatusCode.ServiceUnavailable, client.get("/readyz").status)
            }
        } finally {
            backends.close()
        }
    }

    @Test
    fun `readyz is 200 when the readiness check passes`() = testApplication {
        application { workerModule { true } }
        assertEquals(HttpStatusCode.OK, client.get("/readyz").status)
    }

    @Test
    fun `embedded Netty server serves healthz over h2c`() = runBlocking {
        val server = createWorkerServer(WorkerSettings(port = 0)).start(wait = false)
        try {
            val port = server.engine.resolvedConnectors().first().port
            val response = HttpClient.newBuilder().version(HttpClient.Version.HTTP_2).build().send(
                HttpRequest.newBuilder(URI("http://127.0.0.1:$port/healthz")).build(),
                HttpResponse.BodyHandlers.ofString(),
            )
            assertEquals(200, response.statusCode())
            assertEquals(HttpClient.Version.HTTP_2, response.version())
        } finally {
            server.stop(100, 500)
        }
    }
}
