package dev.hononeko.koshin.proxy

import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.ports.JobQueuePublisher
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.net.Socket
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

class ProxyHttp2Test {

    private val noOpPublisher = object : JobQueuePublisher {
        override suspend fun publish(event: RenovateJobEvent) = Unit
        override suspend fun publishToDeadLetter(event: RenovateJobEvent) = Unit
    }

    @Test
    fun `embedded Netty server negotiates h2c via HTTP_2 client and HTTP 1_1 Upgrade`() = runBlocking {
        val config = ProxyConfig(port = 0, http2Enabled = true)
        val server = createProxyServer(
            config = config,
            publisher = noOpPublisher,
            readinessCheck = { true },
        ).start(wait = false)

        try {
            val connector = server.engine.resolvedConnectors().first()
            val port = connector.port

            val client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .build()

            val request = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:$port/healthz"))
                .GET()
                .build()

            // First request sends HTTP/1.1 with Upgrade: h2c and negotiates HTTP_2
            val firstResponse = client.send(request, HttpResponse.BodyHandlers.ofString())
            assertEquals(200, firstResponse.statusCode())
            assertEquals(HttpClient.Version.HTTP_2, firstResponse.version())
            assertTrue(firstResponse.body().contains("\"status\":\"ok\""))

            // Subsequent request reuses the HTTP/2 connection / prior knowledge cache
            val secondResponse = client.send(request, HttpResponse.BodyHandlers.ofString())
            assertEquals(200, secondResponse.statusCode())
            assertEquals(HttpClient.Version.HTTP_2, secondResponse.version())
            assertTrue(secondResponse.body().contains("\"status\":\"ok\""))

            // Also verify raw HTTP/1.1 Upgrade: h2c returns 101 Switching Protocols followed by HTTP/2 frame
            Socket("127.0.0.1", port).use { socket ->
                socket.soTimeout = 5000
                val upgradeRequest = buildString {
                    append("GET /healthz HTTP/1.1\r\n")
                    append("Host: 127.0.0.1:$port\r\n")
                    append("Connection: Upgrade, HTTP2-Settings\r\n")
                    append("Upgrade: h2c\r\n")
                    append("HTTP2-Settings: AAMAAABkAAQCAAAAAAIAAAAA\r\n")
                    append("\r\n")
                }
                socket.getOutputStream().write(upgradeRequest.toByteArray(Charsets.US_ASCII))
                socket.getOutputStream().flush()

                val buffer = ByteArray(256)
                val read = socket.getInputStream().read(buffer)
                assertTrue(read > 0)
                val statusLine = String(buffer, 0, read, Charsets.ISO_8859_1).lineSequence().first()
                assertEquals("HTTP/1.1 101 Switching Protocols", statusLine)
            }
        } finally {
            server.stop(100, 500)
        }
    }
}
