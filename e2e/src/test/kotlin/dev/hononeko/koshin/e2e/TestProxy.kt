package dev.hononeko.koshin.e2e

import dev.hononeko.koshin.adapters.nats.NatsConfig
import dev.hononeko.koshin.adapters.valkey.ValkeyConfig
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.proxy.ProxyConfig
import dev.hononeko.koshin.proxy.ProxyNatsLifecycle
import dev.hononeko.koshin.proxy.ProxyValkey
import dev.hononeko.koshin.proxy.createProxyServer
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonObject
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.HexFormat
import java.util.UUID
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

/**
 * The real proxy (Ktor, NATS, Valkey), receiving signed GitHub webhooks over HTTP. Without [valkeyConfig] it
 * publishes every webhook, as when it fails open.
 */
class TestProxy(natsUrl: String, natsConfig: NatsConfig, valkeyConfig: ValkeyConfig?) : AutoCloseable {
    private val valkey = valkeyConfig?.let { config -> ProxyValkey { ProxyValkey.connect(config) } }
    private val nats = ProxyNatsLifecycle(
        natsUrl = natsUrl,
        natsConfig = natsConfig,
        onDeadLettered = { event -> valkey?.clearQueuedLater(event.repo) },
    )
    private val server = createProxyServer(
        config = ProxyConfig(port = 0, natsUrl = natsUrl, githubWebhookSecret = SECRET),
        publisher = nats,
        readinessCheck = nats::isReady,
        onStop = {
            nats.close()
            valkey?.close()
        },
        coordination = valkey?.coordination,
    )
    private val http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()
    private var port = 0

    fun start() {
        valkey?.startAsync()
        nats.startAsync()
        server.start(wait = false)
        port = runBlocking { server.engine.resolvedConnectors().first().port }
        awaitUntil("proxy to connect to NATS") { nats.isReady() }
        valkey?.let { awaitUntil("proxy to connect to Valkey") { it.isConnected } }
    }

    /** Sends a signed `push` webhook and returns the proxy's JSON response. */
    fun push(
        repo: RepoRef,
        ref: String = "refs/heads/main",
        sender: String = "octocat",
        senderType: String = "User",
    ): JsonObject {
        val payload = buildJsonObject {
            put("ref", ref)
            put("after", "1".repeat(SHA_LENGTH))
            put("deleted", false)
            putJsonObject("repository") {
                put("full_name", repo.repository)
                put("default_branch", "main")
            }
            putJsonObject("sender") {
                put("login", sender)
                put("type", senderType)
            }
            putJsonObject("pusher") { put("name", sender) }
        }.toString().toByteArray()

        val request = HttpRequest.newBuilder(URI("http://localhost:$port/webhooks/github"))
            .header("Content-Type", "application/json")
            .header("X-GitHub-Event", "push")
            .header("X-GitHub-Delivery", UUID.randomUUID().toString())
            .header("X-Hub-Signature-256", "sha256=${sign(payload)}")
            .POST(HttpRequest.BodyPublishers.ofByteArray(payload))
            .build()
        val response = http.send(request, HttpResponse.BodyHandlers.ofString())
        check(response.statusCode() == ACCEPTED) { "Webhook failed with ${response.statusCode()}: ${response.body()}" }
        return Json.parseToJsonElement(response.body()).jsonObject
    }

    override fun close() {
        server.stop(GRACE_MILLIS, TIMEOUT_MILLIS)
        http.close()
    }

    private fun sign(payload: ByteArray): String {
        val mac = Mac.getInstance("HmacSHA256")
        mac.init(SecretKeySpec(SECRET.toByteArray(), "HmacSHA256"))
        return HexFormat.of().formatHex(mac.doFinal(payload))
    }

    private companion object {
        const val SECRET = "e2e-webhook-secret"
        const val SHA_LENGTH = 40
        const val ACCEPTED = 202
        const val GRACE_MILLIS = 100L
        const val TIMEOUT_MILLIS = 1000L
    }
}
