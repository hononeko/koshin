package dev.hononeko.koshin.proxy

import dev.hononeko.koshin.adapters.nats.NatsConfig
import dev.hononeko.koshin.core.ports.JobQueuePublisher
import dev.hononeko.koshin.core.ports.RepositoryDiscoveryPort
import dev.hononeko.koshin.core.usecase.IngestWebhookUseCase
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.ApplicationStopped
import io.ktor.server.application.install
import io.ktor.server.engine.EmbeddedServer
import io.ktor.server.engine.connector
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.netty.NettyApplicationEngine
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.request.receiveChannel
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import io.ktor.utils.io.readBuffer
import kotlinx.io.readByteArray
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory

private val logger = LoggerFactory.getLogger("dev.hononeko.koshin.proxy.Application")

val ProxyJson: Json = Json {
    ignoreUnknownKeys = true
    encodeDefaults = true
}

@Serializable
data class HealthResponse(val status: String)

@Serializable
data class WebhookResponse(
    val status: String,
    val enqueued: Boolean = false,
    val deliveryId: String? = null,
    val reason: String? = null,
)

@Serializable
data class TriggerRepositoryItem(val platform: String = "github", val repository: String)

@Serializable
data class TriggerRequest(val repositories: List<TriggerRepositoryItem> = emptyList(), val discover: Boolean = false)

@Serializable
data class TriggerResponse(val status: String, val enqueued: Int)

@Serializable
data class ErrorResponse(val error: String)

fun Application.proxyModule(
    config: ProxyConfig,
    publisher: JobQueuePublisher,
    discoveryPort: RepositoryDiscoveryPort? = config.sweepRepositoriesFile?.let { StaticFileRepositoryDiscovery(it) },
    readinessCheck: () -> Boolean = { true },
) {
    val ingestUseCase = IngestWebhookUseCase(
        RetryingJobQueuePublisher(
            delegate = publisher,
            baseDelay = config.publishRetryBaseDelay,
            maxTotalDelay = config.publishRetryBudget,
        ),
    )
    val eventFilter = WebhookEventFilter(config.botLogins, ProxyJson)
    val routeHandler = ProxyRouteHandler(config, eventFilter, ingestUseCase, discoveryPort)

    install(ContentNegotiation) {
        json(ProxyJson)
    }

    routing {
        get("/healthz") {
            call.respond(HttpStatusCode.OK, HealthResponse(status = "ok"))
        }

        get("/readyz") {
            if (readinessCheck()) {
                call.respond(HttpStatusCode.OK, HealthResponse(status = "ready"))
            } else {
                call.respond(HttpStatusCode.ServiceUnavailable, HealthResponse(status = "not_ready"))
            }
        }

        post("/webhooks/github") {
            routeHandler.handleGitHubWebhook(call)
        }

        post("/webhooks/gitlab") {
            routeHandler.handleGitLabWebhook(call)
        }

        post("/api/v1/trigger") {
            routeHandler.handleTriggerRequest(call)
        }
    }
}

suspend fun ApplicationCall.receiveBoundedBytes(maxBytes: Int): ByteArray? {
    val declaredLength = request.headers[HttpHeaders.ContentLength]?.toLongOrNull()
    return if (declaredLength != null && declaredLength > maxBytes) {
        respond(HttpStatusCode.PayloadTooLarge, ErrorResponse("Request body exceeds maximum size of $maxBytes bytes"))
        null
    } else {
        val packet = receiveChannel().readBuffer((maxBytes + 1).toLong())
        val bytes = packet.readByteArray()
        if (bytes.size > maxBytes) {
            respond(
                HttpStatusCode.PayloadTooLarge,
                ErrorResponse("Request body exceeds maximum size of $maxBytes bytes"),
            )
            null
        } else {
            bytes
        }
    }
}

fun createProxyServer(
    config: ProxyConfig,
    publisher: JobQueuePublisher,
    discoveryPort: RepositoryDiscoveryPort? = config.sweepRepositoriesFile?.let { StaticFileRepositoryDiscovery(it) },
    readinessCheck: () -> Boolean = { true },
    onStop: () -> Unit = {},
): EmbeddedServer<NettyApplicationEngine, NettyApplicationEngine.Configuration> = embeddedServer(
    factory = Netty,
    configure = {
        enableHttp2 = config.http2Enabled
        enableH2c = config.http2Enabled
        requestReadTimeoutSeconds = config.requestReadTimeoutSeconds
        connector {
            port = config.port
        }
    },
    module = {
        monitor.subscribe(ApplicationStopped) {
            onStop()
        }
        proxyModule(
            config = config,
            publisher = publisher,
            discoveryPort = discoveryPort,
            readinessCheck = readinessCheck,
        )
    },
)

fun main() {
    val proxyConfig = ProxyConfig.fromEnv()
    val natsConfig = NatsConfig.fromEnv()

    logger.info(
        "Starting koshin-proxy on port {} (http2Enabled={}, platforms={})",
        proxyConfig.port,
        proxyConfig.http2Enabled,
        proxyConfig.platforms,
    )

    val natsLifecycle = ProxyNatsLifecycle(proxyConfig.natsUrl, natsConfig)
    natsLifecycle.startAsync()

    createProxyServer(
        config = proxyConfig,
        publisher = natsLifecycle,
        readinessCheck = natsLifecycle::isReady,
        onStop = natsLifecycle::close,
    ).start(wait = true)
}
