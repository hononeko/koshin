package dev.hononeko.koshin.proxy

import dev.hononeko.koshin.adapters.nats.NatsConfig
import dev.hononeko.koshin.adapters.nats.NatsConnectionHealth
import dev.hononeko.koshin.adapters.nats.NatsJetStreamBootstrap
import dev.hononeko.koshin.adapters.nats.NatsJobQueuePublisher
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.model.TriggerReason
import dev.hononeko.koshin.core.ports.JobQueuePublisher
import dev.hononeko.koshin.core.ports.RepositoryDiscoveryPort
import dev.hononeko.koshin.core.usecase.IngestWebhookUseCase
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.install
import io.ktor.server.engine.EmbeddedServer
import io.ktor.server.engine.connector
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.netty.NettyApplicationEngine
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.request.header
import io.ktor.server.request.receiveChannel
import io.ktor.server.request.receiveText
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import io.ktor.utils.io.toByteArray
import io.nats.client.Nats
import kotlinx.coroutines.CancellationException
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory

private val logger = LoggerFactory.getLogger("dev.hononeko.koshin.proxy.Application")

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
    discoveryPort: RepositoryDiscoveryPort = StaticFileRepositoryDiscovery(config.sweepRepositoriesFile),
    readinessCheck: () -> Boolean = { true },
) {
    val json = Json { ignoreUnknownKeys = true }
    val ingestUseCase = IngestWebhookUseCase(
        RetryingJobQueuePublisher(
            delegate = publisher,
            baseDelay = config.publishRetryBaseDelay,
            maxTotalDelay = config.publishRetryBudget,
        ),
    )
    val eventFilter = WebhookEventFilter(config.botLogins, json)

    install(ContentNegotiation) {
        json(json)
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
            handleGitHubWebhook(call, config, eventFilter, ingestUseCase)
        }

        post("/webhooks/gitlab") {
            handleGitLabWebhook(call, config, eventFilter, ingestUseCase)
        }

        post("/api/v1/trigger") {
            handleTriggerRequest(call, config, discoveryPort, ingestUseCase, json)
        }
    }
}

private suspend fun handleGitHubWebhook(
    call: ApplicationCall,
    config: ProxyConfig,
    eventFilter: WebhookEventFilter,
    ingestUseCase: IngestWebhookUseCase,
) {
    if (!config.isPlatformEnabled(Platform.GITHUB)) {
        call.respond(HttpStatusCode.Forbidden, ErrorResponse("GitHub platform is not enabled"))
        return
    }

    val payloadBytes = call.receiveChannel().toByteArray()
    val signatureHeader = call.request.header("X-Hub-Signature-256")
    if (!SignatureVerifier.verifyGitHubSignature(payloadBytes, signatureHeader, config.githubWebhookSecret)) {
        logger.warn("Rejected GitHub webhook due to invalid or missing X-Hub-Signature-256")
        call.respond(HttpStatusCode.Forbidden, ErrorResponse("Invalid webhook signature"))
        return
    }

    val eventHeader = call.request.header("X-GitHub-Event")
    val deliveryIdHeader = call.request.header("X-GitHub-Delivery")
    val rawPayload = payloadBytes.toString(Charsets.UTF_8)

    processWebhookDecision(
        call = call,
        platform = Platform.GITHUB,
        deliveryId = deliveryIdHeader,
        decision = eventFilter.evaluateGitHub(eventHeader, rawPayload),
        ingestUseCase = ingestUseCase,
    )
}

private suspend fun handleGitLabWebhook(
    call: ApplicationCall,
    config: ProxyConfig,
    eventFilter: WebhookEventFilter,
    ingestUseCase: IngestWebhookUseCase,
) {
    if (!config.isPlatformEnabled(Platform.GITLAB)) {
        call.respond(HttpStatusCode.Forbidden, ErrorResponse("GitLab platform is not enabled"))
        return
    }

    val tokenHeader = call.request.header("X-Gitlab-Token")
    if (!SignatureVerifier.verifyGitLabToken(tokenHeader, config.gitlabWebhookSecret)) {
        logger.warn("Rejected GitLab webhook due to invalid or missing X-Gitlab-Token")
        call.respond(HttpStatusCode.Forbidden, ErrorResponse("Invalid webhook token"))
        return
    }

    val payloadBytes = call.receiveChannel().toByteArray()
    val eventHeader = call.request.header("X-Gitlab-Event")
    val deliveryIdHeader = call.request.header("X-Gitlab-Event-UUID")
    val rawPayload = payloadBytes.toString(Charsets.UTF_8)

    processWebhookDecision(
        call = call,
        platform = Platform.GITLAB,
        deliveryId = deliveryIdHeader,
        decision = eventFilter.evaluateGitLab(eventHeader, rawPayload),
        ingestUseCase = ingestUseCase,
    )
}

@Suppress("TooGenericExceptionCaught")
private suspend fun processWebhookDecision(
    call: ApplicationCall,
    platform: Platform,
    deliveryId: String?,
    decision: WebhookDecision,
    ingestUseCase: IngestWebhookUseCase,
) {
    when (decision) {
        is WebhookDecision.Malformed -> {
            call.respond(HttpStatusCode.BadRequest, ErrorResponse(decision.reason))
        }

        is WebhookDecision.Ignore -> {
            logger.debug("Filtered {} webhook (delivery={}): {}", platform, deliveryId, decision.reason)
            call.respond(
                HttpStatusCode.Accepted,
                WebhookResponse(status = "ignored", enqueued = false, reason = decision.reason),
            )
        }

        is WebhookDecision.Enqueue -> {
            try {
                val event = ingestUseCase.execute(
                    platform = platform,
                    repository = decision.repository,
                    deliveryId = deliveryId,
                    triggerReason = TriggerReason.WEBHOOK,
                )
                call.respond(
                    HttpStatusCode.Accepted,
                    WebhookResponse(status = "accepted", enqueued = true, deliveryId = event.deliveryId),
                )
            } catch (e: CancellationException) {
                throw e
            } catch (e: IllegalArgumentException) {
                logger.warn("Invalid {} repository format '{}': {}", platform, decision.repository, e.message)
                call.respond(HttpStatusCode.BadRequest, ErrorResponse(e.message ?: "Invalid repository"))
            } catch (e: Exception) {
                logger.error("Failed to publish {} webhook job for {}", platform, decision.repository, e)
                call.respond(HttpStatusCode.ServiceUnavailable, ErrorResponse("Queue unavailable"))
            }
        }
    }
}

private suspend fun handleTriggerRequest(
    call: ApplicationCall,
    config: ProxyConfig,
    discoveryPort: RepositoryDiscoveryPort,
    ingestUseCase: IngestWebhookUseCase,
    json: Json,
) {
    val authHeader = call.request.header("Authorization")
    if (!SignatureVerifier.verifyBearerToken(authHeader, config.internalTriggerToken)) {
        logger.warn("Rejected /api/v1/trigger request due to invalid or missing Bearer token")
        call.respond(HttpStatusCode.Unauthorized, ErrorResponse("Unauthorized"))
        return
    }

    val rawBody = call.receiveText()
    val targetRepos = resolveTriggerRepositories(call, rawBody, discoveryPort, json) ?: return

    enqueueScheduledRepositories(call, targetRepos, ingestUseCase)
}

private suspend fun resolveTriggerRepositories(
    call: ApplicationCall,
    rawBody: String,
    discoveryPort: RepositoryDiscoveryPort,
    json: Json,
): List<RepoRef>? = try {
    val triggerRequest = json.decodeFromString(TriggerRequest.serializer(), rawBody)
    buildList {
        for (item in triggerRequest.repositories) {
            val platform = StaticFileRepositoryDiscovery.parsePlatform(item.platform)
            add(RepoRef(platform, item.repository.trim()))
        }
        if (triggerRequest.discover) {
            addAll(discoveryPort.discover())
        }
    }
} catch (e: CancellationException) {
    throw e
} catch (e: SerializationException) {
    logger.warn("Malformed /api/v1/trigger JSON payload: {}", e.message)
    call.respond(HttpStatusCode.BadRequest, ErrorResponse("Malformed trigger request payload"))
    null
} catch (e: IllegalArgumentException) {
    logger.warn("Invalid repository or discovery configuration in /api/v1/trigger: {}", e.message)
    call.respond(HttpStatusCode.BadRequest, ErrorResponse(e.message ?: "Invalid trigger request"))
    null
}

@Suppress("TooGenericExceptionCaught")
private suspend fun enqueueScheduledRepositories(
    call: ApplicationCall,
    targetRepos: List<RepoRef>,
    ingestUseCase: IngestWebhookUseCase,
) {
    try {
        for (repoRef in targetRepos) {
            ingestUseCase.execute(
                platform = repoRef.platform,
                repository = repoRef.repository,
                deliveryId = null,
                triggerReason = TriggerReason.SCHEDULE,
            )
        }
        call.respond(
            HttpStatusCode.Accepted,
            TriggerResponse(status = "accepted", enqueued = targetRepos.size),
        )
    } catch (e: CancellationException) {
        throw e
    } catch (e: Exception) {
        logger.error("Failed to publish scheduled trigger jobs", e)
        call.respond(HttpStatusCode.ServiceUnavailable, ErrorResponse("Queue unavailable"))
    }
}

fun createProxyServer(
    config: ProxyConfig,
    publisher: JobQueuePublisher,
    discoveryPort: RepositoryDiscoveryPort = StaticFileRepositoryDiscovery(config.sweepRepositoriesFile),
    readinessCheck: () -> Boolean = { true },
): EmbeddedServer<NettyApplicationEngine, NettyApplicationEngine.Configuration> = embeddedServer(
    factory = Netty,
    configure = {
        enableHttp2 = config.http2Enabled
        enableH2c = config.http2Enabled
        connector {
            port = config.port
        }
    },
    module = {
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

    val natsConnection = Nats.connect(proxyConfig.natsUrl)
    Runtime.getRuntime().addShutdownHook(
        Thread {
            runCatching { natsConnection.close() }
        },
    )

    NatsJetStreamBootstrap(natsConnection, natsConfig).bootstrap(proxyConfig.platforms)
    val health = NatsConnectionHealth(natsConnection)
    val publisher = NatsJobQueuePublisher(natsConnection, natsConfig)

    createProxyServer(
        config = proxyConfig,
        publisher = publisher,
        readinessCheck = health::isConnected,
    ).start(wait = true)
}
