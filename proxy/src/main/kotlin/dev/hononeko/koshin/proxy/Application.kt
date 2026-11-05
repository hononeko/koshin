package dev.hononeko.koshin.proxy

import dev.hononeko.koshin.adapters.github.AccountInfo
import dev.hononeko.koshin.adapters.github.GitHubApp
import dev.hononeko.koshin.adapters.github.GitHubAppConfig
import dev.hononeko.koshin.adapters.nats.NatsConfig
import dev.hononeko.koshin.adapters.valkey.ValkeyConfig
import dev.hononeko.koshin.core.model.RepoTarget
import dev.hononeko.koshin.core.ports.InstallationPolicyPort
import dev.hononeko.koshin.core.ports.JobQueuePublisher
import dev.hononeko.koshin.core.ports.RepoCoordinationPort
import dev.hononeko.koshin.core.ports.RepositoryDiscoveryPort
import dev.hononeko.koshin.core.usecase.IngestObserver
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
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import io.ktor.utils.io.readBuffer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.io.readByteArray
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import org.slf4j.bridge.SLF4JBridgeHandler
import java.io.IOException

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

/** [enqueued] jobs published, [coalesced] repos already queued or running. */
@Serializable
data class TriggerResponse(val status: String, val enqueued: Int, val coalesced: Int = 0)

@Serializable
data class ErrorResponse(val error: String)

/** With a [coordination] port, webhooks and triggers are coalesced before publishing. */
@Suppress("LongParameterList") // All but the first two have defaults
fun Application.proxyModule(
    config: ProxyConfig,
    publisher: JobQueuePublisher,
    discoveryPort: RepositoryDiscoveryPort? = config.sweepRepositoriesFile?.let { StaticFileRepositoryDiscovery(it) },
    readinessCheck: () -> Boolean = { true },
    gitHubApp: ProxyGitHubApp = ProxyGitHubApp(),
    coordination: RepoCoordinationPort? = null,
    ingestObserver: IngestObserver = LoggingIngestObserver(),
    encryptionKey: EncryptionKey? = null,
) {
    val ingestUseCase = IngestWebhookUseCase(
        publisher = RetryingJobQueuePublisher(
            delegate = publisher,
            baseDelay = config.publishRetryBaseDelay,
            maxTotalDelay = config.publishRetryBudget,
        ),
        coordination = coordination,
        config = config.ingest,
        observer = ingestObserver,
    )
    val eventFilter = WebhookEventFilter(config.botLogins, ProxyJson, gitHubApp.botLogins)
    val routeHandler = ProxyRouteHandler(config, eventFilter, ingestUseCase, discoveryPort, gitHubApp)

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

        // Unauthenticated: it's a public key
        get("/api/v1/encryption-key") {
            respondEncryptionKey(call, encryptionKey)
        }
    }
}

private suspend fun respondEncryptionKey(call: ApplicationCall, key: EncryptionKey?) {
    if (key == null) {
        call.respond(HttpStatusCode.NotFound, ErrorResponse("No encryption key is configured"))
        return
    }
    val armored = try {
        key.armored()
    } catch (e: IOException) {
        logger.warn("Cannot read the encryption key", e)
        null
    }
    if (armored == null) {
        call.respond(HttpStatusCode.ServiceUnavailable, ErrorResponse("The encryption key is unavailable"))
    } else {
        call.respondText(armored, EncryptionKey.CONTENT_TYPE)
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

/**
 * What the proxy does as a GitHub App: ignore its bot's webhooks, forget a gone installation's tokens, and serve only
 * accounts the [policy] allows: [onDisallowedInstallation] applies `GITHUB_APP_DISALLOWED_ACTION` to a new
 * installation by another account, and [resolveTargets] drops such accounts' repositories from triggers. With
 * [usesInstallations], GitHub jobs run as the App's installations and are queued in their scopes:
 * [resolveTargets] fills in the installation of a triggered repository.
 */
class ProxyGitHubApp(
    val botLogins: () -> Collection<String> = { emptyList() },
    val onInstallationGone: (Long) -> Unit = {},
    val policy: InstallationPolicyPort? = null,
    val onDisallowedInstallation: suspend (installationId: Long, account: WebhookAccount) -> Unit = { _, _ -> },
    val resolveTargets: suspend (List<RepoTarget>) -> List<RepoTarget> = { it },
    val usesInstallations: Boolean = false,
)

@Suppress("LongParameterList") // The server's wiring: every parameter but the first two has a default
fun createProxyServer(
    config: ProxyConfig,
    publisher: JobQueuePublisher,
    discoveryPort: RepositoryDiscoveryPort? = config.sweepRepositoriesFile?.let { StaticFileRepositoryDiscovery(it) },
    readinessCheck: () -> Boolean = { true },
    onStop: () -> Unit = {},
    gitHubApp: ProxyGitHubApp = ProxyGitHubApp(),
    coordination: RepoCoordinationPort? = null,
    encryptionKey: EncryptionKey? = null,
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
            gitHubApp = gitHubApp,
            coordination = coordination,
            encryptionKey = encryptionKey,
        )
    },
)

fun main() {
    // Libraries logging through java.util.logging end up in the JSON log too
    SLF4JBridgeHandler.removeHandlersForRootLogger()
    SLF4JBridgeHandler.install()
    checkLogLevels()
    val proxyConfig = ProxyConfig.fromEnv()
    val natsConfig = NatsConfig.fromEnv()
    // Checked before anything starts: a key that can't be read is a configuration error
    val encryptionKey = proxyConfig.publicKeyPath?.let(EncryptionKey::load)

    logger.info(
        "Starting koshin-proxy on port {} (http2Enabled={}, platforms={})",
        proxyConfig.port,
        proxyConfig.http2Enabled,
        proxyConfig.platforms,
    )

    // Readiness doesn't wait for Valkey: until it's connected, webhooks are published without coalescing
    val valkeyConfig = ValkeyConfig.fromEnv()
    val valkey = ProxyValkey { ProxyValkey.connect(valkeyConfig) }
    valkey.startAsync()

    val natsLifecycle = ProxyNatsLifecycle(
        natsUrl = proxyConfig.natsUrl,
        natsConfig = natsConfig,
        onDeadLettered = { event -> valkey.clearQueuedLater(event.repo) },
    )
    natsLifecycle.startAsync()

    val appScope = CoroutineScope(SupervisorJob())
    val dispatcher = workerDispatcher(proxyConfig, valkey, natsLifecycle)
    dispatcher?.start(appScope)

    val gitHubApp = GitHubAppConfig.fromEnv()?.let(::GitHubApp)
    gitHubApp?.let { runBlocking { it.allowList.resolveLogins() } }
    val botLogin = gitHubApp?.let { app -> AppBotLogin(app::botLogin).also { it.start(appScope) } }
    val discovery = CompositeRepositoryDiscovery.of(
        listOfNotNull(gitHubApp?.discovery(), proxyConfig.sweepRepositoriesFile?.let(::StaticFileRepositoryDiscovery)),
    )

    createProxyServer(
        config = proxyConfig,
        publisher = dispatcher?.let { WakingPublisher(natsLifecycle, it::wake) } ?: natsLifecycle,
        discoveryPort = discovery,
        readinessCheck = natsLifecycle::isReady,
        onStop = {
            runBlocking { dispatcher?.stop() }
            appScope.cancel()
            gitHubApp?.close()
            natsLifecycle.close()
            valkey.close()
        },
        gitHubApp = ProxyGitHubApp(
            botLogins = { listOfNotNull(botLogin?.get()) },
            onInstallationGone = { gitHubApp?.forgetInstallation(it) },
            // Only with an allow-list: unset, every account is served
            policy = gitHubApp?.allowList?.takeUnless { it.allowsAll },
            onDisallowedInstallation = { id, account ->
                gitHubApp?.applyDisallowedAction(id, AccountInfo(account.id, account.login))
            },
            resolveTargets = { targets -> gitHubApp?.resolve(targets) ?: targets },
            usesInstallations = gitHubApp != null,
        ),
        coordination = valkey.coordination,
        encryptionKey = encryptionKey,
    ).start(wait = true)
}

/** The burst-worker dispatcher when `WORKER_DISPATCHER_ENABLED`, talking to this pod's Kubernetes API. */
private fun workerDispatcher(config: ProxyConfig, valkey: ProxyValkey, nats: ProxyNatsLifecycle): WorkerDispatcher? {
    val dispatch = config.dispatcher.takeIf { it.enabled } ?: return null
    val burstWorkers = KubernetesBurstWorkers(
        api = KubernetesApi.inCluster(),
        namespace = dispatch.namespace ?: KubernetesApi.inClusterNamespace(),
        podTemplateName = dispatch.podTemplateName,
        activeDeadline = dispatch.jobActiveDeadline,
        ttlAfterFinished = dispatch.jobTtlAfterFinished,
    )
    return WorkerDispatcher(dispatch, config.platforms, valkey::dispatchPorts, nats.queueDepth, burstWorkers)
}
