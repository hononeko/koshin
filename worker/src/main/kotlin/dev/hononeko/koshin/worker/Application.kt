package dev.hononeko.koshin.worker

import dev.hononeko.koshin.adapters.nats.NatsConfig
import dev.hononeko.koshin.adapters.valkey.ValkeyConfig
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.ports.CliExecutorPort
import dev.hononeko.koshin.core.usecase.ProcessRenovateJobUseCase
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationStopped
import io.ktor.server.application.ApplicationStopping
import io.ktor.server.application.install
import io.ktor.server.engine.EmbeddedServer
import io.ktor.server.engine.connector
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.netty.NettyApplicationEngine
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import org.slf4j.LoggerFactory

private val logger = LoggerFactory.getLogger("dev.hononeko.koshin.worker.Application")

@Serializable
data class HealthResponse(val status: String)

fun Application.workerModule(readinessCheck: suspend () -> Boolean = { true }) {
    install(ContentNegotiation) {
        json()
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
    }
}

/**
 * On SIGTERM, Ktor's shutdown hook stops the server, raising [ApplicationStopping] (stop the daemon while NATS and
 * Valkey are still connected) and then [ApplicationStopped] (close them).
 */
fun createWorkerServer(
    settings: WorkerSettings,
    readinessCheck: suspend () -> Boolean = { true },
    onStopping: () -> Unit = {},
    onStopped: () -> Unit = {},
): EmbeddedServer<NettyApplicationEngine, NettyApplicationEngine.Configuration> = embeddedServer(
    factory = Netty,
    configure = {
        enableHttp2 = settings.http2Enabled
        enableH2c = settings.http2Enabled
        connector {
            port = settings.port
        }
    },
    module = {
        monitor.subscribe(ApplicationStopping) { onStopping() }
        monitor.subscribe(ApplicationStopped) { onStopped() }
        workerModule(readinessCheck)
    },
)

fun main() {
    val settings = WorkerSettings.fromEnv()
    val natsConfig = NatsConfig.fromEnv()
    val valkeyConfig = ValkeyConfig.fromEnv()
    logger.info("Starting koshin-worker with {}", settings)

    val credentials = StaticCredentialsProvider(settings)
    settings.platforms.filterNot(credentials::isConfigured).forEach { platform ->
        logger.warn("No token configured for {}; its jobs will fail until one is set", platform)
    }

    val workspaceCleaner = WorkspaceCleaner(settings.renovateBaseDir)
    runBlocking { workspaceCleaner.purgeAll() }

    val processExecutor = RenovateProcessExecutor(
        baseDir = settings.renovateBaseDir,
        binarySource = settings.renovateBinarySource,
        redisUrl = settings.renovateRedisUrl,
        requestLimits = settings.requestLimits,
        rateLimitPattern = Regex(settings.rateLimitLogPattern),
    )
    val cliExecutor: CliExecutorPort = if (settings.podDeletionCostEnabled) {
        PodDeletionCostCliExecutor(
            processExecutor,
            KubernetesPodDeletionCostUpdater(checkNotNull(settings.podName), checkNotNull(settings.podNamespace)),
        )
    } else {
        processExecutor
    }

    val backends = WorkerBackends(
        // Consumers for every platform, so KEDA's per-platform triggers never hit a missing consumer.
        connectNats = { WorkerBackends.connectNats(settings.natsUrl, natsConfig, Platform.entries) },
        connectValkey = { WorkerBackends.connectValkey(valkeyConfig, settings.throttle) },
    )
    backends.startAsync()

    val daemon = WorkerDaemon(settings.platforms) {
        val ports = backends.awaitPorts()
        ProcessRenovateJobUseCase(
            queueConsumer = ports.consumer,
            queuePublisher = ports.publisher,
            coordinationPort = ports.coordination,
            throttlePort = ports.throttle,
            cachePort = NoOpRepoCache,
            credentialsPort = credentials,
            cliExecutor = cliExecutor,
            workspaceCleaner = workspaceCleaner,
            config = settings.jobConfig,
        )
    }
    daemon.start()

    createWorkerServer(
        settings = settings,
        readinessCheck = backends::isReady,
        onStopping = { daemon.stop() },
        onStopped = backends::close,
    ).start(wait = true)
}
