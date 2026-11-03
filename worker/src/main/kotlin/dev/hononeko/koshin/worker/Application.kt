package dev.hononeko.koshin.worker

import dev.hononeko.koshin.adapters.github.GitHubApp
import dev.hononeko.koshin.adapters.github.GitHubAppConfig
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
import org.slf4j.bridge.SLF4JBridgeHandler
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread
import kotlin.system.exitProcess

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

/**
 * Serves the health endpoints and runs the daemon [newDaemon] makes, keeping [presence]'s lease, until SIGTERM or until
 * the daemon finishes on its own (drain-then-exit or max lifetime, KSH-31). Returns true in the second case, for the
 * process to exit 0. Either way the server's stop hooks stop the daemon and then run [onStopped].
 */
internal fun serveWorker(
    settings: WorkerSettings,
    readinessCheck: suspend () -> Boolean,
    presence: WarmPresence?,
    onStopped: () -> Unit,
    newDaemon: (onFinished: () -> Unit) -> WorkerDaemon,
): Boolean {
    val finished = AtomicBoolean(false)
    val stopped = CountDownLatch(1)
    lateinit var server: EmbeddedServer<NettyApplicationEngine, NettyApplicationEngine.Configuration>
    val daemon = newDaemon {
        finished.set(true)
        // Off the daemon's coroutine, which stopping the server cancels and joins
        thread(name = "koshin-worker-exit") { server.stop(EXIT_GRACE_MILLIS, EXIT_TIMEOUT_MILLIS) }
    }
    server = createWorkerServer(
        settings = settings,
        readinessCheck = readinessCheck,
        onStopping = {
            presence?.stop()
            daemon.stop()
        },
        onStopped = {
            try {
                onStopped()
            } finally {
                stopped.countDown()
            }
        },
    )
    server.start(wait = false)
    presence?.start()
    daemon.start()
    stopped.await()
    return finished.get()
}

private const val EXIT_GRACE_MILLIS = 500L
private const val EXIT_TIMEOUT_MILLIS = 5_000L

internal fun cliExecutor(settings: WorkerSettings): CliExecutorPort = RenovateProcessExecutor(
    baseDir = settings.renovateBaseDir,
    binarySource = settings.renovateBinarySource,
    redisUrl = settings.renovateRedisUrl,
    requestLimits = settings.requestLimits,
    rateLimitPattern = Regex(settings.rateLimitLogPattern),
    repoCache = settings.repoCache,
    cacheDir = settings.renovateCacheDir,
    launcher = settings.runnerSocket?.let { RunnerRenovateLauncher(it) } ?: LocalRenovateLauncher(),
)

/** The GitHub App for GitHub runs when it's configured, the platform tokens otherwise. */
internal fun workerCredentials(settings: WorkerSettings, gitHubApp: GitHubApp?): WorkerCredentials {
    val credentials = WorkerCredentials(
        StaticCredentialsProvider(settings),
        gitHubApp?.credentialsProvider(settings.jobConfig.cliTimeout),
    )
    if (gitHubApp != null) {
        logger.info("GitHub runs authenticate as the GitHub App")
    }
    settings.platforms.filterNot(credentials::isConfigured).forEach { platform ->
        logger.warn("No token configured for {}; its jobs will fail until one is set", platform)
    }
    return credentials
}

fun main() {
    // Libraries logging through java.util.logging end up in the JSON log too
    SLF4JBridgeHandler.removeHandlersForRootLogger()
    SLF4JBridgeHandler.install()
    checkLogLevels()
    val settings = WorkerSettings.fromEnv()
    val natsConfig = NatsConfig.fromEnv()
    val valkeyConfig = ValkeyConfig.fromEnv()
    logger.info("Starting koshin-worker with {}", settings)

    val gitHubApp = GitHubAppConfig.fromEnv()?.let(::GitHubApp)
    gitHubApp?.let { runBlocking { it.allowList.resolveLogins() } }
    val credentials = workerCredentials(settings, gitHubApp)

    val workspaceCleaner = WorkspaceCleaner(settings.renovateBaseDir)
    runBlocking { workspaceCleaner.purgeAll() }

    val cliExecutor = cliExecutor(settings)
    if (settings.runnerSocket == null) {
        logger.warn("No RUNNER_SOCKET: Renovate must be installed in this container, and can read its secrets")
    }

    val backends = WorkerBackends(
        // Consumers for every platform, so KEDA's per-platform triggers never hit a missing consumer.
        connectNats = {
            WorkerBackends.connectNats(
                url = settings.natsUrl,
                config = natsConfig,
                platforms = Platform.entries,
                repoCache = settings.repoCache.natsConfig(natsConfig),
                cacheDir = settings.renovateCacheDir,
            )
        },
        connectValkey = { WorkerBackends.connectValkey(valkeyConfig, settings.throttle) },
    )
    backends.startAsync()

    val finished = serveWorker(
        settings = settings,
        readinessCheck = backends::isReady,
        presence = warmPresence(settings, backends),
        onStopped = {
            backends.close()
            gitHubApp?.close()
        },
    ) { onFinished ->
        WorkerDaemon(
            platforms = settings.platforms,
            idleTimeout = settings.idleTimeout,
            maxLifetime = settings.maxLifetime,
            onFinished = onFinished,
        ) {
            jobUseCase(settings, backends.awaitPorts(), credentials, cliExecutor, workspaceCleaner)
        }
    }
    if (finished) {
        logger.info("Worker finished; exiting")
        exitProcess(0)
    }
}

/** A daemon-mode worker tells the Job dispatcher it's ready (KSH-31); a burst Job pod drains and exits instead. */
internal fun warmPresence(settings: WorkerSettings, backends: WorkerBackends): WarmPresence? =
    if (settings.idleTimeout.isPositive()) {
        null
    } else {
        WarmPresence(settings.jobConfig.heartbeatInterval, settings.jobConfig.lockTtl) {
            backends.awaitPorts().warmWorkers
        }
    }

internal fun jobUseCase(
    settings: WorkerSettings,
    ports: WorkerBackends.Ports,
    credentials: WorkerCredentials,
    cliExecutor: CliExecutorPort,
    workspaceCleaner: WorkspaceCleaner,
) = ProcessRenovateJobUseCase(
    queueConsumer = ports.consumer,
    queuePublisher = ports.publisher,
    coordinationPort = ports.coordination,
    throttlePort = ports.throttle,
    cachePort = ports.repoCache,
    credentialsPort = credentials,
    cliExecutor = cliExecutor,
    workspaceCleaner = workspaceCleaner,
    config = settings.jobConfig,
    jobContext = MdcJobContext,
)
