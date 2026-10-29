package dev.hononeko.koshin.e2e

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.PlatformCredentials
import dev.hononeko.koshin.core.usecase.ProcessRenovateJobUseCase
import dev.hononeko.koshin.worker.NoOpRepoCache
import dev.hononeko.koshin.worker.StaticCredentialsProvider
import dev.hononeko.koshin.worker.UnthrottledPlatformThrottle
import dev.hononeko.koshin.worker.WorkerBackends
import dev.hononeko.koshin.worker.WorkerDaemon
import dev.hononeko.koshin.worker.WorkerSettings
import dev.hononeko.koshin.worker.WorkspaceCleaner
import dev.hononeko.koshin.worker.createWorkerServer
import kotlinx.coroutines.runBlocking
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.createTempDirectory
import kotlin.io.path.deleteRecursively
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

/** A worker wired like `worker/Application.kt`, with a fake CLI and recording decorators around its ports. */
class TestWorker(val name: String, private val cluster: E2eCluster, runTime: Duration) : AutoCloseable {
    val cli = FakeCli(name, cluster.runs, runTime)
    val stall = Stall()

    private val baseDir = createTempDirectory("koshin-e2e-$name-")
    private val nats = WorkerBackends.connectNats(Containers.natsUrl, cluster.natsConfig, Platform.entries)
    private val valkey = runBlocking { WorkerBackends.connectValkey(cluster.valkeyConfig) }
    private val backends = WorkerBackends(connectNats = { nats }, connectValkey = { valkey })

    val consumer = RecordingConsumer(nats.consumer, stall)
    val coordination = RecordingCoordination(valkey.coordination, stall)

    private val useCase = ProcessRenovateJobUseCase(
        queueConsumer = consumer,
        queuePublisher = nats.publisher,
        coordinationPort = coordination,
        throttlePort = UnthrottledPlatformThrottle,
        cachePort = NoOpRepoCache,
        credentialsPort = StaticCredentialsProvider(mapOf(Platform.GITHUB to PlatformCredentials("e2e-token"))),
        cliExecutor = cli,
        workspaceCleaner = WorkspaceCleaner(baseDir),
        config = cluster.jobConfig,
    )
    private val daemon = WorkerDaemon(
        platforms = setOf(Platform.GITHUB),
        idleDelay = 100.milliseconds,
        errorDelay = 500.milliseconds,
    ) { useCase }
    private val server = createWorkerServer(
        settings = WorkerSettings(port = 0, renovateBaseDir = baseDir, jobConfig = cluster.jobConfig),
        readinessCheck = backends::isReady,
        onStopping = { daemon.stop() },
        onStopped = backends::close,
    )
    private val stopped = AtomicBoolean(false)

    val deliveries: List<DeliveryRecord>
        get() = consumer.deliveries

    fun runs(): List<CliRun> = cluster.runs.filter { it.worker == name }

    fun start() {
        backends.startAsync()
        server.start(wait = false)
        daemon.start()
    }

    /** Graceful shutdown, the path Ktor's shutdown hook takes on SIGTERM. */
    fun sigterm() {
        if (stopped.compareAndSet(false, true)) {
            server.stop(GRACE_MILLIS, TIMEOUT_MILLIS)
        }
    }

    /**
     * Process death: the NATS and Valkey connections drop first, so neither a NAK nor a lock release from the
     * in-flight job's cleanup can reach them.
     */
    fun crash() {
        backends.close()
        sigterm()
    }

    @OptIn(ExperimentalPathApi::class)
    override fun close() {
        stall.end()
        cli.release()
        sigterm()
        runCatching { baseDir.deleteRecursively() }
    }

    private companion object {
        const val GRACE_MILLIS = 100L
        const val TIMEOUT_MILLIS = 1000L
    }
}
