package dev.hononeko.koshin.e2e

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.PlatformCredentials
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.usecase.ProcessRenovateJobUseCase
import dev.hononeko.koshin.worker.StaticCredentialsProvider
import dev.hononeko.koshin.worker.WarmPresence
import dev.hononeko.koshin.worker.WorkerBackends
import dev.hononeko.koshin.worker.WorkerDaemon
import dev.hononeko.koshin.worker.WorkerSettings
import dev.hononeko.koshin.worker.WorkspaceCleaner
import dev.hononeko.koshin.worker.createWorkerServer
import kotlinx.coroutines.runBlocking
import java.nio.file.Path
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.createTempDirectory
import kotlin.io.path.deleteRecursively
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

/**
 * A worker wired like `worker/Application.kt`, with a fake CLI and recording decorators around its ports. A positive
 * [idleTimeout] makes it a burst worker that drains and exits (KSH-31); otherwise it keeps a warm-worker lease.
 */
@Suppress("LongParameterList") // Mirrors the worker's settings
class TestWorker(
    val name: String,
    private val cluster: E2eCluster,
    runTime: Duration,
    platforms: Set<Platform>,
    idleTimeout: Duration = Duration.ZERO,
    maxLifetime: Duration = Duration.ZERO,
) : AutoCloseable {
    val cli = FakeCli(name, cluster.runs, runTime)
    val stall = Stall()

    private val baseDir = createTempDirectory("koshin-e2e-$name-")
    private val cacheDir = baseDir.resolve(WorkerSettings.RENOVATE_CACHE_SUBDIR)
    private val nats = WorkerBackends.connectNats(
        url = Containers.natsUrl,
        config = cluster.natsConfig,
        repoCache = cluster.repoCacheConfig,
        cacheDir = cacheDir,
    )
    private val valkey = runBlocking { WorkerBackends.connectValkey(cluster.valkeyConfig, cluster.throttle) }
    private val backends = WorkerBackends(connectNats = { nats }, connectValkey = { valkey })

    val consumer = RecordingConsumer(nats.consumer, stall)
    val coordination = RecordingCoordination(valkey.coordination, stall)
    val throttle = RecordingThrottle(valkey.throttle)

    private val useCase = ProcessRenovateJobUseCase(
        queueConsumer = consumer,
        queueDepth = nats.queueDepth,
        queuePublisher = nats.publisher,
        coordinationPort = coordination,
        throttlePort = throttle,
        cachePort = nats.repoCache,
        credentialsPort = StaticCredentialsProvider(
            Platform.entries.associateWith { PlatformCredentials("e2e-token") },
        ),
        cliExecutor = cli,
        workspaceCleaner = WorkspaceCleaner(baseDir, cacheDir),
        config = cluster.jobConfig,
    )
    private val presence = if (idleTimeout.isPositive()) {
        null
    } else {
        WarmPresence(cluster.jobConfig.heartbeatInterval, cluster.jobConfig.lockTtl, workerId = name) {
            valkey.warmWorkers
        }
    }

    /** Counted down when the daemon finishes on its own, where the worker process would exit 0. */
    val finished = CountDownLatch(1)
    private val daemon = WorkerDaemon(
        platforms = platforms,
        idleDelay = 100.milliseconds,
        errorDelay = 500.milliseconds,
        idleTimeout = idleTimeout,
        maxLifetime = maxLifetime,
        onFinished = {
            finished.countDown()
            thread { sigterm() }
        },
    ) { useCase }
    private val server = createWorkerServer(
        settings = WorkerSettings(port = 0, renovateBaseDir = baseDir, jobConfig = cluster.jobConfig),
        readinessCheck = backends::isReady,
        onStopping = {
            presence?.stop()
            daemon.stop()
        },
        onStopped = backends::close,
    )
    private val stopped = AtomicBoolean(false)

    val deliveries: List<DeliveryRecord>
        get() = consumer.deliveries

    fun runs(): List<CliRun> = cluster.runs.filter { it.worker == name }

    /** Where Renovate (`repositoryCacheType=local`) keeps [repo]'s cache on this worker. */
    fun cacheFile(repo: RepoRef): Path =
        cacheDir.resolve("renovate/repository/${repo.platform.name.lowercase()}/${repo.repository}.json")

    fun start() {
        backends.startAsync()
        server.start(wait = false)
        presence?.start()
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
