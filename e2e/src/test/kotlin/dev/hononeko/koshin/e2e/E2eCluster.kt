package dev.hononeko.koshin.e2e

import dev.hononeko.koshin.adapters.nats.NatsConfig
import dev.hononeko.koshin.adapters.nats.NatsJetStreamBootstrap
import dev.hononeko.koshin.adapters.nats.NatsJobQueueConsumer
import dev.hononeko.koshin.adapters.nats.NatsJobQueuePublisher
import dev.hononeko.koshin.adapters.nats.NatsQueueDepth
import dev.hononeko.koshin.adapters.nats.NatsRepoCacheConfig
import dev.hononeko.koshin.adapters.valkey.PlatformThrottleConfig
import dev.hononeko.koshin.adapters.valkey.ValkeyConfig
import dev.hononeko.koshin.adapters.valkey.ValkeyPlatformThrottleAdapter
import dev.hononeko.koshin.adapters.valkey.ValkeyRepoCoordinationAdapter
import dev.hononeko.koshin.adapters.valkey.ValkeyWarmWorkerAdapter
import dev.hononeko.koshin.core.model.CredentialScope
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.ports.QueueDepth
import dev.hononeko.koshin.core.usecase.WorkerConfig
import io.lettuce.core.RedisClient
import io.nats.client.Nats
import io.nats.client.api.StorageType
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import org.testcontainers.containers.GenericContainer
import java.io.ByteArrayOutputStream
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

object Containers {
    private val nats: GenericContainer<*> by lazy {
        GenericContainer("nats:2.10-alpine").withCommand("-js").withExposedPorts(4222).apply { start() }
    }

    private val valkey: GenericContainer<*> by lazy {
        GenericContainer("valkey/valkey:8-alpine").withExposedPorts(6379).apply { start() }
    }

    val natsUrl: String
        get() = "nats://${nats.host}:${nats.getMappedPort(4222)}"

    val valkeyUrl: String
        get() = "redis://${valkey.host}:${valkey.getMappedPort(6379)}"
}

/** One proxy and any number of workers, isolated from other tests by their own streams and key prefix. */
class E2eCluster(
    maxRateLimitRequeues: Int = WorkerConfig().maxRateLimitRequeues,
    val throttle: PlatformThrottleConfig = PlatformThrottleConfig(),
    maxDeliver: Long = 5L,
    exhaustedSweepInterval: Duration = 60.seconds,
) : AutoCloseable {
    private val id = nextId.incrementAndGet()

    val jobConfig = WorkerConfig(
        natsAckWait = 5.seconds,
        lockTtl = 3.seconds,
        heartbeatInterval = 1.seconds,
        dirtyTtl = 2.minutes,
        cliTimeout = 1.minutes,
        lockCollisionRetryDelay = 1.seconds,
        fetchTimeout = 1.seconds,
        maxRateLimitRequeues = maxRateLimitRequeues,
    )

    val natsConfig = NatsConfig(
        jobsStreamName = "E2E_JOBS_$id",
        dlqStreamName = "E2E_DLQ_$id",
        jobsSubjectPrefix = "e2e.$id.jobs",
        dlqSubjectPrefix = "e2e.$id.dlq",
        ackWait = jobConfig.natsAckWait,
        dlqForwarderQueueGroup = "e2e-$id-dlq-forwarders",
        storageType = StorageType.Memory,
        maxDeliver = maxDeliver,
        exhaustedSweepInterval = exhaustedSweepInterval,
    )

    val valkeyConfig = ValkeyConfig(valkeyUrl = Containers.valkeyUrl, keyPrefix = "e2e-$id")

    val repoCacheConfig = NatsRepoCacheConfig(bucket = "e2e-$id-repo-cache", storageType = StorageType.Memory)

    /** Every CLI run of every worker, in start order. */
    val runs: MutableList<CliRun> = CopyOnWriteArrayList()

    private val workers = CopyOnWriteArrayList<TestWorker>()
    private var proxy: TestProxy? = null

    private val nats = Nats.connect(Containers.natsUrl)
    private val jsm = nats.jetStreamManagement()
    private val queueDepth = NatsQueueDepth(jsm, natsConfig)
    private val publisher = NatsJobQueuePublisher(nats, natsConfig)
    private val redisClient = RedisClient.create(valkeyConfig.toRedisUri())
    private val redis = redisClient.connect()

    /** Plays a worker the suite doesn't run, e.g. one holding a lock. */
    val coordination = ValkeyRepoCoordinationAdapter(redis, valkeyConfig)

    /** Rate limits as a worker would register them. */
    val throttlePort = ValkeyPlatformThrottleAdapter(redis, valkeyConfig, throttle)

    private val warmWorkers = ValkeyWarmWorkerAdapter(redis, valkeyConfig)

    fun liveWarmWorkers(): Int = runBlocking { warmWorkers.liveCount() }

    init {
        NatsJetStreamBootstrap(nats, natsConfig).bootstrap()
    }

    /** With [coalescing] off, the proxy publishes every webhook, as when Valkey is down and it fails open. */
    fun startProxy(
        coalescing: Boolean = true,
        platforms: Set<Platform> = setOf(Platform.GITHUB, Platform.GITLAB),
    ): TestProxy = TestProxy(Containers.natsUrl, natsConfig, valkeyConfig.takeIf { coalescing }, platforms).also {
        proxy = it
        it.start()
    }

    /** With a positive [idleTimeout], a burst worker that drains and exits. */
    fun startWorker(
        name: String,
        runTime: Duration = DEFAULT_RUN_TIME,
        platforms: Set<Platform> = setOf(Platform.GITHUB),
        idleTimeout: Duration = Duration.ZERO,
        maxLifetime: Duration = Duration.ZERO,
    ): TestWorker = TestWorker(name, this, runTime, platforms, idleTimeout, maxLifetime).also {
        workers += it
        it.start()
    }

    fun publish(event: RenovateJobEvent) = runBlocking { publisher.publish(event) }

    /** A consumer outside any worker, to bounce a delivery so a worker sees it as a redelivery. */
    fun consumer(): NatsJobQueueConsumer = NatsJobQueueConsumer(nats, natsConfig)

    fun lockHolder(repo: RepoRef): String? = redis.sync().get(valkeyConfig.lockKeyFor(repo))

    fun isDirty(repo: RepoRef): Boolean = redis.sync().exists(valkeyConfig.dirtyKeyFor(repo)) > 0

    fun isQueued(repo: RepoRef): Boolean = redis.sync().exists(valkeyConfig.queuedKeyFor(repo)) > 0

    fun hasBackoff(scope: CredentialScope): Boolean =
        redis.sync().exists(valkeyConfig.throttleKeyFor(scope, "backoff_n")) > 0

    fun deadLetters(): List<RenovateJobEvent> {
        val state = jsm.getStreamInfo(natsConfig.dlqStreamName).streamState
        if (state.msgCount == 0L) return emptyList()
        return (state.firstSequence..state.lastSequence).map { seq ->
            val data = checkNotNull(jsm.getMessage(natsConfig.dlqStreamName, seq).data).decodeToString()
            Json.decodeFromString(RenovateJobEvent.serializer(), data)
        }
    }

    /** The repo's cache as stored in the Object Store bucket for [scope], or `null` if none was persisted. */
    fun storedCache(repo: RepoRef, scope: CredentialScope = CredentialScope.Token(repo.platform)): String? {
        val store = nats.objectStore(repoCacheConfig.bucket)
        val name = "${repo.platform.name.lowercase()}/${scope.token}/${repo.repository}.json"
        if (store.getInfo(name) == null) return null
        return ByteArrayOutputStream().also { store.get(name, it) }.toString(Charsets.UTF_8)
    }

    fun pendingJobs(scope: CredentialScope): Long = queueDepth(scope).pending

    /** [scope]'s queue; empty while it has no consumer. */
    fun queueDepth(scope: CredentialScope): QueueDepth =
        runBlocking { queueDepth.depths(listOf(scope.platform))[scope] } ?: QueueDepth(0, 0)

    /** Each delivery of [scope]'s consumer so far, counting redeliveries. */
    fun deliveredCount(scope: CredentialScope): Long =
        jsm.getConsumerInfo(natsConfig.jobsStreamName, natsConfig.consumerNameFor(scope)).delivered.consumerSequence

    fun publishedCount(): Long = jsm.getStreamInfo(natsConfig.jobsStreamName).streamState.lastSequence

    /**
     * Waits until no job is queued or in flight and the repo has no lock, dirty flag or queued mark. Only [scope]'s
     * queue counts if it's given, else every queue of the repo's platform.
     */
    fun awaitQuiescent(repo: RepoRef, scope: CredentialScope? = null) {
        var calmSince: TimeSource.Monotonic.ValueTimeMark? = null
        var state = ""
        try {
            awaitUntil("all jobs for $repo to drain", QUIESCENCE_TIMEOUT) {
                val queues = runBlocking { queueDepth.depths(listOf(repo.platform)) }
                    .filterKeys { scope == null || it == scope }
                    .values
                val consumer = QueueDepth(queues.sumOf { it.pending }, queues.sumOf { it.ackPending })
                val lock = lockHolder(repo)
                val dirty = isDirty(repo)
                val queued = isQueued(repo)
                val running = runs.filter { it.state == CliRun.State.RUNNING }.map { it.worker }
                state = "pending=${consumer.pending}, ackPending=${consumer.ackPending}, lock=$lock, " +
                    "dirty=$dirty, queued=$queued, running=$running"
                val calm = consumer.isEmpty &&
                    lock == null &&
                    !dirty &&
                    !queued &&
                    running.isEmpty()
                calmSince = if (calm) calmSince ?: TimeSource.Monotonic.markNow() else null
                calmSince?.let { it.elapsedNow() >= CALM_PERIOD } == true
            }
        } catch (e: IllegalStateException) {
            throw IllegalStateException("${e.message} ($state)", e)
        }
    }

    override fun close() {
        workers.forEach { runCatching { it.close() } }
        proxy?.let { runCatching { it.close() } }
        runCatching { redis.close() }
        runCatching { redisClient.shutdown() }
        runCatching { nats.close() }
    }

    companion object {
        private val nextId = AtomicInteger()
        val DEFAULT_RUN_TIME = 200.milliseconds
        private val QUIESCENCE_TIMEOUT = 30.seconds
        private val CALM_PERIOD = 500.milliseconds
    }
}

private val POLL_INTERVAL = 50.milliseconds

fun <T : Any> awaitValue(description: String, timeout: Duration = 20.seconds, probe: () -> T?): T {
    val deadline = TimeSource.Monotonic.markNow() + timeout
    while (true) {
        probe()?.let { return it }
        check(deadline.hasNotPassedNow()) { "Timed out after $timeout waiting for $description" }
        Thread.sleep(POLL_INTERVAL.inWholeMilliseconds)
    }
}

fun awaitUntil(description: String, timeout: Duration = 20.seconds, condition: () -> Boolean) {
    awaitValue(description, timeout) { Unit.takeIf { condition() } }
}
