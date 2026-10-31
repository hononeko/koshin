package dev.hononeko.koshin.e2e

import dev.hononeko.koshin.adapters.nats.NatsConfig
import dev.hononeko.koshin.adapters.nats.NatsJetStreamBootstrap
import dev.hononeko.koshin.adapters.nats.NatsJobQueueConsumer
import dev.hononeko.koshin.adapters.nats.NatsJobQueuePublisher
import dev.hononeko.koshin.adapters.nats.NatsRepoCacheConfig
import dev.hononeko.koshin.adapters.valkey.PlatformThrottleConfig
import dev.hononeko.koshin.adapters.valkey.ValkeyConfig
import dev.hononeko.koshin.adapters.valkey.ValkeyRepoCoordinationAdapter
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.model.RepoRef
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
    )

    val valkeyConfig = ValkeyConfig(valkeyUrl = Containers.valkeyUrl, keyPrefix = "e2e-$id")

    val repoCacheConfig = NatsRepoCacheConfig(bucket = "e2e-$id-repo-cache", storageType = StorageType.Memory)

    /** Every CLI run of every worker, in start order. */
    val runs: MutableList<CliRun> = CopyOnWriteArrayList()

    private val workers = CopyOnWriteArrayList<TestWorker>()
    private var proxy: TestProxy? = null

    private val nats = Nats.connect(Containers.natsUrl)
    private val jsm = nats.jetStreamManagement()
    private val publisher = NatsJobQueuePublisher(nats, natsConfig)
    private val redisClient = RedisClient.create(valkeyConfig.toRedisUri())
    private val redis = redisClient.connect()

    /** Plays a worker the suite doesn't run, e.g. one holding a lock. */
    val coordination = ValkeyRepoCoordinationAdapter(redis, valkeyConfig)

    init {
        NatsJetStreamBootstrap(nats, natsConfig).bootstrap(Platform.entries)
    }

    fun startProxy(): TestProxy = TestProxy(Containers.natsUrl, natsConfig).also {
        proxy = it
        it.start()
    }

    fun startWorker(
        name: String,
        runTime: Duration = DEFAULT_RUN_TIME,
        platforms: Set<Platform> = setOf(Platform.GITHUB),
    ): TestWorker = TestWorker(name, this, runTime, platforms).also {
        workers += it
        it.start()
    }

    fun publish(event: RenovateJobEvent) = runBlocking { publisher.publish(event) }

    /** A consumer outside any worker, to bounce a delivery so a worker sees it as a redelivery. */
    fun consumer(): NatsJobQueueConsumer = NatsJobQueueConsumer(nats, natsConfig)

    fun lockHolder(repo: RepoRef): String? = redis.sync().get(valkeyConfig.lockKeyFor(repo))

    fun isDirty(repo: RepoRef): Boolean = redis.sync().exists(valkeyConfig.dirtyKeyFor(repo)) > 0

    fun hasBackoff(platform: Platform): Boolean =
        redis.sync().exists(valkeyConfig.throttleKeyFor(platform, "backoff_n")) > 0

    fun deadLetters(): List<RenovateJobEvent> {
        val state = jsm.getStreamInfo(natsConfig.dlqStreamName).streamState
        if (state.msgCount == 0L) return emptyList()
        return (state.firstSequence..state.lastSequence).map { seq ->
            val data = checkNotNull(jsm.getMessage(natsConfig.dlqStreamName, seq).data).decodeToString()
            Json.decodeFromString(RenovateJobEvent.serializer(), data)
        }
    }

    /** The repo's cache as stored in the Object Store bucket, or `null` if none was persisted. */
    fun storedCache(repo: RepoRef): String? {
        val store = nats.objectStore(repoCacheConfig.bucket)
        val name = "${repo.platform.name.lowercase()}/${repo.repository}.json"
        if (store.getInfo(name) == null) return null
        return ByteArrayOutputStream().also { store.get(name, it) }.toString(Charsets.UTF_8)
    }

    fun publishedCount(): Long = jsm.getStreamInfo(natsConfig.jobsStreamName).streamState.lastSequence

    /** Waits until no job is queued or in flight and the repo has neither a lock nor a dirty flag. */
    fun awaitQuiescent(repo: RepoRef) {
        var calmSince: TimeSource.Monotonic.ValueTimeMark? = null
        var state = ""
        try {
            awaitUntil("all jobs for $repo to drain", QUIESCENCE_TIMEOUT) {
                val consumer = jsm.getConsumerInfo(natsConfig.jobsStreamName, natsConfig.consumerNameFor(repo.platform))
                val lock = lockHolder(repo)
                val dirty = isDirty(repo)
                val running = runs.filter { it.state == CliRun.State.RUNNING }.map { it.worker }
                state = "pending=${consumer.numPending}, ackPending=${consumer.numAckPending}, lock=$lock, " +
                    "dirty=$dirty, running=$running"
                val calm = consumer.numPending == 0L &&
                    consumer.numAckPending == 0L &&
                    lock == null &&
                    !dirty &&
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
