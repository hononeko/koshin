package dev.hononeko.koshin.proxy

import dev.hononeko.koshin.adapters.valkey.PlatformThrottleConfig
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.ports.BurstWorkerPort
import dev.hononeko.koshin.core.ports.JobQueuePublisher
import dev.hononeko.koshin.core.ports.QueueDepthPort
import dev.hononeko.koshin.core.usecase.DispatchConfig
import dev.hononeko.koshin.core.usecase.DispatchResult
import dev.hononeko.koshin.core.usecase.DispatchWorkersUseCase
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import java.util.UUID
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.seconds

/** The proxy's worker dispatcher (KSH-32); off outside Kubernetes (`WORKER_DISPATCHER_ENABLED`). */
data class DispatcherConfig(
    val enabled: Boolean = false,
    val podTemplateName: String = DEFAULT_POD_TEMPLATE_NAME,
    /** `POD_NAMESPACE`; the ServiceAccount's namespace when unset. */
    val namespace: String? = null,
    /** `WORKER_MAX_REPLICAS`: more workers than global run slots would only wait for one. */
    val maxWorkers: Int = PlatformThrottleConfig.DEFAULT_MAX_RUNS_GLOBAL,
    /** `MAX_CONCURRENT_RUNS_PER_CREDENTIAL` (KSH-24). */
    val maxRunsPerCredential: Int = PlatformThrottleConfig.DEFAULT_MAX_RUNS_PER_CREDENTIAL,
    /** `MAX_CONCURRENT_RUNS_GLOBAL` (KSH-24). */
    val maxRunsGlobal: Int = PlatformThrottleConfig.DEFAULT_MAX_RUNS_GLOBAL,
    val minWarmWorkers: Int = 0,
    /** Between reconciliations when nothing is published; also how often the leader renews its lease. */
    val interval: Duration = DEFAULT_INTERVAL,
    val jobActiveDeadline: Duration = DEFAULT_JOB_ACTIVE_DEADLINE,
    val jobTtlAfterFinished: Duration = DEFAULT_JOB_TTL_AFTER_FINISHED,
) {
    init {
        require(podTemplateName.isNotBlank()) { "WORKER_POD_TEMPLATE_NAME cannot be blank" }
        require(namespace == null || namespace.isNotBlank()) { "POD_NAMESPACE cannot be blank" }
        require(maxWorkers >= 1) { "WORKER_MAX_REPLICAS must be at least 1: $maxWorkers" }
        require(maxRunsPerCredential >= 1) {
            "MAX_CONCURRENT_RUNS_PER_CREDENTIAL must be at least 1: $maxRunsPerCredential"
        }
        require(maxRunsGlobal >= 1) { "MAX_CONCURRENT_RUNS_GLOBAL must be at least 1: $maxRunsGlobal" }
        require(minWarmWorkers in 0..maxWorkers) { "MIN_WARM_WORKERS must be in 0..$maxWorkers: $minWarmWorkers" }
        require(interval.isPositive() && interval.isFinite()) { "interval must be positive: $interval" }
        require(jobActiveDeadline.inWholeSeconds >= 1) { "jobActiveDeadline must be at least 1s: $jobActiveDeadline" }
        require(!jobTtlAfterFinished.isNegative()) { "jobTtlAfterFinished cannot be negative: $jobTtlAfterFinished" }
    }

    /** Outlives two missed renewals, so a slow reconciliation doesn't hand leadership over. */
    val leaseTtl: Duration
        get() = interval * LEASE_RENEWALS

    fun dispatch(platforms: Set<Platform>) =
        DispatchConfig(platforms, maxWorkers, maxRunsPerCredential, minWarmWorkers, maxRunsGlobal)

    companion object {
        const val DEFAULT_POD_TEMPLATE_NAME = "koshin-worker"
        private const val LEASE_RENEWALS = 3
        val DEFAULT_INTERVAL: Duration = 5.seconds
        val DEFAULT_JOB_ACTIVE_DEADLINE: Duration = 6.hours
        val DEFAULT_JOB_TTL_AFTER_FINISHED: Duration = 60.seconds
    }
}

/**
 * Keeps enough burst workers for the queue (KSH-32): reconciles right after the proxy publishes a job and every
 * [DispatcherConfig.interval] otherwise (cooldowns ending, a warm worker gone, a crashed `Job` to replace). Only the
 * replica holding the Valkey leader lease reconciles; the others take over within the lease TTL if it stops renewing.
 */
class WorkerDispatcher(
    private val config: DispatcherConfig,
    private val platforms: Set<Platform>,
    /** The Valkey ports, or `null` while Valkey isn't connected. */
    private val valkey: () -> ProxyValkey.DispatchPorts?,
    private val queueDepth: QueueDepthPort,
    private val burstWorkers: BurstWorkerPort,
    private val holderId: String = "proxy-${UUID.randomUUID()}",
) {
    private val logger = LoggerFactory.getLogger(WorkerDispatcher::class.java)
    private val wakeups = Channel<Unit>(Channel.CONFLATED)

    @Volatile
    var isLeader: Boolean = false
        private set

    /** Reconciles as soon as possible, e.g. after a publish. */
    fun wake() {
        wakeups.trySend(Unit)
    }

    fun start(scope: CoroutineScope): Job = scope.launch {
        logger.info("Dispatching burst workers from PodTemplate {} as {}", config.podTemplateName, holderId)
        while (isActive) {
            reconcileQuietly()
            withTimeoutOrNull(config.interval) { wakeups.receive() }
        }
    }

    /** One reconciliation if this replica leads; `null` if it doesn't or Valkey isn't connected. */
    suspend fun reconcileOnce(): DispatchResult? {
        val ports = valkey()?.takeIf { it.leader.acquireOrRenew(holderId, config.leaseTtl) }
        lead(ports != null)
        if (ports == null) return null
        val useCase = DispatchWorkersUseCase(
            queueDepth = queueDepth,
            throttle = ports.throttle,
            warmWorkers = ports.warmWorkers,
            burstWorkers = burstWorkers,
            config = config.dispatch(platforms),
        ) { e -> logger.warn("Can't count warm workers; assuming {}", config.minWarmWorkers, e) }
        return useCase.reconcile().also { result ->
            if (result.started > 0) {
                logger.atInfo()
                    .addKeyValue("desiredWorkers", result.desiredWorkers)
                    .addKeyValue("warmWorkers", result.warmWorkers)
                    .log("Started {} burst worker(s)", result.started)
            }
        }
    }

    private suspend fun reconcileQuietly() {
        try {
            reconcileOnce()
        } catch (e: CancellationException) {
            throw e
        } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
            logger.warn("Worker dispatch failed; retrying in {}", config.interval, e)
        }
    }

    private fun lead(leading: Boolean) {
        if (leading != isLeader) {
            logger.info(if (leading) "Leading worker dispatch" else "No longer leading worker dispatch")
        }
        isLeader = leading
    }

    /** Hands the lease over at once instead of letting it expire. */
    suspend fun stop() {
        if (!isLeader) return
        withContext(NonCancellable) {
            try {
                valkey()?.leader?.release(holderId)
            } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
                logger.warn("Could not release the dispatcher lease; it expires in {}", config.leaseTtl, e)
            }
            isLeader = false
        }
    }
}

/** Wakes the dispatcher after every job the proxy publishes. */
class WakingPublisher(private val delegate: JobQueuePublisher, private val onPublished: () -> Unit) :
    JobQueuePublisher by delegate {
    override suspend fun publish(event: RenovateJobEvent) {
        delegate.publish(event)
        onPublished()
    }
}
