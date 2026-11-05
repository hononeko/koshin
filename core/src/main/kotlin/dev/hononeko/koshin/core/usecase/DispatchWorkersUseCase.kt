package dev.hononeko.koshin.core.usecase

import dev.hononeko.koshin.core.model.CredentialScope
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.ports.BurstWorkerPort
import dev.hononeko.koshin.core.ports.PlatformThrottlePort
import dev.hononeko.koshin.core.ports.QueueDepth
import dev.hononeko.koshin.core.ports.QueueDepthPort
import dev.hononeko.koshin.core.ports.WarmWorkerPort
import kotlinx.coroutines.CancellationException
import kotlin.time.Duration

/**
 * How many workers the queue needs. [maxWorkers] caps them all, [maxRunsGlobal] the runs across scopes
 * (`MAX_CONCURRENT_RUNS_GLOBAL`), [maxRunsPerScope] the runs of one credential scope
 * (`MAX_CONCURRENT_RUNS_PER_CREDENTIAL`), and [minWarmWorkers] stands in for the warm-worker count when it
 * can't be read.
 */
data class DispatchConfig(
    val platforms: Set<Platform>,
    val maxWorkers: Int,
    val maxRunsPerScope: Int,
    val minWarmWorkers: Int = 0,
    val maxRunsGlobal: Int = maxWorkers,
) {
    init {
        require(platforms.isNotEmpty()) { "platforms cannot be empty" }
        require(maxWorkers >= 1) { "maxWorkers must be at least 1: $maxWorkers" }
        require(maxRunsPerScope >= 1) { "maxRunsPerScope must be at least 1: $maxRunsPerScope" }
        require(maxRunsGlobal >= 1) { "maxRunsGlobal must be at least 1: $maxRunsGlobal" }
        require(minWarmWorkers in 0..maxWorkers) { "minWarmWorkers must be in 0..$maxWorkers: $minWarmWorkers" }
    }
}

/**
 * One scope's load. A worker holds a run slot while it waits on a fetch too, so a run is a slot with a delivery:
 * `min(runSlots, ackPending)`. Deliveries without a slot were NAKed for a later retry and still need a worker.
 */
data class ScopeLoad(val pending: Long, val ackPending: Long, val runSlots: Int, val coolingDown: Boolean) {
    val running: Long
        get() = minOf(runSlots.toLong(), ackPending)

    val runnable: Long
        get() = pending + ackPending - running
}

/** What a reconciliation saw and did. */
data class DispatchResult(val desiredWorkers: Int, val warmWorkers: Int, val desiredBurst: Int, val started: Int)

/**
 * Starts the burst workers the runnable jobs need, on top of the warm workers, summing over the credential
 * scopes with queued jobs: `desiredWorkers = min(maxWorkers, maxRunsGlobal, running +
 * Σ_{scope not cooling down} min(runnable, maxRunsPerScope - running))` and
 * `desiredBurst = max(0, min(maxWorkers - warm, desiredWorkers - warm))`. A cooling-down scope's backlog starts no
 * worker until its cooldown ends. Never stops a worker: burst workers drain and exit on their own.
 */
class DispatchWorkersUseCase(
    private val queueDepth: QueueDepthPort,
    private val throttle: PlatformThrottlePort,
    private val warmWorkers: WarmWorkerPort,
    private val burstWorkers: BurstWorkerPort,
    private val config: DispatchConfig,
    /** The warm-worker count couldn't be read, so [DispatchConfig.minWarmWorkers] is assumed. */
    private val onWarmCountFailed: (Throwable) -> Unit = {},
) {
    suspend fun reconcile(): DispatchResult {
        val loads = queueDepth.depths(config.platforms)
            .filterValues { !it.isEmpty }
            .map { (scope, depth) -> load(scope, depth) }
        val desiredWorkers = desiredWorkers(loads)
        val warm = warmCount()
        val desiredBurst = desiredBurst(desiredWorkers, warm)
        val toStart = if (desiredBurst > 0) (desiredBurst - burstWorkers.active()).coerceAtLeast(0) else 0
        if (toStart > 0) burstWorkers.start(toStart)
        return DispatchResult(desiredWorkers, warm, desiredBurst, toStart)
    }

    internal fun desiredWorkers(loads: Collection<ScopeLoad>): Int {
        val running = loads.sumOf { it.running }
        val wanted = loads.filterNot { it.coolingDown }.sumOf { load ->
            minOf(load.runnable, (config.maxRunsPerScope - load.running).coerceAtLeast(0))
        }
        return minOf(config.maxWorkers.toLong(), config.maxRunsGlobal.toLong(), running + wanted).toInt()
    }

    internal fun desiredBurst(desiredWorkers: Int, warm: Int): Int =
        minOf(config.maxWorkers - warm, desiredWorkers - warm).coerceAtLeast(0)

    private suspend fun load(scope: CredentialScope, depth: QueueDepth): ScopeLoad {
        val cooldown = throttle.cooldownRemaining(scope)
        return ScopeLoad(
            pending = depth.pending,
            ackPending = depth.ackPending,
            runSlots = throttle.activeRunSlots(scope),
            coolingDown = cooldown != null && cooldown > Duration.ZERO,
        )
    }

    private suspend fun warmCount(): Int = try {
        warmWorkers.liveCount()
    } catch (e: CancellationException) {
        throw e
    } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
        onWarmCountFailed(e)
        config.minWarmWorkers
    }
}
