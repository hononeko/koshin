package dev.hononeko.koshin.e2e

import dev.hononeko.koshin.core.model.JobExecutionResult
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.PlatformCredentials
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.ports.AcquireResult
import dev.hononeko.koshin.core.ports.CliExecutorPort
import dev.hononeko.koshin.core.ports.JobDelivery
import dev.hononeko.koshin.core.ports.JobQueueConsumer
import dev.hononeko.koshin.core.ports.PlatformThrottlePort
import dev.hononeko.koshin.core.ports.ReleaseResult
import dev.hononeko.koshin.core.ports.RepoCoordinationPort
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration
import kotlin.time.TimeSource

/** Holds a worker's heartbeats between [begin] and [end], like a long GC or VM pause would. */
class Stall {
    private val gate = AtomicReference(CompletableDeferred(Unit))

    fun begin() {
        gate.set(CompletableDeferred())
    }

    fun end() {
        gate.get().complete(Unit)
    }

    suspend fun await() {
        gate.get().await()
    }
}

sealed interface Settlement {
    data object Ack : Settlement
    data class Nak(val delay: Duration) : Settlement
}

class DeliveryRecord(val event: RenovateJobEvent, val attempt: Int) {
    val receivedAt = TimeSource.Monotonic.markNow()

    /** Only set once the broker accepted it, so a crashed worker's attempt leaves it null. */
    @Volatile
    var settlement: Settlement? = null
}

data class Fetch(val platform: Platform, val at: TimeSource.Monotonic.ValueTimeMark)

class RecordingConsumer(
    private val delegate: JobQueueConsumer,
    private val stall: Stall,
    val deliveries: MutableList<DeliveryRecord> = CopyOnWriteArrayList(),
) : JobQueueConsumer {
    val fetches: MutableList<Fetch> = CopyOnWriteArrayList()

    override suspend fun fetchNext(platform: Platform, timeout: Duration): JobDelivery? {
        fetches += Fetch(platform, TimeSource.Monotonic.markNow())
        val delivery = delegate.fetchNext(platform, timeout) ?: return null
        val record = DeliveryRecord(delivery.event, delivery.deliveryAttempt)
        deliveries += record
        return RecordingDelivery(delivery, record, stall)
    }
}

private class RecordingDelivery(
    private val delegate: JobDelivery,
    private val record: DeliveryRecord,
    private val stall: Stall,
) : JobDelivery by delegate {
    override suspend fun ack() {
        delegate.ack()
        record.settlement = Settlement.Ack
    }

    override suspend fun nack(delay: Duration) {
        delegate.nack(delay)
        record.settlement = Settlement.Nak(delay)
    }

    override suspend fun heartbeat() {
        stall.await()
        delegate.heartbeat()
    }
}

class RecordingCoordination(private val delegate: RepoCoordinationPort, private val stall: Stall) :
    RepoCoordinationPort {

    data class Acquire(val token: String, val result: AcquireResult)
    data class Release(val token: String, val result: ReleaseResult)
    data class Unlock(val token: String, val released: Boolean)

    val acquires: MutableList<Acquire> = CopyOnWriteArrayList()
    val releases: MutableList<Release> = CopyOnWriteArrayList()
    val unlocks: MutableList<Unlock> = CopyOnWriteArrayList()

    override suspend fun acquireOrMarkDirty(
        repo: RepoRef,
        token: String,
        lockTtl: Duration,
        dirtyTtl: Duration,
    ): AcquireResult = delegate.acquireOrMarkDirty(repo, token, lockTtl, dirtyTtl).also {
        acquires += Acquire(token, it)
    }

    override suspend fun extendLock(repo: RepoRef, token: String, ttl: Duration): Boolean {
        stall.await()
        return delegate.extendLock(repo, token, ttl)
    }

    override suspend fun releaseLockAndTakeDirty(repo: RepoRef, token: String): ReleaseResult =
        delegate.releaseLockAndTakeDirty(repo, token).also { releases += Release(token, it) }

    override suspend fun releaseLock(repo: RepoRef, token: String): Boolean =
        delegate.releaseLock(repo, token).also { unlocks += Unlock(token, it) }
}

/** Records each platform cooldown the worker starts. */
class RecordingThrottle(private val delegate: PlatformThrottlePort) : PlatformThrottlePort by delegate {
    data class RateLimit(val platform: Platform, val cooldown: Duration, val at: TimeSource.Monotonic.ValueTimeMark)

    val rateLimits: MutableList<RateLimit> = CopyOnWriteArrayList()

    override suspend fun registerRateLimit(platform: Platform, retryAfter: Duration?): Duration {
        val at = TimeSource.Monotonic.markNow()
        return delegate.registerRateLimit(platform, retryAfter).also { rateLimits += RateLimit(platform, it, at) }
    }
}

class CliRun(val worker: String, val repo: RepoRef) {
    enum class State { RUNNING, COMPLETED, KILLED }

    val startedAt = TimeSource.Monotonic.markNow()

    @Volatile
    var state: State = State.RUNNING

    @Volatile
    var result: JobExecutionResult? = null
}

/**
 * Stands in for Renovate: sleeps for [runTime], or until released while [hold] is in effect, then returns [outcome].
 */
class FakeCli(private val worker: String, private val runs: MutableList<CliRun>, private val runTime: Duration) :
    CliExecutorPort {

    private val gate = AtomicReference<CompletableDeferred<Unit>?>(null)

    @Volatile
    var outcome: (RepoRef) -> JobExecutionResult = { JobExecutionResult.Success }

    /** Runs started from now on block until [release]. */
    fun hold() {
        gate.set(CompletableDeferred())
    }

    fun release() {
        gate.getAndSet(null)?.complete(Unit)
    }

    override suspend fun execute(repo: RepoRef, creds: PlatformCredentials, timeout: Duration): JobExecutionResult {
        val run = CliRun(worker, repo).also { runs += it }
        try {
            gate.get()?.await()
            delay(runTime)
        } catch (e: CancellationException) {
            run.state = CliRun.State.KILLED
            throw e
        }
        return outcome(repo).also {
            run.result = it
            run.state = CliRun.State.COMPLETED
        }
    }
}
