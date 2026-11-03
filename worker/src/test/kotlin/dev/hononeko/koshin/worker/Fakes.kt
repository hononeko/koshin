package dev.hononeko.koshin.worker

import dev.hononeko.koshin.core.model.CredentialScope
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.ports.AcquireResult
import dev.hononeko.koshin.core.ports.EnqueueDecision
import dev.hononeko.koshin.core.ports.JobDelivery
import dev.hononeko.koshin.core.ports.JobQueueConsumer
import dev.hononeko.koshin.core.ports.JobQueuePublisher
import dev.hononeko.koshin.core.ports.QueueDepth
import dev.hononeko.koshin.core.ports.QueueDepthPort
import dev.hononeko.koshin.core.ports.ReleaseResult
import dev.hononeko.koshin.core.ports.RepoCoordinationPort
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.time.Duration

class RecordingDelivery(override val event: RenovateJobEvent, override val deliveryAttempt: Int = 1) : JobDelivery {
    val acks = CopyOnWriteArrayList<Unit>()
    val nacks = CopyOnWriteArrayList<Duration>()

    override suspend fun ack() {
        acks += Unit
    }

    override suspend fun nack(delay: Duration) {
        nacks += delay
    }

    override suspend fun heartbeat() = Unit
}

/** A queue of [deliveries] that's also its own depth, by the scope of each delivery's job. */
class QueueConsumer(vararg deliveries: JobDelivery) :
    JobQueueConsumer,
    QueueDepthPort {
    private val queue = ConcurrentLinkedQueue(deliveries.toList())

    override suspend fun fetchNext(scope: CredentialScope, timeout: Duration): JobDelivery? = synchronized(queue) {
        queue.firstOrNull { it.event.scope == scope }?.also { queue.remove(it) }
    }

    override suspend fun depths(platforms: Collection<Platform>): Map<CredentialScope, QueueDepth> =
        synchronized(queue) {
            queue.filter { it.event.repo.platform in platforms }
                .groupingBy { it.event.scope }
                .eachCount()
                .mapValues { (_, count) -> QueueDepth(count.toLong(), 0) }
        }
}

class RecordingPublisher : JobQueuePublisher {
    val published = CopyOnWriteArrayList<RenovateJobEvent>()
    val deadLettered = CopyOnWriteArrayList<RenovateJobEvent>()

    override suspend fun publish(event: RenovateJobEvent) {
        published += event
    }

    override suspend fun publishToDeadLetter(event: RenovateJobEvent) {
        deadLettered += event
    }
}

/** Same semantics as the Valkey Lua scripts. */
class InMemoryCoordination : RepoCoordinationPort {
    val locks = ConcurrentHashMap<RepoRef, String>()
    val dirty: MutableSet<RepoRef> = ConcurrentHashMap.newKeySet()
    val queued: MutableSet<RepoRef> = ConcurrentHashMap.newKeySet()

    override suspend fun enqueueOrCoalesce(repo: RepoRef, queuedTtl: Duration, dirtyTtl: Duration): EnqueueDecision =
        synchronized(this) {
            when {
                locks.containsKey(repo) -> EnqueueDecision.MARKED_DIRTY.also { dirty += repo }
                queued.add(repo) -> EnqueueDecision.ENQUEUE
                else -> EnqueueDecision.ALREADY_QUEUED
            }
        }

    override suspend fun clearQueued(repo: RepoRef) {
        queued -= repo
    }

    override suspend fun acquireOrMarkDirty(
        repo: RepoRef,
        token: String,
        lockTtl: Duration,
        dirtyTtl: Duration,
    ): AcquireResult = synchronized(this) {
        if (locks.putIfAbsent(repo, token) == null) {
            queued -= repo
            AcquireResult.ACQUIRED
        } else {
            dirty += repo
            AcquireResult.COALESCED
        }
    }

    override suspend fun extendLock(repo: RepoRef, token: String, ttl: Duration): Boolean = locks[repo] == token

    override suspend fun releaseLockAndTakeDirty(repo: RepoRef, token: String, queuedTtl: Duration?): ReleaseResult =
        synchronized(this) {
            when {
                !locks.remove(repo, token) -> ReleaseResult.NOT_OWNER
                dirty.remove(repo) -> ReleaseResult.DIRTY.also { if (queuedTtl != null) queued += repo }
                else -> ReleaseResult.CLEAN
            }
        }

    override suspend fun releaseLock(repo: RepoRef, token: String, queuedTtl: Duration?): Boolean = synchronized(this) {
        locks.remove(repo, token).also { released -> if (released && queuedTtl != null) queued += repo }
    }
}
