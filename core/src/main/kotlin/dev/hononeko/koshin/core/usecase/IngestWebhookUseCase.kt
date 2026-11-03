package dev.hononeko.koshin.core.usecase

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.model.TriggerReason
import dev.hononeko.koshin.core.ports.EnqueueDecision
import dev.hononeko.koshin.core.ports.JobQueuePublisher
import dev.hononeko.koshin.core.ports.RepoCoordinationPort
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import java.util.UUID
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/** Pre-queue coalescing (KSH-30): how long marks last, and how long ingest waits for Valkey before failing open. */
data class IngestConfig(
    val queuedTtl: Duration = DEFAULT_QUEUED_TTL,
    val dirtyTtl: Duration = DEFAULT_DIRTY_TTL,
    val coalesceTimeout: Duration = DEFAULT_COALESCE_TIMEOUT,
) {
    init {
        require(queuedTtl.isFinite() && queuedTtl >= 1.seconds) { "queuedTtl must be at least 1s: $queuedTtl" }
        require(dirtyTtl.isFinite() && dirtyTtl >= 1.seconds) { "dirtyTtl must be at least 1s: $dirtyTtl" }
        require(coalesceTimeout.isFinite() && coalesceTimeout.isPositive()) {
            "coalesceTimeout must be positive: $coalesceTimeout"
        }
    }

    companion object {
        val DEFAULT_QUEUED_TTL: Duration = 24.hours
        val DEFAULT_DIRTY_TTL: Duration = 3600.seconds
        val DEFAULT_COALESCE_TIMEOUT: Duration = 500.milliseconds
    }
}

/** An ingested event, and whether it was published ([EnqueueDecision.ENQUEUE]) or coalesced. */
data class IngestResult(val event: RenovateJobEvent, val decision: EnqueueDecision) {
    val published: Boolean
        get() = decision == EnqueueDecision.ENQUEUE
}

/** What ingest reports about Valkey failing it. */
interface IngestObserver {
    /** Coalescing failed with [cause], or timed out (`null`), so the event is published anyway. */
    fun failedOpen(repo: RepoRef, cause: Throwable?) = Unit

    /**
     * After a failed publish, clearing [repo]'s queued mark failed with [cause] or timed out (`null`): its webhooks
     * are dropped until the mark expires.
     */
    fun queuedMarkKept(repo: RepoRef, cause: Throwable?) = Unit

    object None : IngestObserver
}

/**
 * Publishes one job per webhook or trigger. With a [coordination] port, duplicates are coalesced before publishing,
 * so the queue holds distinct runnable repos; if Valkey fails or is slow, the event is published anyway (fail-open),
 * since GitHub doesn't retry failed webhook deliveries.
 */
class IngestWebhookUseCase(
    private val publisher: JobQueuePublisher,
    private val coordination: RepoCoordinationPort? = null,
    private val config: IngestConfig = IngestConfig(),
    private val observer: IngestObserver = IngestObserver.None,
    private val uuidGenerator: () -> String = { UUID.randomUUID().toString() },
) {
    suspend fun execute(
        platform: Platform,
        repository: String,
        deliveryId: String? = null,
        triggerReason: TriggerReason = TriggerReason.WEBHOOK,
        /** The GitHub App installation the job runs as, routing it to that installation's queue (KSH-24). */
        installationId: Long? = null,
    ): IngestResult {
        val repo = RepoRef(platform, repository.trim())

        val resolvedDeliveryId = if (deliveryId.isNullOrBlank()) {
            uuidGenerator()
        } else {
            deliveryId.trim()
        }

        val event = RenovateJobEvent(
            repo = repo,
            deliveryId = resolvedDeliveryId,
            triggerReason = triggerReason,
            rateLimitAttempt = 0,
            installationId = installationId,
        )
        val decision = coalesce(repo)
        if (decision == EnqueueDecision.ALREADY_QUEUED || decision == EnqueueDecision.MARKED_DIRTY) {
            return IngestResult(event, decision)
        }
        try {
            publisher.publish(event)
        } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
            if (decision == EnqueueDecision.ENQUEUE) {
                withContext(NonCancellable) { forgetQueued(repo) }
            }
            throw e
        }
        return IngestResult(event, EnqueueDecision.ENQUEUE)
    }

    /** The decision, or `null` to publish without one (no Valkey, or it failed). */
    private suspend fun coalesce(repo: RepoRef): EnqueueDecision? {
        val port = coordination ?: return null
        return try {
            withTimeoutOrNull(config.coalesceTimeout) {
                port.enqueueOrCoalesce(repo, config.queuedTtl, config.dirtyTtl)
            } ?: run {
                observer.failedOpen(repo, null)
                null
            }
        } catch (e: CancellationException) {
            throw e
        } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
            observer.failedOpen(repo, e)
            null
        }
    }

    /** Clears the mark of an event that wasn't published, so the next webhook publishes again. */
    private suspend fun forgetQueued(repo: RepoRef) {
        val port = coordination ?: return
        try {
            withTimeoutOrNull(config.coalesceTimeout) { port.clearQueued(repo) }
                ?: observer.queuedMarkKept(repo, null)
        } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
            observer.queuedMarkKept(repo, e)
        }
    }

    companion object {
        fun validateRepositoryFormat(platform: Platform, repository: String) {
            RepoRef(platform, repository)
        }
    }
}
