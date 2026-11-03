package dev.hononeko.koshin.core.ports

import dev.hononeko.koshin.core.model.RepoRef
import kotlin.time.Duration

/**
 * Per-repo coordination: `queued` (a job waits in the queue), `lock` (a run is in progress) and `dirty` (a change
 * arrived during the run). Methods taking a `queuedTtl` mark the repo queued for the job the caller publishes or NAKs
 * back to the queue; `null` leaves it unmarked.
 */
interface RepoCoordinationPort {
    /**
     * Pre-queue coalescing (KSH-30): marks the repo dirty if it's running ([EnqueueDecision.MARKED_DIRTY]), does
     * nothing if it's queued ([EnqueueDecision.ALREADY_QUEUED]), and otherwise marks it queued
     * ([EnqueueDecision.ENQUEUE]), for the caller to publish its event.
     */
    suspend fun enqueueOrCoalesce(repo: RepoRef, queuedTtl: Duration, dirtyTtl: Duration): EnqueueDecision

    /** Forgets that a job is queued for [repo]: its publish failed, or it was dead-lettered. */
    suspend fun clearQueued(repo: RepoRef)

    /** Takes the lock and clears the queued mark, or marks the repo dirty if another run holds the lock. */
    suspend fun acquireOrMarkDirty(repo: RepoRef, token: String, lockTtl: Duration, dirtyTtl: Duration): AcquireResult

    suspend fun extendLock(repo: RepoRef, token: String, ttl: Duration): Boolean

    /**
     * Atomically verifies ownership of the repository lock for [token], deletes the lock, and
     * consumes any pending dirty flag (`GETDEL`). With [queuedTtl], a consumed dirty flag also marks the repo
     * queued for the follow-up run the caller publishes.
     *
     * Note: This operation is non-idempotent once it reaches the backing store. If this method throws
     * an exception (for example, a connection drop or command timeout after the request was transmitted),
     * the lock and dirty flag may have already been deleted on the server. Callers must conservatively
     * handle recovery (such as NAKing the delivery for redelivery) so coalesced commits are not dropped.
     */
    suspend fun releaseLockAndTakeDirty(repo: RepoRef, token: String, queuedTtl: Duration? = null): ReleaseResult

    /** Releases the lock if [token] owns it, keeping the dirty flag; with [queuedTtl], also marks the repo queued. */
    suspend fun releaseLock(repo: RepoRef, token: String, queuedTtl: Duration? = null): Boolean
}
