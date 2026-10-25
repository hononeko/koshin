package dev.hononeko.koshin.core.ports

import dev.hononeko.koshin.core.model.RepoRef
import kotlin.time.Duration

interface RepoCoordinationPort {
    suspend fun acquireOrMarkDirty(repo: RepoRef, token: String, lockTtl: Duration, dirtyTtl: Duration): AcquireResult

    suspend fun extendLock(repo: RepoRef, token: String, ttl: Duration): Boolean

    /**
     * Atomically verifies ownership of the repository lock for [token], deletes the lock, and
     * consumes any pending dirty flag (`GETDEL`).
     *
     * Note: This operation is non-idempotent once it reaches the backing store. If this method throws
     * an exception (for example, a connection drop or command timeout after the request was transmitted),
     * the lock and dirty flag may have already been deleted on the server. Callers must conservatively
     * handle recovery (such as NAKing the delivery for redelivery) so coalesced commits are not dropped.
     */
    suspend fun releaseLockAndTakeDirty(repo: RepoRef, token: String): ReleaseResult

    suspend fun releaseLock(repo: RepoRef, token: String): Boolean
}
