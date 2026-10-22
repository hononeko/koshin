package dev.hononeko.koshin.core.ports

import dev.hononeko.koshin.core.model.RepoRef
import kotlin.time.Duration

interface RepoCoordinationPort {
    suspend fun acquireOrMarkDirty(repo: RepoRef, token: String, lockTtl: Duration, dirtyTtl: Duration): AcquireResult

    suspend fun extendLock(repo: RepoRef, token: String, ttl: Duration): Boolean

    suspend fun releaseLockAndTakeDirty(repo: RepoRef, token: String): ReleaseResult

    suspend fun releaseLock(repo: RepoRef, token: String): Boolean
}
