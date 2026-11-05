package dev.hononeko.koshin.core.ports

import dev.hononeko.koshin.core.model.CredentialScope
import kotlin.time.Duration

/**
 * Run slots and rate-limit cooldowns per credential scope. A run slot is two leases taken together: one of
 * the global cap on runs, and one of its scope's cap.
 */
interface PlatformThrottlePort {
    /** Leases a global slot and one of [scope]'s, both or neither. */
    suspend fun tryAcquireRunSlot(scope: CredentialScope, token: String, ttl: Duration): Boolean

    /** Extends both leases; false if either is gone. */
    suspend fun extendRunSlot(scope: CredentialScope, token: String, ttl: Duration): Boolean
    suspend fun releaseRunSlot(scope: CredentialScope, token: String)

    /** [scope]'s run slots currently leased, by running jobs and by workers waiting on a fetch. */
    suspend fun activeRunSlots(scope: CredentialScope): Int

    suspend fun cooldownRemaining(scope: CredentialScope): Duration?
    suspend fun registerRateLimit(scope: CredentialScope, retryAfter: Duration?): Duration
    suspend fun resetBackoff(scope: CredentialScope)
}
