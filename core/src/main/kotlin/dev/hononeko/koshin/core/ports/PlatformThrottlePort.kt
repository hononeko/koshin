package dev.hononeko.koshin.core.ports

import dev.hononeko.koshin.core.model.Platform
import kotlin.time.Duration

interface PlatformThrottlePort {
    suspend fun tryAcquireRunSlot(platform: Platform, token: String, ttl: Duration): Boolean
    suspend fun extendRunSlot(platform: Platform, token: String, ttl: Duration): Boolean
    suspend fun releaseRunSlot(platform: Platform, token: String)

    /** Run slots currently leased, by running jobs and by workers waiting on a fetch. */
    suspend fun activeRunSlots(platform: Platform): Int

    suspend fun cooldownRemaining(platform: Platform): Duration?
    suspend fun registerRateLimit(platform: Platform, retryAfter: Duration?): Duration
    suspend fun resetBackoff(platform: Platform)
}
