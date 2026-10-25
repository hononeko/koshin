package dev.hononeko.koshin.worker

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.ports.PlatformThrottlePort
import dev.hononeko.koshin.core.ports.RepoCachePort
import kotlin.time.Duration

/** Grants every run slot and never cools down. Replaced by the Valkey throttle in KSH-11. */
object UnthrottledPlatformThrottle : PlatformThrottlePort {
    override suspend fun tryAcquireRunSlot(platform: Platform, token: String, ttl: Duration): Boolean = true
    override suspend fun extendRunSlot(platform: Platform, token: String, ttl: Duration): Boolean = true
    override suspend fun releaseRunSlot(platform: Platform, token: String) = Unit
    override suspend fun cooldownRemaining(platform: Platform): Duration? = null
    override suspend fun registerRateLimit(platform: Platform, retryAfter: Duration?): Duration = Duration.ZERO
    override suspend fun resetBackoff(platform: Platform) = Unit
}

/** No shared repository cache. Replaced in KSH-10. */
object NoOpRepoCache : RepoCachePort {
    override suspend fun restore(repo: RepoRef): Boolean = false
    override suspend fun persist(repo: RepoRef) = Unit
}
