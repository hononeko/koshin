package dev.hononeko.koshin.core.ports

import kotlin.time.Duration

/** A single-holder lease, e.g. so one proxy replica at a time dispatches workers (KSH-32). */
interface LeaderLeasePort {
    /** Takes the lease if it's free, or extends it if [holderId] holds it; false if someone else holds it. */
    suspend fun acquireOrRenew(holderId: String, ttl: Duration): Boolean

    /** Gives the lease up if [holderId] holds it. */
    suspend fun release(holderId: String)
}
