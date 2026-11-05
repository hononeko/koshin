package dev.hononeko.koshin.core.ports

import kotlin.time.Duration

/**
 * Warm-worker presence: a daemon-mode worker keeps a lease renewed while it runs, so the Job dispatcher
 * knows whether one is ready before it starts a burst worker for a single job.
 */
interface WarmWorkerPort {
    /** Adds or extends [workerId]'s lease to [ttl] from now. */
    suspend fun renew(workerId: String, ttl: Duration)

    /** Drops [workerId]'s lease, e.g. on shutdown. */
    suspend fun remove(workerId: String)

    /** Workers whose lease hasn't expired. */
    suspend fun liveCount(): Int
}
