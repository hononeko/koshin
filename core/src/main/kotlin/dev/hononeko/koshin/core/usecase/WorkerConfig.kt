package dev.hononeko.koshin.core.usecase

import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

data class WorkerConfig(
    val natsAckWait: Duration = 60.seconds,
    val lockTtl: Duration = 45.seconds,
    val dirtyTtl: Duration = 3600.seconds,
    val cliTimeout: Duration = 45.minutes,
    val heartbeatInterval: Duration = 15.seconds,
    val lockCollisionRetryDelay: Duration = 30.seconds,
    val maxRateLimitRequeues: Int = 5,
    val fetchTimeout: Duration = 5.seconds,
    val infraErrorBackoffBase: Duration = 1.seconds,
    val infraErrorBackoffMax: Duration = 30.seconds,
) {
    init {
        require(natsAckWait > Duration.ZERO) { "natsAckWait must be positive: $natsAckWait" }
        require(lockTtl > Duration.ZERO) { "lockTtl must be positive: $lockTtl" }
        require(dirtyTtl > Duration.ZERO) { "dirtyTtl must be positive: $dirtyTtl" }
        require(cliTimeout > Duration.ZERO) { "cliTimeout must be positive: $cliTimeout" }
        require(heartbeatInterval > Duration.ZERO) { "heartbeatInterval must be positive: $heartbeatInterval" }
        require(lockCollisionRetryDelay > Duration.ZERO) {
            "lockCollisionRetryDelay must be positive: $lockCollisionRetryDelay"
        }
        require(fetchTimeout > Duration.ZERO) { "fetchTimeout must be positive: $fetchTimeout" }
        require(infraErrorBackoffBase > Duration.ZERO) {
            "infraErrorBackoffBase must be positive: $infraErrorBackoffBase"
        }
        require(infraErrorBackoffMax >= infraErrorBackoffBase) {
            "infraErrorBackoffMax ($infraErrorBackoffMax) must be >= infraErrorBackoffBase ($infraErrorBackoffBase)"
        }
        require(maxRateLimitRequeues >= 0) {
            "MAX_RATE_LIMIT_REQUEUES ($maxRateLimitRequeues) must be >= 0"
        }

        val ackSecs = natsAckWait.inWholeSeconds
        val lockSecs = lockTtl.inWholeSeconds
        require(natsAckWait > lockTtl) {
            "NATS_ACK_WAIT_SECONDS ($ackSecs) must be > LOCK_TTL_SECONDS ($lockSecs)"
        }
        val dirtySecs = dirtyTtl.inWholeSeconds
        val cliSecs = cliTimeout.inWholeSeconds
        require(dirtyTtl > cliTimeout) {
            "DIRTY_TTL_SECONDS ($dirtySecs) must be > CLI_TIMEOUT_MINUTES × 60 ($cliSecs)"
        }
        val hbSecs = heartbeatInterval.inWholeSeconds
        val halfLockSecs = lockSecs / 2
        require(heartbeatInterval < lockTtl / 2) {
            "HEARTBEAT_INTERVAL_SECONDS ($hbSecs) must be < LOCK_TTL_SECONDS / 2 ($halfLockSecs)"
        }
        require(fetchTimeout < lockTtl) {
            "fetchTimeout (${fetchTimeout.inWholeSeconds}s) must be < lockTtl (${lockSecs}s)"
        }
    }
}
