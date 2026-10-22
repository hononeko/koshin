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
        validate()
    }

    fun validate() {
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
        require(maxRateLimitRequeues >= 0) {
            "MAX_RATE_LIMIT_REQUEUES ($maxRateLimitRequeues) must be >= 0"
        }
    }

    companion object {
        fun validate(config: WorkerConfig) {
            config.validate()
        }
    }
}
