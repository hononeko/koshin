package dev.hononeko.koshin.core.usecase

import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class WorkerConfigTest {

    @Test
    fun `default config is valid`() {
        assertDoesNotThrow {
            WorkerConfig()
        }
    }

    @Test
    fun `durations must be strictly positive`() {
        assertThrows(IllegalArgumentException::class.java) { WorkerConfig(natsAckWait = Duration.ZERO) }
        assertThrows(IllegalArgumentException::class.java) { WorkerConfig(natsAckWait = (-1).seconds) }
        assertThrows(IllegalArgumentException::class.java) { WorkerConfig(lockTtl = Duration.ZERO) }
        assertThrows(IllegalArgumentException::class.java) { WorkerConfig(dirtyTtl = Duration.ZERO) }
        assertThrows(IllegalArgumentException::class.java) { WorkerConfig(cliTimeout = Duration.ZERO) }
        assertThrows(IllegalArgumentException::class.java) { WorkerConfig(heartbeatInterval = Duration.ZERO) }
        assertThrows(IllegalArgumentException::class.java) { WorkerConfig(lockCollisionRetryDelay = Duration.ZERO) }
        assertThrows(IllegalArgumentException::class.java) { WorkerConfig(fetchTimeout = Duration.ZERO) }
        assertThrows(IllegalArgumentException::class.java) { WorkerConfig(infraErrorBackoffBase = Duration.ZERO) }
        assertThrows(IllegalArgumentException::class.java) {
            WorkerConfig(
                infraErrorBackoffBase = 10.seconds,
                infraErrorBackoffMax = 5.seconds,
            )
        }
    }

    @Test
    fun `fetchTimeout must be strictly less than lockTtl`() {
        assertThrows(IllegalArgumentException::class.java) {
            WorkerConfig(
                lockTtl = 5.seconds,
                fetchTimeout = 5.seconds,
                heartbeatInterval = 1.seconds,
                natsAckWait = 10.seconds,
            )
        }
        assertThrows(IllegalArgumentException::class.java) {
            WorkerConfig(
                lockTtl = 5.seconds,
                fetchTimeout = 6.seconds,
                heartbeatInterval = 1.seconds,
                natsAckWait = 10.seconds,
            )
        }
    }

    @Test
    fun `natsAckWait must be strictly greater than lockTtl`() {
        assertThrows(IllegalArgumentException::class.java) {
            WorkerConfig(
                natsAckWait = 30.seconds,
                lockTtl = 45.seconds,
            )
        }
        assertThrows(IllegalArgumentException::class.java) {
            WorkerConfig(
                natsAckWait = 45.seconds,
                lockTtl = 45.seconds,
            )
        }
    }

    @Test
    fun `dirtyTtl must be strictly greater than cliTimeout`() {
        assertThrows(IllegalArgumentException::class.java) {
            WorkerConfig(
                dirtyTtl = 30.minutes,
                cliTimeout = 45.minutes,
            )
        }
        assertThrows(IllegalArgumentException::class.java) {
            WorkerConfig(
                dirtyTtl = 45.minutes,
                cliTimeout = 45.minutes,
            )
        }
    }

    @Test
    fun `heartbeatInterval must be strictly less than half of lockTtl`() {
        assertThrows(IllegalArgumentException::class.java) {
            WorkerConfig(
                lockTtl = 40.seconds,
                heartbeatInterval = 20.seconds,
            )
        }
        assertThrows(IllegalArgumentException::class.java) {
            WorkerConfig(
                lockTtl = 40.seconds,
                heartbeatInterval = 25.seconds,
            )
        }
    }

    @Test
    fun `maxRateLimitRequeues must be non-negative`() {
        assertThrows(IllegalArgumentException::class.java) {
            WorkerConfig(maxRateLimitRequeues = -1)
        }
        assertDoesNotThrow {
            WorkerConfig(maxRateLimitRequeues = 0)
        }
    }
}
