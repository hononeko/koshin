package dev.hononeko.koshin.core.usecase

import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class WorkerConfigTest {

    @Test
    fun `default config is valid`() {
        assertDoesNotThrow {
            WorkerConfig().validate()
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
