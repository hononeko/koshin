package dev.hononeko.koshin.adapters.nats

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.time.Duration.Companion.seconds

class NatsConfigTest {
    @Test
    fun `default config has expected values`() {
        val config = NatsConfig()
        assertEquals("RENOVATE_JOBS", config.jobsStreamName)
        assertEquals("RENOVATE_DLQ", config.dlqStreamName)
        assertEquals(1, config.replicas)
        assertEquals(60.seconds, config.ackWait)
        assertEquals(5L, config.maxDeliver)
    }

    @Test
    fun `rejects invalid parameters`() {
        assertThrows<IllegalArgumentException> {
            NatsConfig(jobsStreamName = " ")
        }
        assertThrows<IllegalArgumentException> {
            NatsConfig(dlqStreamName = " ")
        }
        assertThrows<IllegalArgumentException> {
            NatsConfig(replicas = 0)
        }
        assertThrows<IllegalArgumentException> {
            NatsConfig(maxDeliver = 0)
        }
        assertThrows<IllegalArgumentException> {
            NatsConfig(ackWait = 0.seconds)
        }
        assertThrows<IllegalArgumentException> {
            NatsConfig(duplicateWindow = 0.seconds)
        }
        assertThrows<IllegalArgumentException> {
            NatsConfig(dlqMaxAge = 0.seconds)
        }
        assertThrows<IllegalArgumentException> {
            NatsConfig(dlqForwarderQueueGroup = " ")
        }
        assertThrows<IllegalArgumentException> {
            NatsConfig(consumerPrefix = " ")
        }
    }
}
