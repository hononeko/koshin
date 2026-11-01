package dev.hononeko.koshin.adapters.nats

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.time.Duration.Companion.days
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
        assertEquals(300.seconds, config.exhaustedSweepInterval)
        assertEquals(1.days, config.dlqDuplicateWindow)
    }

    @Test
    fun `fromEnv reads environment configuration`() {
        val env = mapOf(
            "NATS_STREAM_REPLICAS" to "3",
            "NATS_ACK_WAIT_SECONDS" to "120",
            "NATS_MAX_DELIVER" to "10",
            "NATS_JOBS_STREAM" to "CUSTOM_JOBS",
            "NATS_DLQ_STREAM" to "CUSTOM_DLQ",
            "NATS_STORAGE_TYPE" to "file",
            "NATS_EXHAUSTED_SWEEP_SECONDS" to "0",
        )
        val config = NatsConfig.fromEnv(env)
        assertEquals("CUSTOM_JOBS", config.jobsStreamName)
        assertEquals("CUSTOM_DLQ", config.dlqStreamName)
        assertEquals(3, config.replicas)
        assertEquals(120.seconds, config.ackWait)
        assertEquals(10L, config.maxDeliver)
        assertEquals(io.nats.client.api.StorageType.File, config.storageType)
        assertEquals(0.seconds, config.exhaustedSweepInterval)

        val streamNameConfig = NatsConfig.fromEnv(mapOf("NATS_STREAM_NAME" to "PRIMARY_JOBS_STREAM"))
        assertEquals("PRIMARY_JOBS_STREAM", streamNameConfig.jobsStreamName)
    }

    @Test
    fun `fromEnv rejects invalid parameters at startup`() {
        assertThrows<IllegalArgumentException> {
            NatsConfig.fromEnv(mapOf("NATS_STREAM_REPLICAS" to "three"))
        }
        assertThrows<IllegalArgumentException> {
            NatsConfig.fromEnv(mapOf("NATS_ACK_WAIT_SECONDS" to "invalid"))
        }
        assertThrows<IllegalArgumentException> {
            NatsConfig.fromEnv(mapOf("NATS_STORAGE_TYPE" to "invalid_type"))
        }
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
            NatsConfig(exhaustedSweepInterval = (-1).seconds)
        }
        assertThrows<IllegalArgumentException> {
            NatsConfig(dlqDuplicateWindow = 8.days)
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
