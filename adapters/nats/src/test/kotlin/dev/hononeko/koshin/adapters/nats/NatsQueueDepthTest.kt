package dev.hononeko.koshin.adapters.nats

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.ports.QueueDepth
import io.mockk.every
import io.mockk.mockk
import io.nats.client.JetStreamApiException
import io.nats.client.JetStreamManagement
import io.nats.client.api.ConsumerInfo
import io.nats.client.api.Error
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class NatsQueueDepthTest {
    private val config = NatsConfig()
    private val jsm = mockk<JetStreamManagement>()
    private val depth = NatsQueueDepth(jsm, config)

    private class ApiError(private val code: Int) : JetStreamApiException(Error.JsNoMessageFoundErr) {
        override fun getApiErrorCode() = code
    }

    @Test
    fun `reads the platform consumer's pending and ack-pending counts`() = runTest {
        every { jsm.getConsumerInfo(config.jobsStreamName, "koshin-workers-gitlab") } returns mockk<ConsumerInfo> {
            every { numPending } returns 4
            every { numAckPending } returns 2
        }
        assertEquals(QueueDepth(4, 2), depth.depth(Platform.GITLAB))
    }

    @Test
    fun `a missing consumer or stream has no jobs, other errors propagate`() = runTest {
        every { jsm.getConsumerInfo(any(), any()) } throws ApiError(10014) andThenThrows ApiError(10059) andThenThrows
            ApiError(10071)
        assertEquals(QueueDepth(0, 0), depth.depth(Platform.GITHUB))
        assertEquals(QueueDepth(0, 0), depth.depth(Platform.GITHUB))
        assertThrows<JetStreamApiException> { depth.depth(Platform.GITHUB) }
    }
}
