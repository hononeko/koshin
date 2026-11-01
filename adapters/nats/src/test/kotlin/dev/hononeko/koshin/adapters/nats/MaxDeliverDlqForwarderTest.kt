package dev.hononeko.koshin.adapters.nats

import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import io.nats.client.Connection
import io.nats.client.JetStream
import io.nats.client.JetStreamApiException
import io.nats.client.JetStreamManagement
import io.nats.client.Message
import io.nats.client.PublishOptions
import io.nats.client.api.Error
import io.nats.client.api.MessageInfo
import io.nats.client.api.PublishAck
import io.nats.client.impl.Headers
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.IOException

class MaxDeliverDlqForwarderTest {
    private val config = NatsConfig()
    private val jsm = mockk<JetStreamManagement>()
    private val js = mockk<JetStream>()
    private val forwarder = MaxDeliverDlqForwarder(mockk<Connection>(), config, jsm, js, autoStart = false)
    private val headers = slot<Headers>()

    private class ApiError(private val code: Int) : JetStreamApiException(Error.JsNoMessageFoundErr) {
        override fun getApiErrorCode() = code
    }

    private fun stored(seq: Long) {
        every { jsm.getMessage(config.jobsStreamName, seq) } returns mockk<MessageInfo> {
            every { isMessage } returns true
            every { subject } returns "renovate.jobs.github"
            every { data } returns "{}".toByteArray()
            every { this@mockk.headers } returns Headers().put("Nats-Msg-Id", "github:o/r:d-1")
        }
        every { js.publish("renovate.dlq.github", capture(headers), any<ByteArray>(), any<PublishOptions>()) } returns
            mockk<PublishAck> { every { isDuplicate } returns false }
        every { jsm.deleteMessage(config.jobsStreamName, seq) } returns true
    }

    private fun advisory(json: String) = mockk<Message> { every { data } returns json.toByteArray() }

    @Test
    fun `moves a stored job to the DLQ and deletes it`() {
        stored(7)

        assertTrue(forwarder.moveToDeadLetter(7, 5, MaxDeliverDlqForwarder.SOURCE_SWEEP))

        assertEquals("sweep", headers.captured.getFirst("Koshin-Dlq-Source"))
        assertEquals("5", headers.captured.getFirst("Koshin-Deliveries"))
        assertEquals("7", headers.captured.getFirst("Koshin-Stream-Seq"))
        assertEquals("github:o/r:d-1", headers.captured.getFirst("Koshin-Original-Msg-Id"))
        verify { jsm.deleteMessage(config.jobsStreamName, 7) }
    }

    @Test
    fun `a job that's already gone is left alone`() {
        every { jsm.getMessage(config.jobsStreamName, 3) } throws ApiError(10037)

        assertFalse(forwarder.moveToDeadLetter(3, 5, MaxDeliverDlqForwarder.SOURCE_SWEEP))

        verify(exactly = 0) { js.publish(any<String>(), any<Headers>(), any<ByteArray>(), any<PublishOptions>()) }
    }

    @Test
    fun `gives up after the retries and rethrows`() {
        stored(4)
        every { js.publish(any<String>(), any<Headers>(), any<ByteArray>(), any<PublishOptions>()) } throws
            IOException("timeout")

        assertThrows<IOException> { forwarder.moveToDeadLetter(4, 5, MaxDeliverDlqForwarder.SOURCE_SWEEP, 2) }
        verify(exactly = 2) { js.publish(any<String>(), any<Headers>(), any<ByteArray>(), any<PublishOptions>()) }
    }

    @Test
    fun `an advisory moves its job with the advisory as source`() {
        stored(9)

        forwarder.handleAdvisory(advisory("""{"stream":"RENOVATE_JOBS","stream_seq":9,"deliveries":5}"""))
        forwarder.handleAdvisory(advisory("""{"stream":"OTHER","stream_seq":9}"""))
        forwarder.handleAdvisory(advisory("not json"))

        assertEquals("advisory", headers.captured.getFirst("Koshin-Dlq-Source"))
        assertEquals(1, forwarder.handledAdvisoriesCount.get())
    }
}
