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
    fun `a job another replica deleted first counts as moved, without retrying the delete`() {
        stored(7)
        every { jsm.deleteMessage(config.jobsStreamName, 7) } throws ApiError(10043)

        assertTrue(forwarder.moveToDeadLetter(7, 5, MaxDeliverDlqForwarder.SOURCE_SWEEP))

        verify(exactly = 1) { jsm.deleteMessage(config.jobsStreamName, 7) }
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
        forwarder.handleAdvisory(advisory("""{"stream":"RENOVATE_JOBS","stream_seq":0}"""))
        forwarder.handleAdvisory(advisory("not json"))

        assertEquals("advisory", headers.captured.getFirst("Koshin-Dlq-Source"))
        assertEquals(1, forwarder.handledAdvisoriesCount.get())
    }

    @Test
    fun `a message with null data or non-message is ignored`() {
        every { jsm.getMessage(config.jobsStreamName, 12) } returns mockk<MessageInfo> {
            every { isMessage } returns false
            every { data } returns null
        }
        assertFalse(forwarder.moveToDeadLetter(12, 5, MaxDeliverDlqForwarder.SOURCE_SWEEP))

        every { jsm.getMessage(config.jobsStreamName, 13) } returns mockk<MessageInfo> {
            every { isMessage } returns true
            every { data } returns null
        }
        assertFalse(forwarder.moveToDeadLetter(13, 5, MaxDeliverDlqForwarder.SOURCE_SWEEP))
    }

    @Test
    fun `a message from non-standard subject and invalid json falls back to unknown platform`() {
        every { jsm.getMessage(config.jobsStreamName, 15) } returns mockk<MessageInfo> {
            every { isMessage } returns true
            every { subject } returns "custom.unprefixed.subject"
            every { data } returns "not-json-payload".toByteArray()
            every { this@mockk.headers } returns null
        }
        every { js.publish("renovate.dlq.unknown", capture(headers), any<ByteArray>(), any<PublishOptions>()) } returns
            mockk<PublishAck> { every { isDuplicate } returns false }
        every { jsm.deleteMessage(config.jobsStreamName, 15) } returns true

        assertTrue(forwarder.moveToDeadLetter(15, 5, MaxDeliverDlqForwarder.SOURCE_SWEEP))
    }

    @Test
    fun `reports the dead-lettered job, and a failing report doesn't fail the move`() {
        val event = """{"repo":{"platform":"GITLAB","repository":"group/project"},"deliveryId":"d-1",""" +
            """"triggerReason":"WEBHOOK"}"""
        every { jsm.getMessage(config.jobsStreamName, 9) } returns mockk<MessageInfo> {
            every { isMessage } returns true
            // Not a jobs subject: the platform comes from the payload
            every { subject } returns "elsewhere"
            every { data } returns event.toByteArray()
            every { this@mockk.headers } returns null
        }
        every { js.publish("renovate.dlq.gitlab", any<Headers>(), any<ByteArray>(), any<PublishOptions>()) } returns
            mockk<PublishAck> { every { isDuplicate } returns true }
        every { jsm.deleteMessage(config.jobsStreamName, 9) } returns true
        val reported = mutableListOf<String>()
        val reporting = MaxDeliverDlqForwarder(mockk<Connection>(), config, jsm, js, autoStart = false) {
            reported += it.repo.toString()
            error("Valkey down")
        }

        assertTrue(reporting.moveToDeadLetter(9, 5, MaxDeliverDlqForwarder.SOURCE_SWEEP))

        assertEquals(listOf("gitlab:group/project"), reported)
        verify { jsm.deleteMessage(config.jobsStreamName, 9) }
    }

    @Test
    fun `moveToDeadLetter with zero deliveries omits deliveries header`() {
        stored(20)

        assertTrue(forwarder.moveToDeadLetter(20, 0, MaxDeliverDlqForwarder.SOURCE_SWEEP))

        assertEquals("sweep", headers.captured.getFirst("Koshin-Dlq-Source"))
        org.junit.jupiter.api.Assertions.assertNull(headers.captured.getFirst("Koshin-Deliveries"))
        assertEquals("20", headers.captured.getFirst("Koshin-Stream-Seq"))
    }

    @Test
    fun `fetch message throws non-404 JetStreamApiException`() {
        every { jsm.getMessage(config.jobsStreamName, 21) } throws ApiError(10050)

        assertThrows<JetStreamApiException> {
            forwarder.moveToDeadLetter(21, 1, MaxDeliverDlqForwarder.SOURCE_SWEEP)
        }
    }

    @Test
    fun `delete retry handles already deleted code and exhausts retries`() {
        stored(22)
        every { jsm.deleteMessage(config.jobsStreamName, 22) } throws ApiError(10019)
        assertTrue(forwarder.moveToDeadLetter(22, 1, MaxDeliverDlqForwarder.SOURCE_SWEEP))

        stored(23)
        every { jsm.deleteMessage(config.jobsStreamName, 23) } throws ApiError(10001)
        assertTrue(forwarder.moveToDeadLetter(23, 1, MaxDeliverDlqForwarder.SOURCE_SWEEP, maxRetries = 1))

        stored(24)
        every { jsm.deleteMessage(config.jobsStreamName, 24) } throws RuntimeException("generic failure")
        assertTrue(forwarder.moveToDeadLetter(24, 1, MaxDeliverDlqForwarder.SOURCE_SWEEP, maxRetries = 1))
    }

    @Test
    fun `forwarder lifecycle and running status`() {
        assertFalse(forwarder.isRunning)
        forwarder.close()
        assertFalse(forwarder.isRunning)
    }
}
