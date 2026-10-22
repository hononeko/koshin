package dev.hononeko.koshin.adapters.nats

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.model.TriggerReason
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import io.nats.client.Connection
import io.nats.client.JetStream
import io.nats.client.JetStreamManagement
import io.nats.client.Message
import io.nats.client.api.PublishAck
import io.nats.client.api.StreamConfiguration
import io.nats.client.api.StreamInfo
import io.nats.client.impl.Headers
import io.nats.client.impl.NatsJetStreamMetaData
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.io.IOException
import java.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@OptIn(ExperimentalCoroutinesApi::class)
class NatsComponentUnitTest {
    private val testDispatcher = UnconfinedTestDispatcher()

    @Test
    fun `connection health reports false when disconnected`() {
        val mockConnection = mockk<Connection>()
        every { mockConnection.status } returns Connection.Status.CLOSED

        val health = NatsConnectionHealth(mockConnection)
        assertFalse(health.isConnected())
    }

    @Test
    fun `delivery nack with delay invokes nakWithDelay on message`() = runBlocking(testDispatcher) {
        val mockMsg = mockk<Message>(relaxed = true)
        val event = RenovateJobEvent(
            repo = RepoRef(Platform.GITHUB, "org/repo"),
            deliveryId = "d-1",
            triggerReason = TriggerReason.WEBHOOK,
        )
        val delivery = NatsJobDelivery(mockMsg, event, testDispatcher)

        delivery.nack(500.milliseconds)
        verify { mockMsg.nakWithDelay(Duration.ofMillis(500)) }

        delivery.nack(0.seconds)
        verify { mockMsg.nak() }
    }

    @Test
    fun `publisher publishes to dead letter subject`() = runBlocking(testDispatcher) {
        val mockJs = mockk<JetStream>(relaxed = true)
        val publisher = NatsJobQueuePublisher(mockJs, ioDispatcher = testDispatcher)

        val event = RenovateJobEvent(
            repo = RepoRef(Platform.GITLAB, "gitlab-org/project"),
            deliveryId = "gl-dlq-1",
            triggerReason = TriggerReason.RATE_LIMIT_REQUEUE,
            rateLimitAttempt = 3,
        )

        publisher.publishToDeadLetter(event)

        verify {
            mockJs.publish("renovate.dlq.gitlab", any<ByteArray>(), any())
        }
    }

    @Test
    fun `consumer constructor with connection initializes properly`() {
        val mockConnection = mockk<Connection>()
        val mockJs = mockk<JetStream>()
        every { mockConnection.jetStream() } returns mockJs

        val consumer = NatsJobQueueConsumer(mockConnection)
        assertNotNull(consumer)
        verify { mockConnection.jetStream() }
        consumer.close()
    }

    @Test
    fun `publisher constructor with connection initializes properly`() {
        val mockConnection = mockk<Connection>()
        val mockJs = mockk<JetStream>()
        every { mockConnection.jetStream() } returns mockJs

        val publisher = NatsJobQueuePublisher(mockConnection)
        assertNotNull(publisher)
        verify { mockConnection.jetStream() }
    }

    @Test
    fun `bootstrap constructor with connection initializes properly`() {
        val mockConnection = mockk<Connection>()
        val mockJsm = mockk<JetStreamManagement>()
        every { mockConnection.jetStreamManagement() } returns mockJsm

        val bootstrapper = NatsJetStreamBootstrap(mockConnection)
        assertNotNull(bootstrapper)
        verify { mockConnection.jetStreamManagement() }
    }

    @Test
    fun `bootstrap updates stream with new replica count`() {
        val mockJsm = mockk<JetStreamManagement>(relaxed = true)
        val mockStreamInfo = mockk<StreamInfo>(relaxed = true)
        every { mockJsm.getStreamInfo(any()) } returns mockStreamInfo

        val config = NatsConfig(replicas = 3)
        val bootstrapper = NatsJetStreamBootstrap(mockJsm, config)

        bootstrapper.ensureJobsStream()

        val capturedConfig = slot<StreamConfiguration>()
        verify { mockJsm.updateStream(capture(capturedConfig)) }
        assertEquals(3, capturedConfig.captured.replicas)
    }

    @Test
    fun `consumer publishes poison pill to DLQ and terminates message`() = runBlocking(testDispatcher) {
        val mockJs = mockk<JetStream>()
        val mockSub = mockk<io.nats.client.JetStreamSubscription>()
        val mockMsg = mockk<Message>(relaxed = true)
        val mockMeta = mockk<NatsJetStreamMetaData>()
        val mockAck = mockk<PublishAck>(relaxed = true)

        every { mockMeta.deliveredCount() } returns 1L
        every { mockMeta.streamSequence() } returns 42L
        every { mockMeta.stream } returns "RENOVATE_JOBS"
        val realHeaders = Headers().apply {
            add("Nats-Msg-Id", "github:hononeko/repo:delivery-42")
        }
        every { mockMsg.headers } returns realHeaders
        every { mockMsg.metaData() } returns mockMeta
        every { mockMsg.data } returns "not valid json".toByteArray()
        every { mockSub.fetch(1, any<Duration>()) } returns listOf(mockMsg)
        every { mockJs.subscribe(any<String>(), any<io.nats.client.PullSubscribeOptions>()) } returns mockSub

        val capturedHeaders = slot<Headers>()
        every {
            mockJs.publish("renovate.dlq.github", capture(capturedHeaders), any(), any())
        } returns mockAck

        val consumer = NatsJobQueueConsumer(mockJs, ioDispatcher = testDispatcher)

        val result = consumer.fetchNext(Platform.GITHUB, 1.seconds)
        assertNull(result)

        verify {
            mockJs.publish("renovate.dlq.github", any<Headers>(), any(), any())
            mockMsg.term()
        }
        assertEquals("true", capturedHeaders.captured.getFirst("Koshin-Poison-Pill"))
        assertEquals("42", capturedHeaders.captured.getFirst("Koshin-Stream-Seq"))
        assertEquals("github:hononeko/repo:delivery-42", capturedHeaders.captured.getFirst("Koshin-Original-Msg-Id"))

        consumer.close()
    }

    @Test
    fun `consumer does not terminate and naks poison pill if DLQ publish fails`() = runBlocking(testDispatcher) {
        val mockJs = mockk<JetStream>()
        val mockSub = mockk<io.nats.client.JetStreamSubscription>()
        val mockMsg = mockk<Message>(relaxed = true)
        val mockMeta = mockk<NatsJetStreamMetaData>()

        every { mockMeta.deliveredCount() } returns 1L
        every { mockMeta.streamSequence() } returns 42L
        every { mockMeta.stream } returns "RENOVATE_JOBS"
        every { mockMsg.headers } returns Headers()
        every { mockMsg.metaData() } returns mockMeta
        every { mockMsg.data } returns "not valid json".toByteArray()
        every { mockSub.fetch(1, any<Duration>()) } returns listOf(mockMsg)
        every { mockJs.subscribe(any<String>(), any<io.nats.client.PullSubscribeOptions>()) } returns mockSub

        every {
            mockJs.publish("renovate.dlq.github", any<Headers>(), any(), any())
        } throws IOException("DLQ publish connection dropped")

        val consumer = NatsJobQueueConsumer(mockJs, ioDispatcher = testDispatcher)

        val result = consumer.fetchNext(Platform.GITHUB, 1.seconds)
        assertNull(result)

        verify(exactly = 0) { mockMsg.term() }
        verify(exactly = 1) { mockMsg.nak() }

        consumer.close()
    }
}
