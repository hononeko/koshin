package dev.hononeko.koshin.adapters.nats

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.model.TriggerReason
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.nats.client.Connection
import io.nats.client.JetStream
import io.nats.client.JetStreamManagement
import io.nats.client.Message
import io.nats.client.impl.NatsJetStreamMetaData
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.serialization.SerializationException
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
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
            mockJs.publish("renovate.dlq.gitlab", any(), any(), any())
        }
    }

    @Test
    fun `consumer constructor with connection initializes properly`() {
        val mockConnection = mockk<Connection>()
        val mockJs = mockk<JetStream>()
        every { mockConnection.jetStream() } returns mockJs

        val consumer = NatsJobQueueConsumer(mockConnection)
        consumer.close()
    }

    @Test
    fun `publisher constructor with connection initializes properly`() {
        val mockConnection = mockk<Connection>()
        val mockJs = mockk<JetStream>()
        every { mockConnection.jetStream() } returns mockJs

        NatsJobQueuePublisher(mockConnection)
    }

    @Test
    fun `bootstrap constructor with connection initializes properly`() {
        val mockConnection = mockk<Connection>()
        val mockJsm = mockk<JetStreamManagement>()
        every { mockConnection.jetStreamManagement() } returns mockJsm

        NatsJetStreamBootstrap(mockConnection)
    }

    @Test
    fun `consumer throws on malformed message payload`() = runBlocking(testDispatcher) {
        val mockJs = mockk<JetStream>()
        val mockSub = mockk<io.nats.client.JetStreamSubscription>()
        val mockMsg = mockk<Message>()
        val mockMeta = mockk<NatsJetStreamMetaData>()

        every { mockMeta.deliveredCount() } returns 1L
        every { mockMsg.metaData() } returns mockMeta
        every { mockMsg.data } returns "not valid json".toByteArray()
        every { mockSub.fetch(1, any<Duration>()) } returns listOf(mockMsg)
        every { mockJs.subscribe(any<String>(), any<io.nats.client.PullSubscribeOptions>()) } returns mockSub

        val consumer = NatsJobQueueConsumer(mockJs, ioDispatcher = testDispatcher)

        assertThrows<SerializationException> {
            consumer.fetchNext(Platform.GITHUB, 1.seconds)
        }

        consumer.close()
    }
}
