package dev.hononeko.koshin.adapters.nats

import dev.hononeko.koshin.core.model.CredentialScope
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
import io.nats.client.PullSubscribeOptions
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
import kotlin.time.TestTimeSource

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
    fun `publisher sends each job to its scope's subject, ensuring the scope's consumer every minute`() =
        runBlocking(testDispatcher) {
            val mockJs = mockk<JetStream>(relaxed = true)
            val ensured = mutableListOf<CredentialScope>()
            val clock = TestTimeSource()
            val publisher = NatsJobQueuePublisher(
                mockJs,
                ioDispatcher = testDispatcher,
                ensureConsumer = { ensured += it },
                timeSource = clock,
            )
            fun job(id: String, installationId: Long? = null) = RenovateJobEvent(
                RepoRef(Platform.GITHUB, "o/r"),
                id,
                TriggerReason.WEBHOOK,
                installationId = installationId,
            )

            publisher.publish(job("1", installationId = 42))
            publisher.publish(job("2", installationId = 42))
            publisher.publish(job("3"))
            clock += 61.seconds
            publisher.publish(job("4", installationId = 42))

            val inst42 = CredentialScope.Installation(42)
            assertEquals(listOf(inst42, CredentialScope.Token(Platform.GITHUB), inst42), ensured)
            verify(exactly = 3) { mockJs.publish("renovate.jobs.github.inst-42", any<ByteArray>(), any()) }
            verify(exactly = 1) { mockJs.publish("renovate.jobs.github.pat", any<ByteArray>(), any()) }
        }

    @Test
    fun `a job isn't published when its scope's consumer can't be ensured`() = runBlocking(testDispatcher) {
        val mockJs = mockk<JetStream>(relaxed = true)
        val publisher = NatsJobQueuePublisher(
            mockJs,
            ioDispatcher = testDispatcher,
            ensureConsumer = { throw IOException("NATS down") },
        )

        val result = runCatching {
            publisher.publish(RenovateJobEvent(RepoRef(Platform.GITHUB, "o/r"), "d", TriggerReason.WEBHOOK))
        }

        assertEquals("NATS down", result.exceptionOrNull()?.message)
        verify(exactly = 0) { mockJs.publish(any<String>(), any<ByteArray>(), any()) }
    }

    @Test
    fun `a failed fetch binds the scope's subscription again next time`() = runBlocking(testDispatcher) {
        val mockJs = mockk<JetStream>()
        val broken = mockk<io.nats.client.JetStreamSubscription>(relaxed = true) {
            every { fetch(1, any<Duration>()) } throws IllegalStateException("consumer deleted")
        }
        val fresh = mockk<io.nats.client.JetStreamSubscription>(relaxed = true) {
            every { fetch(1, any<Duration>()) } returns emptyList()
        }
        every { mockJs.subscribe("renovate.jobs.github.inst-1", any<PullSubscribeOptions>()) } returnsMany
            listOf(broken, fresh)
        val consumer = NatsJobQueueConsumer(mockJs, ioDispatcher = testDispatcher)
        val scope = CredentialScope.Installation(1)

        val failed = runCatching { consumer.fetchNext(scope, 1.seconds) }
        assertEquals("consumer deleted", failed.exceptionOrNull()?.message)
        assertNull(consumer.fetchNext(scope, 1.seconds))

        verify { broken.unsubscribe() }
        verify(exactly = 2) { mockJs.subscribe(any<String>(), any<PullSubscribeOptions>()) }
        consumer.close()
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
        every { mockConnection.jetStreamManagement() } returns mockk()

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

        val result = consumer.fetchNext(CredentialScope.Token(Platform.GITHUB), 1.seconds)
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

        val result = consumer.fetchNext(CredentialScope.Token(Platform.GITHUB), 1.seconds)
        assertNull(result)

        verify(exactly = 0) { mockMsg.term() }
        verify(exactly = 1) { mockMsg.nak() }

        consumer.close()
    }
}
