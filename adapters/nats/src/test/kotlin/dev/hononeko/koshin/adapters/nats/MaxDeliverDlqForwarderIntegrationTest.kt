package dev.hononeko.koshin.adapters.nats

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.model.TriggerReason
import io.nats.client.Connection
import io.nats.client.JetStreamManagement
import io.nats.client.Nats
import io.nats.client.Options
import io.nats.client.PullSubscribeOptions
import io.nats.client.api.StorageType
import io.nats.client.impl.NatsMessage
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class MaxDeliverDlqForwarderIntegrationTest {
    companion object {
        private lateinit var connection1: Connection
        private lateinit var connection2: Connection

        @BeforeAll
        @JvmStatic
        fun setUp() {
            connection1 = Nats.connect(Options.Builder().server(SharedNatsContainer.natsUrl).build())
            connection2 = Nats.connect(Options.Builder().server(SharedNatsContainer.natsUrl).build())
        }

        @AfterAll
        @JvmStatic
        fun tearDown() {
            connection1.close()
            connection2.close()
        }
    }

    @Test
    fun `max deliver exhaustion with two forwarders creates exactly one DLQ entry and evicts from work queue`() =
        runBlocking {
            val jobsStream = "FORWARDER_JOBS"
            val dlqStream = "FORWARDER_DLQ"
            val config = NatsConfig(
                jobsStreamName = jobsStream,
                dlqStreamName = dlqStream,
                jobsSubjectPrefix = "forwarder.jobs",
                dlqSubjectPrefix = "forwarder.dlq",
                storageType = StorageType.Memory,
                ackWait = 1.seconds,
                maxDeliver = 2,
            )

            val jsm = connection1.jetStreamManagement()
            NatsJetStreamBootstrap(jsm, config).bootstrap(listOf(Platform.GITHUB))

            val forwarder1 = MaxDeliverDlqForwarder(connection1, config)
            val forwarder2 = MaxDeliverDlqForwarder(connection2, config)
            val publisher = NatsJobQueuePublisher(connection1, config)
            val consumer = NatsJobQueueConsumer(connection1, config)

            exhaustJobDeliveries(publisher, consumer)
            verifyDlqForwarding(dlqStream, jobsStream, jsm)
            verifyRedeliveryIgnored(jobsStream, dlqStream, jsm)

            forwarder1.close()
            forwarder2.close()
            consumer.close()
        }

    private suspend fun exhaustJobDeliveries(publisher: NatsJobQueuePublisher, consumer: NatsJobQueueConsumer) {
        val event = RenovateJobEvent(
            repo = RepoRef(Platform.GITHUB, "hononeko/failing-job"),
            deliveryId = "fail-delivery-123",
            triggerReason = TriggerReason.WEBHOOK,
        )
        publisher.publish(event)

        val d1 = consumer.fetchNext(Platform.GITHUB, 2.seconds)
        assertNotNull(d1)
        assertEquals(1, d1?.deliveryAttempt)
        d1?.nack()

        val d2 = consumer.fetchNext(Platform.GITHUB, 2.seconds)
        assertNotNull(d2)
        assertEquals(2, d2?.deliveryAttempt)
        d2?.nack()

        assertNull(consumer.fetchNext(Platform.GITHUB, 500.milliseconds))
    }

    private suspend fun verifyDlqForwarding(dlqStream: String, jobsStream: String, jsm: JetStreamManagement) {
        val dlqSub = connection1.jetStream().subscribe(
            "forwarder.dlq.github",
            PullSubscribeOptions.builder().stream(dlqStream).build(),
        )

        var dlqMessages = dlqSub.fetch(10, java.time.Duration.ofSeconds(4))
        val start = System.currentTimeMillis()
        while (dlqMessages.isEmpty() && System.currentTimeMillis() - start < 5000) {
            delay(100.milliseconds)
            dlqMessages = dlqSub.fetch(10, java.time.Duration.ofMillis(500))
        }

        assertEquals(1, dlqMessages.size)
        val dlqMsg = dlqMessages[0]
        val receivedEvent = Json.decodeFromString(
            RenovateJobEvent.serializer(),
            dlqMsg.data.decodeToString(),
        )
        assertEquals("hononeko/failing-job", receivedEvent.repo.repository)
        assertEquals("fail-delivery-123", receivedEvent.deliveryId)
        assertEquals("dlq:$jobsStream:1", dlqMsg.headers?.getFirst("Nats-Msg-Id"))
        assertEquals(0L, jsm.getStreamInfo(jobsStream).streamState.msgCount)
    }

    private suspend fun verifyRedeliveryIgnored(jobsStream: String, dlqStream: String, jsm: JetStreamManagement) {
        val advisoryJson = """
        {
            "type": "io.nats.jetstream.advisory.v1.max_deliver",
            "stream": "$jobsStream",
            "consumer": "koshin-workers-github",
            "stream_seq": 1,
            "deliveries": 2
        }
        """.trimIndent()
        connection1.publish(
            "\$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.$jobsStream.koshin-workers-github",
            advisoryJson.toByteArray(),
        )

        delay(500.milliseconds)
        assertEquals(1L, jsm.getStreamInfo(dlqStream).streamState.msgCount)
    }

    @Test
    fun `forwarder ignores invalid or unrelated advisories`() {
        val config = NatsConfig(
            jobsStreamName = "IGNORE_TEST_JOBS",
            dlqStreamName = "IGNORE_TEST_DLQ",
            storageType = StorageType.Memory,
        )
        val forwarder = MaxDeliverDlqForwarder(connection1, config, autoStart = false)

        val invalidMsg = NatsMessage.builder()
            .subject("\$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.IGNORE_TEST_JOBS.foo")
            .data("not-json".toByteArray())
            .build()
        forwarder.handleAdvisory(invalidMsg)

        val unrelatedMsg = NatsMessage.builder()
            .subject("\$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.UNRELATED_STREAM.foo")
            .data("""{"stream": "OTHER_STREAM", "stream_seq": 99}""".toByteArray())
            .build()
        forwarder.handleAdvisory(unrelatedMsg)

        forwarder.close()
    }
}
