package dev.hononeko.koshin.adapters.nats

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.model.TriggerReason
import io.nats.client.Connection
import io.nats.client.Nats
import io.nats.client.Options
import io.nats.client.api.StorageType
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class NatsJobQueueIntegrationTest {
    companion object {
        private lateinit var connection: Connection

        @BeforeAll
        @JvmStatic
        fun setUp() {
            connection = Nats.connect(Options.Builder().server(SharedNatsContainer.natsUrl).build())
        }

        @AfterAll
        @JvmStatic
        fun tearDown() {
            connection.close()
        }
    }

    @Test
    fun `connection health check returns true when connected`() {
        assertTrue(connection.isConnected())
        val health = NatsConnectionHealth(connection)
        assertTrue(health.isConnected())
    }

    @Test
    fun `per-platform consumers only receive their platform jobs`() = runBlocking {
        val jobsStream = "PLATFORM_ISOLATION_JOBS"
        val dlqStream = "PLATFORM_ISOLATION_DLQ"
        val config = NatsConfig(
            jobsStreamName = jobsStream,
            dlqStreamName = dlqStream,
            jobsSubjectPrefix = "isolation.jobs",
            dlqSubjectPrefix = "isolation.dlq",
            storageType = StorageType.Memory,
            ackWait = 30.seconds,
        )

        val jsm = connection.jetStreamManagement()
        val bootstrap = NatsJetStreamBootstrap(jsm, config)
        bootstrap.bootstrap(listOf(Platform.GITHUB, Platform.GITLAB))

        val publisher = NatsJobQueuePublisher(connection, config)
        val consumer = NatsJobQueueConsumer(connection, config)

        val ghEvent = RenovateJobEvent(
            repo = RepoRef(Platform.GITHUB, "hononeko/gh-repo"),
            deliveryId = "gh-1",
            triggerReason = TriggerReason.WEBHOOK,
        )
        val glEvent = RenovateJobEvent(
            repo = RepoRef(Platform.GITLAB, "hononeko/gl-repo"),
            deliveryId = "gl-1",
            triggerReason = TriggerReason.WEBHOOK,
        )

        publisher.publish(ghEvent)
        publisher.publish(glEvent)

        val receivedGh = consumer.fetchNext(Platform.GITHUB, 2.seconds)
        assertNotNull(receivedGh)
        assertEquals("hononeko/gh-repo", receivedGh?.event?.repo?.repository)
        assertEquals(Platform.GITHUB, receivedGh?.event?.repo?.platform)
        receivedGh?.ack()

        val receivedGl = consumer.fetchNext(Platform.GITLAB, 2.seconds)
        assertNotNull(receivedGl)
        assertEquals("hononeko/gl-repo", receivedGl?.event?.repo?.repository)
        assertEquals(Platform.GITLAB, receivedGl?.event?.repo?.platform)
        receivedGl?.ack()

        // No more messages
        assertNull(consumer.fetchNext(Platform.GITHUB, 200.milliseconds))
        assertNull(consumer.fetchNext(Platform.GITLAB, 200.milliseconds))

        consumer.close()
    }

    @Test
    fun `ack removes message from WorkQueue stream`() = runBlocking {
        val jobsStream = "ACK_TEST_JOBS"
        val dlqStream = "ACK_TEST_DLQ"
        val config = NatsConfig(
            jobsStreamName = jobsStream,
            dlqStreamName = dlqStream,
            jobsSubjectPrefix = "ack.jobs",
            dlqSubjectPrefix = "ack.dlq",
            storageType = StorageType.Memory,
            ackWait = 30.seconds,
        )

        val jsm = connection.jetStreamManagement()
        val bootstrap = NatsJetStreamBootstrap(jsm, config)
        bootstrap.bootstrap(listOf(Platform.GITHUB))

        val publisher = NatsJobQueuePublisher(connection, config)
        val consumer = NatsJobQueueConsumer(connection, config)

        val event = RenovateJobEvent(
            repo = RepoRef(Platform.GITHUB, "hononeko/ack-repo"),
            deliveryId = "ack-1",
            triggerReason = TriggerReason.WEBHOOK,
        )
        publisher.publish(event)

        assertEquals(1L, jsm.getStreamInfo(jobsStream).streamState.msgCount)

        val delivery = consumer.fetchNext(Platform.GITHUB, 2.seconds)
        assertNotNull(delivery)
        delivery?.ack()

        assertEquals(0L, jsm.getStreamInfo(jobsStream).streamState.msgCount)
        assertNull(consumer.fetchNext(Platform.GITHUB, 200.milliseconds))

        consumer.close()
    }

    @Test
    fun `deduplication window keeps different deliveryIds and deduplicates identical deliveryIds`() = runBlocking {
        val jobsStream = "DEDUP_TEST_JOBS"
        val dlqStream = "DEDUP_TEST_DLQ"
        val config = NatsConfig(
            jobsStreamName = jobsStream,
            dlqStreamName = dlqStream,
            jobsSubjectPrefix = "dedup.jobs",
            dlqSubjectPrefix = "dedup.dlq",
            storageType = StorageType.Memory,
            duplicateWindow = 2.minutes,
            ackWait = 30.seconds,
        )

        val jsm = connection.jetStreamManagement()
        val bootstrap = NatsJetStreamBootstrap(jsm, config)
        bootstrap.bootstrap(listOf(Platform.GITHUB))

        val publisher = NatsJobQueuePublisher(connection, config)

        val event1 = RenovateJobEvent(
            repo = RepoRef(Platform.GITHUB, "hononeko/dedup-repo"),
            deliveryId = "delivery-1",
            triggerReason = TriggerReason.WEBHOOK,
        )
        val event2 = RenovateJobEvent(
            repo = RepoRef(Platform.GITHUB, "hononeko/dedup-repo"),
            deliveryId = "delivery-2",
            triggerReason = TriggerReason.WEBHOOK,
        )
        val event1Duplicate = RenovateJobEvent(
            repo = RepoRef(Platform.GITHUB, "hononeko/dedup-repo"),
            deliveryId = "delivery-1",
            triggerReason = TriggerReason.WEBHOOK,
        )

        publisher.publish(event1)
        publisher.publish(event2)
        publisher.publish(event1Duplicate)

        // Only 2 messages should be in the stream
        val streamInfo = jsm.getStreamInfo(jobsStream)
        assertEquals(2L, streamInfo.streamState.msgCount)
    }

    @Test
    fun `inProgress resets AckWait timer and prevents premature redelivery`() = runBlocking {
        val jobsStream = "HEARTBEAT_TEST_JOBS"
        val dlqStream = "HEARTBEAT_TEST_DLQ"
        val config = NatsConfig(
            jobsStreamName = jobsStream,
            dlqStreamName = dlqStream,
            jobsSubjectPrefix = "heartbeat.jobs",
            dlqSubjectPrefix = "heartbeat.dlq",
            storageType = StorageType.Memory,
            ackWait = 3.seconds,
        )

        val jsm = connection.jetStreamManagement()
        val bootstrap = NatsJetStreamBootstrap(jsm, config)
        bootstrap.bootstrap(listOf(Platform.GITHUB))

        val publisher = NatsJobQueuePublisher(connection, config)
        val consumer = NatsJobQueueConsumer(connection, config)

        val event = RenovateJobEvent(
            repo = RepoRef(Platform.GITHUB, "hononeko/heartbeat-repo"),
            deliveryId = "hb-1",
            triggerReason = TriggerReason.WEBHOOK,
        )
        publisher.publish(event)

        val delivery = consumer.fetchNext(Platform.GITHUB, 3.seconds)
        assertNotNull(delivery)
        assertEquals(1, delivery?.deliveryAttempt)

        // 1. Wait 1.5s, then call heartbeat()
        delay(1500.milliseconds)
        delivery?.heartbeat()

        // 2. Wait another 2.0s (total 3.5s > 3.0s initial AckWait, but only 2.0s since heartbeat).
        // Since heartbeat reset the timer, a fetch attempt should NOT see redelivery yet.
        delay(2000.milliseconds)
        val earlyAttempt = consumer.fetchNext(Platform.GITHUB, 300.milliseconds)
        assertNull(earlyAttempt)

        // 3. Now let AckWait expire without heartbeat (wait 2.0s more, total 4.0s since heartbeat > 3.0s AckWait)
        delay(2000.milliseconds)

        val redelivered = consumer.fetchNext(Platform.GITHUB, 3.seconds)
        assertNotNull(redelivered)
        assertEquals(2, redelivered?.deliveryAttempt)
        redelivered?.ack()

        consumer.close()
    }

    @Test
    fun `fetchNext with non-positive timeout returns null immediately`() = runBlocking {
        val config = NatsConfig(storageType = StorageType.Memory)
        val consumer = NatsJobQueueConsumer(connection, config)

        assertNull(consumer.fetchNext(Platform.GITHUB, 0.seconds))
        assertNull(consumer.fetchNext(Platform.GITHUB, (-5).seconds))

        consumer.close()
    }
}
