package dev.hononeko.koshin.adapters.nats

import dev.hononeko.koshin.core.model.CredentialScope
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.model.TriggerReason
import dev.hononeko.koshin.core.ports.QueueDepth
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
    private val github = CredentialScope.Token(Platform.GITHUB)
    private val gitlab = CredentialScope.Token(Platform.GITLAB)
    private val inst1 = CredentialScope.Installation(1)
    private val inst2 = CredentialScope.Installation(2)

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
    fun `each credential scope's consumer only receives its own jobs`() = runBlocking {
        val config = NatsConfig(
            jobsStreamName = "SCOPE_ISOLATION_JOBS",
            dlqStreamName = "SCOPE_ISOLATION_DLQ",
            jobsSubjectPrefix = "isolation.jobs",
            dlqSubjectPrefix = "isolation.dlq",
            storageType = StorageType.Memory,
            ackWait = 30.seconds,
        )
        NatsJetStreamBootstrap(connection.jetStreamManagement(), config).bootstrap()
        val publisher = NatsJobQueuePublisher(connection, config)
        val consumer = NatsJobQueueConsumer(connection, config)

        fun event(platform: Platform, repo: String, installationId: Long? = null) =
            RenovateJobEvent(RepoRef(platform, repo), "$repo-1", TriggerReason.WEBHOOK, installationId = installationId)
        val events = mapOf(
            github to event(Platform.GITHUB, "hononeko/pat-repo"),
            gitlab to event(Platform.GITLAB, "hononeko/gl-repo"),
            inst1 to event(Platform.GITHUB, "one/repo", installationId = 1),
            inst2 to event(Platform.GITHUB, "two/repo", installationId = 2),
        )
        events.values.forEach { publisher.publish(it) }

        events.forEach { (scope, event) ->
            val received = checkNotNull(consumer.fetchNext(scope, 2.seconds)) { "nothing for $scope" }
            assertEquals(event, received.event)
            received.ack()
            assertNull(consumer.fetchNext(scope, 200.milliseconds), "only one job for $scope")
        }
        consumer.close()
    }

    @Test
    fun `publishing creates the scope's consumer, whose depth is listed by scope`() = runBlocking {
        val config = NatsConfig(
            jobsStreamName = "SCOPE_DEPTH_JOBS",
            dlqStreamName = "SCOPE_DEPTH_DLQ",
            jobsSubjectPrefix = "scopedepth.jobs",
            dlqSubjectPrefix = "scopedepth.dlq",
            storageType = StorageType.Memory,
        )
        val jsm = connection.jetStreamManagement()
        NatsJetStreamBootstrap(jsm, config).bootstrap()
        val depth = NatsQueueDepth(jsm, config)
        assertEquals(emptyMap<CredentialScope, QueueDepth>(), depth.depths(Platform.entries))

        val publisher = NatsJobQueuePublisher(connection, config)
        repeat(3) { i ->
            publisher.publish(
                RenovateJobEvent(RepoRef(Platform.GITHUB, "o/r$i"), "d$i", TriggerReason.WEBHOOK, installationId = 42),
            )
        }
        publisher.publish(RenovateJobEvent(RepoRef(Platform.GITLAB, "g/p"), "d", TriggerReason.WEBHOOK))

        val info = jsm.getConsumerInfo(config.jobsStreamName, "koshin-workers-github-inst-42")
        assertEquals("scopedepth.jobs.github.inst-42", info.consumerConfiguration.filterSubject)
        assertEquals(
            mapOf(CredentialScope.Installation(42) to QueueDepth(3, 0), gitlab to QueueDepth(1, 0)),
            depth.depths(Platform.entries),
        )
        assertEquals(setOf(gitlab), depth.depths(listOf(Platform.GITLAB)).keys)
        val noStream = NatsQueueDepth(jsm, NatsConfig(jobsStreamName = "NONE"))
        assertEquals(emptyMap<CredentialScope, QueueDepth>(), noStream.depths(Platform.entries))
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
        bootstrap.bootstrap()

        val publisher = NatsJobQueuePublisher(connection, config)
        val consumer = NatsJobQueueConsumer(connection, config)

        val event = RenovateJobEvent(
            repo = RepoRef(Platform.GITHUB, "hononeko/ack-repo"),
            deliveryId = "ack-1",
            triggerReason = TriggerReason.WEBHOOK,
        )
        publisher.publish(event)

        assertEquals(1L, jsm.getStreamInfo(jobsStream).streamState.msgCount)

        val delivery = consumer.fetchNext(github, 2.seconds)
        assertNotNull(delivery)
        delivery?.ack()

        var retries = 0
        while (jsm.getStreamInfo(jobsStream).streamState.msgCount > 0L && retries++ < 20) {
            delay(50.milliseconds)
        }
        assertEquals(0L, jsm.getStreamInfo(jobsStream).streamState.msgCount)
        assertNull(consumer.fetchNext(github, 200.milliseconds))

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
        bootstrap.bootstrap()

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
        bootstrap.bootstrap()

        val publisher = NatsJobQueuePublisher(connection, config)
        val consumer = NatsJobQueueConsumer(connection, config)

        val event = RenovateJobEvent(
            repo = RepoRef(Platform.GITHUB, "hononeko/heartbeat-repo"),
            deliveryId = "hb-1",
            triggerReason = TriggerReason.WEBHOOK,
        )
        publisher.publish(event)

        val delivery = consumer.fetchNext(github, 3.seconds)
        assertNotNull(delivery)
        assertEquals(1, delivery?.deliveryAttempt)

        // 1. Wait 1.5s, then call heartbeat()
        delay(1500.milliseconds)
        delivery?.heartbeat()

        // 2. Wait another 2.0s (total 3.5s > 3.0s initial AckWait, but only 2.0s since heartbeat).
        // Since heartbeat reset the timer, a fetch attempt should NOT see redelivery yet.
        delay(2000.milliseconds)
        val earlyAttempt = consumer.fetchNext(github, 300.milliseconds)
        assertNull(earlyAttempt)

        // 3. Now let AckWait expire without heartbeat (wait 2.0s more, total 4.0s since heartbeat > 3.0s AckWait)
        delay(2000.milliseconds)

        val redelivered = consumer.fetchNext(github, 3.seconds)
        assertNotNull(redelivered)
        assertEquals(2, redelivered?.deliveryAttempt)
        redelivered?.ack()

        consumer.close()
    }

    @Test
    fun `fetchNext with non-positive timeout returns null immediately`() = runBlocking {
        val config = NatsConfig(storageType = StorageType.Memory)
        val consumer = NatsJobQueueConsumer(connection, config)

        assertNull(consumer.fetchNext(github, 0.seconds))
        assertNull(consumer.fetchNext(github, (-5).seconds))

        consumer.close()
    }
}
