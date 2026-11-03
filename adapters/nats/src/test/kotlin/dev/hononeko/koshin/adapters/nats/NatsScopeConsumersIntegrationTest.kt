package dev.hononeko.koshin.adapters.nats

import dev.hononeko.koshin.core.model.CredentialScope
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.model.TriggerReason
import io.nats.client.Connection
import io.nats.client.Nats
import io.nats.client.Options
import io.nats.client.PullSubscribeOptions
import io.nats.client.api.AckPolicy
import io.nats.client.api.ConsumerConfiguration
import io.nats.client.api.StorageType
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class NatsScopeConsumersIntegrationTest {
    companion object {
        private lateinit var connection: Connection
        private val ids = AtomicInteger()

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

    private val id = ids.incrementAndGet()
    private val config = NatsConfig(
        jobsStreamName = "SCOPES_JOBS_$id",
        dlqStreamName = "SCOPES_DLQ_$id",
        jobsSubjectPrefix = "scopes$id.jobs",
        dlqSubjectPrefix = "scopes$id.dlq",
        storageType = StorageType.Memory,
        ackWait = 30.seconds,
    )
    private val jsm = connection.jetStreamManagement()
    private val js = connection.jetStream()

    private fun job(repo: String, installationId: Long? = null) = RenovateJobEvent(
        RepoRef(Platform.GITHUB, "o/$repo"),
        "d-$repo",
        TriggerReason.WEBHOOK,
        installationId = installationId,
    )

    private fun RenovateJobEvent.bytes() = Json.encodeToString(RenovateJobEvent.serializer(), this).toByteArray()

    private fun pending(scope: CredentialScope) =
        jsm.getConsumerInfo(config.jobsStreamName, config.consumerNameFor(scope)).numPending

    @Test
    fun `jobs of an earlier version's per-platform queue move to their scopes, and its consumer goes`() = runBlocking {
        NatsJetStreamBootstrap(jsm, config).bootstrap()
        // What an earlier version left behind: a per-platform consumer and jobs on the per-platform subject
        val legacySubject = config.legacyJobsSubjectFor(Platform.GITHUB)
        jsm.addOrUpdateConsumer(
            config.jobsStreamName,
            ConsumerConfiguration.builder()
                .durable(config.legacyConsumerNameFor(Platform.GITHUB))
                .filterSubject(legacySubject)
                .ackPolicy(AckPolicy.Explicit)
                .build(),
        )
        listOf(job("plain"), job("installed", installationId = 9), job("in-flight")).forEach { event ->
            js.publish(legacySubject, event.bytes())
        }
        js.publish(legacySubject, "not json".toByteArray())
        val legacyConsumer = config.legacyConsumerNameFor(Platform.GITHUB)
        val legacyWorker = js.subscribe(legacySubject, PullSubscribeOptions.bind(config.jobsStreamName, legacyConsumer))
        assertEquals(1, legacyWorker.fetch(1, Duration.ofSeconds(2)).size, "a job an earlier worker is running")

        val consumers = NatsScopeConsumers(jsm, config, js)
        assertEquals(4, consumers.migrateLegacy(Platform.entries))

        assertFalse(config.legacyConsumerNameFor(Platform.GITHUB) in jsm.getConsumerNames(config.jobsStreamName))
        assertEquals(3L, pending(CredentialScope.Token(Platform.GITHUB)), "plain, in-flight and the unreadable one")
        assertEquals(1L, pending(CredentialScope.Installation(9)))
        val installed = NatsJobQueueConsumer(connection, config).use { consumer ->
            checkNotNull(consumer.fetchNext(CredentialScope.Installation(9), 2.seconds)).also { it.ack() }
        }
        assertEquals(job("installed", installationId = 9), installed.event)
        assertEquals(0, consumers.migrateLegacy(Platform.entries), "nothing left to move")
    }

    @Test
    fun `an idle consumer is deleted, and created again for a job stored after`() = runBlocking {
        NatsJetStreamBootstrap(jsm, config).bootstrap()
        val scope = CredentialScope.Installation(5)
        NatsJobQueuePublisher(connection, config).publish(job("once", installationId = 5))
        NatsJobQueueConsumer(connection, config).use { consumer ->
            checkNotNull(consumer.fetchNext(scope, 2.seconds)).ack()
        }
        val name = config.consumerNameFor(scope)
        var waited = 0
        while (jsm.getConsumerInfo(config.jobsStreamName, name).numAckPending > 0 && waited++ < 50) {
            delay(20.milliseconds)
        }

        assertEquals(0, NatsScopeConsumers(jsm, config, js).collectIdle(), "active today")
        val weekLater = NatsScopeConsumers(jsm, config, js) { Instant.now().plusSeconds(8.days.inWholeSeconds) }
        assertEquals(1, weekLater.collectIdle())
        assertFalse(name in jsm.getConsumerNames(config.jobsStreamName))

        // Published as it was being deleted: the next pass creates the consumer, which delivers the job
        js.publish(config.jobsSubjectFor(scope), job("late", installationId = 5).bytes())
        assertEquals(1, weekLater.ensureForStoredJobs())
        assertTrue(name in jsm.getConsumerNames(config.jobsStreamName))
        assertEquals(1L, pending(scope))
        assertEquals(0, weekLater.ensureForStoredJobs())
    }
}
