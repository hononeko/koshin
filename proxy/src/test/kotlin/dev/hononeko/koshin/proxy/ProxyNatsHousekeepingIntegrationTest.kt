package dev.hononeko.koshin.proxy

import dev.hononeko.koshin.adapters.nats.MaxDeliverDlqForwarder
import dev.hononeko.koshin.adapters.nats.NatsConfig
import dev.hononeko.koshin.adapters.nats.NatsJobQueueConsumer
import dev.hononeko.koshin.adapters.nats.NatsJobQueuePublisher
import dev.hononeko.koshin.core.model.CredentialScope
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.model.TriggerReason
import io.nats.client.Connection
import io.nats.client.JetStreamApiException
import io.nats.client.Nats
import io.nats.client.api.StorageType
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.testcontainers.containers.GenericContainer
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/** The proxy alone, no worker: it creates the consumers and moves exhausted jobs to the DLQ (KSH-30). */
class ProxyNatsHousekeepingIntegrationTest {
    private companion object {
        val nats: GenericContainer<*> by lazy {
            GenericContainer("nats:2.10-alpine").withCommand("-js").withExposedPorts(4222).apply { start() }
        }
        val natsUrl: String
            get() = "nats://${nats.host}:${nats.getMappedPort(4222)}"
        val AWAIT_TIMEOUT = 20.seconds
    }

    private val config = NatsConfig(
        jobsStreamName = "PROXY_JOBS",
        dlqStreamName = "PROXY_DLQ",
        jobsSubjectPrefix = "proxy.jobs",
        dlqSubjectPrefix = "proxy.dlq",
        storageType = StorageType.Memory,
        ackWait = 30.seconds,
        maxDeliver = 2,
        exhaustedSweepInterval = 1.seconds,
    )
    private val connection: Connection = Nats.connect(natsUrl)
    private val jsm = connection.jetStreamManagement()
    private val deadLettered = CopyOnWriteArrayList<RenovateJobEvent>()
    private val lifecycles = mutableListOf<ProxyNatsLifecycle>()

    @AfterEach
    fun tearDown() {
        lifecycles.forEach { it.close() }
        runCatching { jsm.deleteStream(config.jobsStreamName) }
        runCatching { jsm.deleteStream(config.dlqStreamName) }
        connection.close()
    }

    private fun startProxy(): ProxyNatsLifecycle =
        ProxyNatsLifecycle(natsUrl, config, onDeadLettered = { deadLettered += it }).also {
            lifecycles += it
            it.startAsync()
            await("the proxy to bootstrap NATS") { it.isReady() }
        }

    /** An installation's job, queued in its credential scope (KSH-24). */
    private fun job(repo: String) = RenovateJobEvent(
        RepoRef(Platform.GITHUB, "hononeko/$repo"),
        "delivery-$repo",
        TriggerReason.WEBHOOK,
        installationId = 3,
    )

    private val scope = CredentialScope.Installation(3)

    private fun await(what: String, condition: suspend () -> Boolean) = runBlocking {
        val deadline = TimeSource.Monotonic.markNow() + AWAIT_TIMEOUT
        while (!condition()) {
            check(deadline.hasNotPassedNow()) { "Timed out waiting for $what" }
            delay(50.milliseconds)
        }
    }

    /** Delivers and NAKs [repo]'s job, the only one waiting, until NATS gives up on it. */
    private fun exhaust(repo: String) = runBlocking {
        NatsJobQueueConsumer(connection, config).use { consumer ->
            repeat(config.maxDeliver.toInt()) {
                val delivery = checkNotNull(consumer.fetchNext(scope, AWAIT_TIMEOUT))
                assertEquals("hononeko/$repo", delivery.event.repo.repository)
                delivery.nack()
            }
            await("hononeko/$repo to be exhausted") {
                assertNull(consumer.fetchNext(scope, 100.milliseconds), "delivered past MaxDeliver")
                jsm.getConsumerInfo(config.jobsStreamName, config.consumerNameFor(scope)).numAckPending == 0L
            }
        }
    }

    /** `Koshin-Dlq-Source` of each dead-lettered job, by repo. */
    private fun dlqSources(): Map<String, String?> {
        val state = jsm.getStreamInfo(config.dlqStreamName).streamState
        if (state.msgCount == 0L) return emptyMap()
        return (state.firstSequence..state.lastSequence).associate { seq ->
            val message = jsm.getMessage(config.dlqStreamName, seq)
            val event = ProxyJson.decodeFromString(RenovateJobEvent.serializer(), message.data!!.decodeToString())
            event.repo.repository to message.headers?.getFirst("Koshin-Dlq-Source")
        }
    }

    @Test
    fun `creates a scope's consumer as it publishes and dead-letters exhausted jobs, advisory and swept`() {
        assertThrows<JetStreamApiException> { jsm.getStreamInfo(config.jobsStreamName) }
        val proxy = startProxy()
        assertEquals(emptyList<String>(), jsm.getConsumerNames(config.jobsStreamName))

        runBlocking { proxy.publish(job("advisory")) }
        assertEquals(listOf("koshin-workers-github-inst-3"), jsm.getConsumerNames(config.jobsStreamName))
        exhaust("advisory")
        await("the advisory forwarder to dead-letter it") { dlqSources().size == 1 }
        assertEquals(mapOf("hononeko/advisory" to MaxDeliverDlqForwarder.SOURCE_ADVISORY), dlqSources())

        // Nobody handles this one's advisory, as when no proxy is up
        proxy.close()
        runBlocking { NatsJobQueuePublisher(connection, config).publish(job("swept")) }
        exhaust("swept")
        startProxy()

        await("the sweeper to dead-letter it") { dlqSources().size == 2 }
        assertEquals(MaxDeliverDlqForwarder.SOURCE_SWEEP, dlqSources()["hononeko/swept"])
        assertEquals(listOf("hononeko/advisory", "hononeko/swept"), deadLettered.map { it.repo.repository })
        assertEquals(0L, jsm.getStreamInfo(config.jobsStreamName).streamState.msgCount)
    }
}
