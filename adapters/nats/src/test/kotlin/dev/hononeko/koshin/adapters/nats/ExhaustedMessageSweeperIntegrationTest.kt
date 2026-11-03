package dev.hononeko.koshin.adapters.nats

import dev.hononeko.koshin.core.model.CredentialScope
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.model.TriggerReason
import dev.hononeko.koshin.core.ports.JobDelivery
import io.nats.client.Connection
import io.nats.client.Nats
import io.nats.client.Options
import io.nats.client.PullSubscribeOptions
import io.nats.client.api.StorageType
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource

/**
 * An exhausted job whose MaxDeliver advisory nobody received (no forwarder subscribed). NATS handles ACKs and NAKs
 * asynchronously, so the tests wait for it to have acted on each one rather than racing it.
 */
class ExhaustedMessageSweeperIntegrationTest {
    companion object {
        private lateinit var connection: Connection
        private val ids = AtomicInteger()
        private val AWAIT_TIMEOUT = 10.seconds

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
        jobsStreamName = "SWEEP_JOBS_$id",
        dlqStreamName = "SWEEP_DLQ_$id",
        jobsSubjectPrefix = "sweep$id.jobs",
        dlqSubjectPrefix = "sweep$id.dlq",
        storageType = StorageType.Memory,
        // Long enough that no in-flight job is redelivered while a slow test runs
        ackWait = 30.seconds,
        maxDeliver = 2,
    )
    private val jsm = connection.jetStreamManagement()
    private val github = CredentialScope.Token(Platform.GITHUB)

    private fun job(repo: String, installationId: Long? = null) = RenovateJobEvent(
        RepoRef(Platform.GITHUB, "hononeko/$repo"),
        "delivery-$repo",
        TriggerReason.WEBHOOK,
        installationId = installationId,
    )

    private fun sweeper(): ExhaustedMessageSweeper {
        // Not started: nobody listens for the advisory, as during a rolling deployment
        val forwarder = MaxDeliverDlqForwarder(connection, config, autoStart = false)
        return ExhaustedMessageSweeper(jsm, config, confirmDelay = 200.milliseconds) { seq ->
            forwarder.moveToDeadLetter(seq, config.maxDeliver, MaxDeliverDlqForwarder.SOURCE_SWEEP)
        }
    }

    private suspend fun setUpStream(vararg repos: String): Pair<NatsJobQueuePublisher, NatsJobQueueConsumer> {
        NatsJetStreamBootstrap(jsm, config).bootstrap()
        val publisher = NatsJobQueuePublisher(connection, config)
        repos.forEach { publisher.publish(job(it)) }
        return publisher to NatsJobQueueConsumer(connection, config)
    }

    private fun consumerInfo(scope: CredentialScope = github) =
        jsm.getConsumerInfo(config.jobsStreamName, config.consumerNameFor(scope))

    private suspend fun awaitUntil(what: String, condition: suspend () -> Boolean) {
        val deadline = TimeSource.Monotonic.markNow() + AWAIT_TIMEOUT
        while (!condition()) {
            check(deadline.hasNotPassedNow()) { "Timed out waiting for $what" }
            delay(20.milliseconds)
        }
    }

    /**
     * Delivers and NAKs [repo]'s job, the only one waiting, until it's over MaxDeliver. NATS gives up on it when it
     * next tries to deliver it, so pull until it's no longer ACK-pending.
     */
    private suspend fun exhaust(consumer: NatsJobQueueConsumer, repo: String, scope: CredentialScope = github) {
        repeat(config.maxDeliver.toInt()) {
            // Waits for the redelivery while NATS hasn't processed the previous NAK yet
            val delivery = consumer.fetchNext(scope, AWAIT_TIMEOUT)
            assertEquals("hononeko/$repo", delivery?.event?.repo?.repository)
            delivery!!.nack()
        }
        awaitUntil("hononeko/$repo to be exhausted") {
            assertNull(consumer.fetchNext(scope, 100.milliseconds), "delivered past MaxDeliver")
            consumerInfo(scope).numAckPending == 0L
        }
    }

    /** ACKs [delivery] and waits until NATS has deleted the job from the work queue. */
    private suspend fun ackAndAwait(delivery: JobDelivery) {
        delivery.ack()
        awaitUntil("${delivery.event.repo} to be ACKed") { delivery.event.repo.repository !in storedJobs() }
    }

    private fun repoOf(data: ByteArray) =
        Json.decodeFromString(RenovateJobEvent.serializer(), data.decodeToString()).repo.repository

    private fun storedJobs(): List<String> {
        val state = jsm.getStreamInfo(config.jobsStreamName).streamState
        if (state.msgCount == 0L) return emptyList()
        return (state.firstSequence..state.lastSequence).mapNotNull { seq ->
            runCatching { jsm.getMessage(config.jobsStreamName, seq) }.getOrNull()
                ?.let { repoOf(it.data!!) }
        }
    }

    @Test
    fun `moves only exhausted jobs to the DLQ`() = runBlocking {
        val (publisher, consumer) = setUpStream("exhausted")
        exhaust(consumer, "exhausted")
        listOf("acked", "in-flight", "waiting").forEach { publisher.publish(job(it)) }

        // Acking "acked" moves the ack floor past "exhausted", which is no longer ACK-pending
        val acked = consumer.fetchNext(github, AWAIT_TIMEOUT)!!
        assertEquals("hononeko/acked", acked.event.repo.repository)
        ackAndAwait(acked)
        awaitUntil("the ack floor to pass hononeko/acked") { consumerInfo().ackFloor.streamSequence >= 2 }
        val inFlight = consumer.fetchNext(github, AWAIT_TIMEOUT)
        assertEquals("hononeko/in-flight", inFlight?.event?.repo?.repository)

        assertEquals(1, sweeper().sweep())

        assertEquals(listOf("hononeko/in-flight", "hononeko/waiting"), storedJobs())
        val dlqOptions = PullSubscribeOptions.builder().stream(config.dlqStreamName).build()
        val dlq = connection.jetStream()
            .subscribe(config.dlqSubjectFor(Platform.GITHUB), dlqOptions)
            .fetch(10, java.time.Duration.ofSeconds(2))
        val moved = dlq.single()
        assertEquals("hononeko/exhausted", repoOf(moved.data))
        assertEquals("dlq:${config.jobsStreamName}:1", moved.headers.getFirst("Nats-Msg-Id"))
        assertEquals("sweep", moved.headers.getFirst("Koshin-Dlq-Source"))
        assertEquals("2", moved.headers.getFirst("Koshin-Deliveries"))

        // The in-flight job is still the worker's to settle
        ackAndAwait(inFlight!!)
        assertEquals(0, sweeper().sweep())
        assertEquals(listOf("hononeko/waiting"), storedJobs())
        consumer.close()
    }

    @Test
    fun `nothing to sweep without a consumer or exhausted jobs`() = runBlocking {
        assertEquals(0, sweeper().sweep(), "no stream or consumer yet")

        NatsJetStreamBootstrap(jsm, config).bootstrap()
        val publisher = NatsJobQueuePublisher(connection, config)
        val consumer = NatsJobQueueConsumer(connection, config)
        publisher.publish(job("ok"))
        ackAndAwait(consumer.fetchNext(github, AWAIT_TIMEOUT)!!)

        assertEquals(0, sweeper().sweep())
        assertNull(runCatching { jsm.getMessage(config.dlqStreamName, 1) }.getOrNull())
        assertNotNull(jsm.getStreamInfo(config.jobsStreamName))
        consumer.close()
    }

    @Test
    fun `an exhausted job at the end of the queue is swept too`() = runBlocking {
        val (_, consumer) = setUpStream("last")
        // An idle worker's next pull is when NATS finds the job over MaxDeliver; no later job moves the ack floor
        exhaust(consumer, "last")

        assertEquals(1, sweeper().sweep())
        assertEquals(emptyList<String>(), storedJobs())
        consumer.close()
    }

    @Test
    fun `exhausted jobs of every scope's consumer are swept`() = runBlocking {
        val (publisher, consumer) = setUpStream()
        publisher.publish(job("one", installationId = 1))
        publisher.publish(job("two", installationId = 2))
        exhaust(consumer, "one", CredentialScope.Installation(1))
        exhaust(consumer, "two", CredentialScope.Installation(2))
        publisher.publish(job("fine", installationId = 2))

        assertEquals(2, sweeper().sweep())
        assertEquals(listOf("hononeko/fine"), storedJobs())
        consumer.close()
    }

    @Test
    fun `a job waiting to be redelivered is left alone`() = runBlocking {
        val (_, consumer) = setUpStream("retry-later")
        consumer.fetchNext(github, AWAIT_TIMEOUT)!!.nack(60.seconds)

        assertEquals(0, sweeper().sweep())
        assertEquals(listOf("hononeko/retry-later"), storedJobs())
        consumer.close()
    }
}
