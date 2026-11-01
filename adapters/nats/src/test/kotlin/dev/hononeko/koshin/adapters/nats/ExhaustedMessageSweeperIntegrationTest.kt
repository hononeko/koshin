package dev.hononeko.koshin.adapters.nats

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.model.TriggerReason
import io.nats.client.Connection
import io.nats.client.Nats
import io.nats.client.Options
import io.nats.client.PullSubscribeOptions
import io.nats.client.api.StorageType
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

/** An exhausted job whose MaxDeliver advisory nobody received (no forwarder subscribed). */
class ExhaustedMessageSweeperIntegrationTest {
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
        jobsStreamName = "SWEEP_JOBS_$id",
        dlqStreamName = "SWEEP_DLQ_$id",
        jobsSubjectPrefix = "sweep$id.jobs",
        dlqSubjectPrefix = "sweep$id.dlq",
        storageType = StorageType.Memory,
        ackWait = 1.seconds,
        maxDeliver = 2,
    )
    private val jsm = connection.jetStreamManagement()

    private fun job(repo: String) =
        RenovateJobEvent(RepoRef(Platform.GITHUB, "hononeko/$repo"), "delivery-$repo", TriggerReason.WEBHOOK)

    private fun sweeper(): ExhaustedMessageSweeper {
        // Not started: nobody listens for the advisory, as during a rolling deployment
        val forwarder = MaxDeliverDlqForwarder(connection, config, autoStart = false)
        return ExhaustedMessageSweeper(jsm, config, listOf(Platform.GITHUB), confirmDelay = 200.milliseconds) { seq ->
            forwarder.moveToDeadLetter(seq, config.maxDeliver, MaxDeliverDlqForwarder.SOURCE_SWEEP)
        }
    }

    private suspend fun setUpStream(vararg repos: String): Pair<NatsJobQueuePublisher, NatsJobQueueConsumer> {
        NatsJetStreamBootstrap(jsm, config).bootstrap(listOf(Platform.GITHUB))
        val publisher = NatsJobQueuePublisher(connection, config)
        repos.forEach { publisher.publish(job(it)) }
        return publisher to NatsJobQueueConsumer(connection, config)
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
        NatsJetStreamBootstrap(jsm, config).bootstrap(listOf(Platform.GITHUB))
        val publisher = NatsJobQueuePublisher(connection, config)
        val consumer = NatsJobQueueConsumer(connection, config)
        listOf("exhausted", "acked", "in-flight", "waiting").forEach { publisher.publish(job(it)) }

        repeat(2) { consumer.fetchNext(Platform.GITHUB, 2.seconds)!!.nack() }
        // The next pull finds "exhausted" over MaxDeliver and serves "acked"; acking it moves the ack floor past both
        val acked = consumer.fetchNext(Platform.GITHUB, 2.seconds)!!
        assertEquals("hononeko/acked", acked.event.repo.repository)
        acked.ack()
        val inFlight = consumer.fetchNext(Platform.GITHUB, 2.seconds)
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
        inFlight!!.ack()
        assertEquals(0, sweeper().sweep())
        assertEquals(listOf("hononeko/waiting"), storedJobs())
        consumer.close()
    }

    @Test
    fun `nothing to sweep without a consumer or exhausted jobs`() = runBlocking {
        assertEquals(0, sweeper().sweep(), "no stream or consumer yet")

        NatsJetStreamBootstrap(jsm, config).bootstrap(listOf(Platform.GITHUB))
        val publisher = NatsJobQueuePublisher(connection, config)
        val consumer = NatsJobQueueConsumer(connection, config)
        publisher.publish(job("ok"))
        consumer.fetchNext(Platform.GITHUB, 2.seconds)!!.ack()

        assertEquals(0, sweeper().sweep())
        assertNull(runCatching { jsm.getMessage(config.dlqStreamName, 1) }.getOrNull())
        assertNotNull(jsm.getStreamInfo(config.jobsStreamName))
        consumer.close()
    }

    @Test
    fun `an exhausted job at the end of the queue is swept too`() = runBlocking {
        val (_, consumer) = setUpStream("last")
        repeat(2) { consumer.fetchNext(Platform.GITHUB, 2.seconds)!!.nack() }
        // An idle worker's next pull is when NATS finds the job over MaxDeliver; no later job moves the ack floor
        assertNull(consumer.fetchNext(Platform.GITHUB, 500.milliseconds))

        assertEquals(1, sweeper().sweep())
        assertEquals(emptyList<String>(), storedJobs())
        consumer.close()
    }

    @Test
    fun `a job waiting to be redelivered is left alone`() = runBlocking {
        val (_, consumer) = setUpStream("retry-later")
        consumer.fetchNext(Platform.GITHUB, 2.seconds)!!.nack(60.seconds)

        assertEquals(0, sweeper().sweep())
        assertEquals(listOf("hononeko/retry-later"), storedJobs())
        consumer.close()
    }
}
