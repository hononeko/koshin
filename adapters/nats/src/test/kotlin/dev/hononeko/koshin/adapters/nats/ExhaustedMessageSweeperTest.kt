package dev.hononeko.koshin.adapters.nats

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.nats.client.JetStreamApiException
import io.nats.client.JetStreamManagement
import io.nats.client.api.ConsumerConfiguration
import io.nats.client.api.ConsumerInfo
import io.nats.client.api.Error
import io.nats.client.api.MessageInfo
import io.nats.client.api.SequenceInfo
import io.nats.client.api.StreamInfo
import io.nats.client.api.StreamInfoOptions
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.io.IOException
import kotlin.time.Duration.Companion.seconds

@OptIn(ExperimentalCoroutinesApi::class)
class ExhaustedMessageSweeperTest {
    private val config = NatsConfig()
    private val jsm = mockk<JetStreamManagement>()
    private val moved = mutableListOf<Long>()
    private val consumers = mutableListOf<ConsumerInfo>()
    private val storedBySubject = mutableMapOf<String, Long>()

    init {
        every { jsm.getConsumers(config.jobsStreamName) } answers { consumers.toList() }
        every { jsm.getStreamInfo(config.jobsStreamName, any<StreamInfoOptions>()) } answers {
            mockk<StreamInfo> { every { streamState.subjectMap } returns storedBySubject.toMap() }
        }
    }

    /** A consumer of [scope]'s subject, [stored] its jobs still in the work queue. */
    @Suppress("LongParameterList")
    private fun consumer(
        stored: Set<Long>,
        floor: Long,
        delivered: Long = floor,
        ackPending: Long = 1,
        pending: Long = 0,
        scope: String = "github.pat",
    ) {
        val subject = "renovate.jobs.$scope"
        storedBySubject[subject] = stored.size.toLong()
        consumers += mockk<ConsumerInfo> {
            every { name } returns "koshin-workers-${scope.replace('.', '-')}"
            every { consumerConfiguration } returns mockk<ConsumerConfiguration> {
                every { filterSubject } returns subject
            }
            every { ackFloor } returns mockk<SequenceInfo> { every { streamSequence } returns floor }
            every { this@mockk.delivered } returns mockk<SequenceInfo> { every { streamSequence } returns delivered }
            every { numAckPending } returns ackPending
            every { numPending } returns pending
        }
        every { jsm.getNextMessage(config.jobsStreamName, any(), subject) } answers {
            val from = secondArg<Long>()
            stored.filter { seq -> seq >= from }.minOrNull()?.let { seq -> message(seq) } ?: throw notFound(10037)
        }
    }

    private fun message(seq: Long) = mockk<MessageInfo> { every { this@mockk.seq } returns seq }

    /** A real exception (logback walks its cause chain), with the given API error code. */
    private class ApiError(private val code: Int) : JetStreamApiException(Error.JsNoMessageFoundErr) {
        override fun getApiErrorCode() = code
    }

    private fun notFound(code: Int): JetStreamApiException = ApiError(code)

    private fun TestScope.sweeper(maxPerSweep: Int = 1000, move: (Long) -> Boolean = { moved.add(it) }) =
        ExhaustedMessageSweeper(jsm, config, 1.seconds, maxPerSweep, UnconfinedTestDispatcher(testScheduler), move)

    @Test
    fun `moves stored jobs up to the ack floor and leaves the rest`() = runTest {
        consumer(setOf(3L, 5L, 9L, 12L), floor = 9L)

        assertEquals(3, sweeper().sweep())
        assertEquals(listOf(3L, 5L, 9L), moved)
    }

    @Test
    fun `with nothing ACK-pending, every delivered job still stored is exhausted`() = runTest {
        consumer(setOf(4L, 6L, 8L), floor = 3L, delivered = 6L, ackPending = 0, pending = 1)

        assertEquals(2, sweeper().sweep())
        assertEquals(listOf(4L, 6L), moved)
    }

    @Test
    fun `every scope's consumer is swept`() = runTest {
        consumer(setOf(2L), floor = 2L, ackPending = 0, scope = "github.inst-1")
        consumer(setOf(3L, 7L), floor = 3L, scope = "github.inst-2")
        consumer(setOf(5L), floor = 5L, ackPending = 0, scope = "gitlab.pat")

        assertEquals(3, sweeper().sweep())
        assertEquals(listOf(2L, 3L, 5L), moved)
    }

    @Test
    fun `a consumer whose stored jobs are all pending or ACK-pending isn't scanned`() = runTest {
        consumer(setOf(4L, 6L), floor = 6L, ackPending = 1, pending = 1)

        assertEquals(0, sweeper().sweep())
        verify(exactly = 0) { jsm.getNextMessage(any(), any(), any()) }
    }

    @Test
    fun `a job gone by confirmation time isn't counted`() = runTest {
        consumer(setOf(4L, 6L), floor = 6L, ackPending = 0)

        assertEquals(1, sweeper { seq -> seq == 6L }.sweep())
    }

    @Test
    fun `one consumer's failure doesn't stop the others`() = runTest {
        consumer(setOf(2L), floor = 2L, ackPending = 0, scope = "gitlab.pat")
        consumer(setOf(2L), floor = 2L, ackPending = 0)
        every { jsm.getNextMessage(config.jobsStreamName, any(), "renovate.jobs.gitlab.pat") } throws IOException("x")

        assertEquals(1, sweeper().sweep())
    }

    @Test
    fun `a failed move is left for the next sweep`() = runTest {
        consumer(setOf(1L, 2L), floor = 2L, ackPending = 0)

        val count = sweeper { seq -> if (seq == 1L) error("NATS unavailable") else moved.add(seq) }.sweep()

        assertEquals(1, count)
        assertEquals(listOf(2L), moved)
    }

    @Test
    fun `caps the jobs per consumer and sweep`() = runTest {
        consumer(setOf(1L, 2L, 3L), floor = 3L, ackPending = 0)

        assertEquals(2, sweeper(maxPerSweep = 2).sweep())
    }

    @Test
    fun `a missing stream, no jobs or a consumer without a single filter is nothing to sweep`() = runTest {
        every { jsm.getStreamInfo(config.jobsStreamName, any<StreamInfoOptions>()) } throws notFound(10059)
        assertEquals(0, sweeper().sweep())

        every { jsm.getStreamInfo(config.jobsStreamName, any<StreamInfoOptions>()) } answers {
            mockk<StreamInfo> { every { streamState.subjectMap } returns storedBySubject.toMap() }
        }
        assertEquals(0, sweeper().sweep(), "nothing stored")

        consumer(setOf(1L), floor = 1L, ackPending = 0)
        every { consumers.single().consumerConfiguration.filterSubject } returns null
        assertEquals(0, sweeper().sweep())
    }

    @Test
    fun `a job gone between listing and scanning is nothing to sweep`() = runTest {
        consumer(setOf(1L), floor = 1L, ackPending = 0)
        every { jsm.getNextMessage(any(), any(), any()) } throws notFound(10037)

        assertEquals(0, sweeper().sweep())
    }

    @Test
    fun `nothing below an empty ack floor`() = runTest {
        consumer(setOf(1L), floor = 0L)

        assertEquals(0, sweeper().sweep())
    }
}
