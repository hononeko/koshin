package dev.hononeko.koshin.adapters.nats

import dev.hononeko.koshin.core.model.Platform
import io.mockk.every
import io.mockk.mockk
import io.nats.client.JetStreamApiException
import io.nats.client.JetStreamManagement
import io.nats.client.api.ConsumerInfo
import io.nats.client.api.Error
import io.nats.client.api.MessageInfo
import io.nats.client.api.SequenceInfo
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.io.IOException
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

@OptIn(ExperimentalCoroutinesApi::class)
class ExhaustedMessageSweeperTest {
    private val config = NatsConfig()
    private val jsm = mockk<JetStreamManagement>()
    private val moved = mutableListOf<Long>()

    /** Stored GitHub jobs and the state of the GitHub consumer. */
    private fun stream(stored: MutableSet<Long>, floor: Long, delivered: Long = floor, ackPending: Long = 1) {
        every { jsm.getConsumerInfo(config.jobsStreamName, "koshin-workers-github") } returns mockk<ConsumerInfo> {
            every { ackFloor } returns mockk<SequenceInfo> { every { streamSequence } returns floor }
            every { this@mockk.delivered } returns mockk<SequenceInfo> { every { streamSequence } returns delivered }
            every { numAckPending } returns ackPending
        }
        every { jsm.getNextMessage(config.jobsStreamName, any(), "renovate.jobs.github") } answers {
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

    private fun TestScope.sweeper(
        maxPerSweep: Int = 1000,
        platforms: List<Platform> = listOf(Platform.GITHUB),
        move: (Long) -> Boolean = { moved.add(it) },
    ) = ExhaustedMessageSweeper(
        jsm,
        config,
        platforms,
        1.seconds,
        maxPerSweep,
        UnconfinedTestDispatcher(testScheduler),
        move,
    )

    @Test
    fun `moves stored jobs up to the ack floor and leaves the rest`() = runTest {
        stream(mutableSetOf(3L, 5L, 9L, 12L), floor = 9L)

        assertEquals(3, sweeper().sweep())
        assertEquals(listOf(3L, 5L, 9L), moved)
    }

    @Test
    fun `with nothing ACK-pending, every delivered job still stored is exhausted`() = runTest {
        stream(mutableSetOf(4L, 6L, 8L), floor = 3L, delivered = 6L, ackPending = 0)

        assertEquals(2, sweeper().sweep())
        assertEquals(listOf(4L, 6L), moved)
    }

    @Test
    fun `a job gone by confirmation time isn't counted`() = runTest {
        stream(mutableSetOf(4L, 6L), floor = 6L)

        assertEquals(1, sweeper { seq -> seq == 6L }.sweep())
    }

    @Test
    fun `one platform's failure doesn't stop the others`() = runTest {
        stream(mutableSetOf(2L), floor = 2L)
        every { jsm.getConsumerInfo(config.jobsStreamName, "koshin-workers-gitlab") } throws IOException("timeout")

        assertEquals(1, sweeper(platforms = listOf(Platform.GITLAB, Platform.GITHUB)).sweep())
    }

    @Test
    fun `a failed move is left for the next sweep`() = runTest {
        stream(mutableSetOf(1L, 2L), floor = 2L)

        val count = sweeper { seq -> if (seq == 1L) error("NATS unavailable") else moved.add(seq) }.sweep()

        assertEquals(1, count)
        assertEquals(listOf(2L), moved)
    }

    @Test
    fun `caps the jobs per sweep`() = runTest {
        stream(mutableSetOf(1L, 2L, 3L), floor = 3L)

        assertEquals(2, sweeper(maxPerSweep = 2).sweep())
    }

    @Test
    fun `a missing consumer or stream is nothing to sweep`() = runTest {
        every { jsm.getConsumerInfo(any(), any()) } throws notFound(10014)
        assertEquals(0, sweeper().sweep())

        every { jsm.getConsumerInfo(any(), any()) } throws notFound(10059)
        assertEquals(0, sweeper().sweep())

        stream(mutableSetOf(1L), floor = 1L)
        every { jsm.getNextMessage(any(), any(), any()) } throws notFound(10037)
        assertEquals(0, sweeper().sweep())
    }

    @Test
    fun `nothing below an empty ack floor`() = runTest {
        stream(mutableSetOf(1L), floor = 0L)

        assertEquals(0, sweeper().sweep())
    }

    @Test
    fun `sweeps on an interval and survives a failed sweep`() = runTest {
        var calls = 0
        stream(mutableSetOf(1L), floor = 1L)
        val failingOnce = sweeper { seq ->
            calls++
            if (calls == 1) throw CancellationException("not a real cancellation").let { IllegalStateException(it) }
            moved.add(seq)
        }

        val job = failingOnce.start(this, 1.minutes)
        advanceTimeBy(3.minutes + 2.seconds)
        job.cancel()

        assertEquals(3, calls, "the first sweep's move failed, the loop kept going")
    }
}
