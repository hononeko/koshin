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
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

@OptIn(ExperimentalCoroutinesApi::class)
class ExhaustedMessageSweeperTest {
    private val config = NatsConfig()
    private val jsm = mockk<JetStreamManagement>()
    private val moved = mutableListOf<Long>()

    /** Stored GitHub jobs and the ack floor of the GitHub consumer. */
    private fun stream(stored: MutableSet<Long>, floor: Long) {
        every { jsm.getConsumerInfo(config.jobsStreamName, "koshin-workers-github") } returns mockk<ConsumerInfo> {
            every { ackFloor } returns mockk<SequenceInfo> { every { streamSequence } returns floor }
        }
        every { jsm.getNextMessage(config.jobsStreamName, any(), "renovate.jobs.github") } answers {
            val from = secondArg<Long>()
            stored.filter { seq -> seq >= from }.minOrNull()?.let { seq -> message(seq) } ?: throw notFound(10037)
        }
        every { jsm.getMessage(config.jobsStreamName, any<Long>()) } answers {
            secondArg<Long>().takeIf { seq -> seq in stored }?.let(::message) ?: throw notFound(10037)
        }
    }

    private fun message(seq: Long) = mockk<MessageInfo> { every { this@mockk.seq } returns seq }

    /** A real exception (logback walks its cause chain), with the given API error code. */
    private class ApiError(private val code: Int) : JetStreamApiException(Error.JsNoMessageFoundErr) {
        override fun getApiErrorCode() = code
    }

    private fun notFound(code: Int): JetStreamApiException = ApiError(code)

    private fun sweeper(maxPerSweep: Int = 1000, move: (Long) -> Unit = { moved += it }) =
        ExhaustedMessageSweeper(jsm, config, listOf(Platform.GITHUB), 1.seconds, maxPerSweep, move)

    @Test
    fun `moves stored jobs up to the ack floor and leaves the rest`() = runTest {
        stream(mutableSetOf(3L, 5L, 9L, 12L), floor = 9L)

        assertEquals(3, sweeper().sweep())
        assertEquals(listOf(3L, 5L, 9L), moved)
    }

    @Test
    fun `a job gone by confirmation time isn't moved`() = runTest {
        val stored = mutableSetOf(4L, 6L)
        stream(stored, floor = 6L)
        every { jsm.getMessage(config.jobsStreamName, 4L) } throws notFound(10037)

        assertEquals(1, sweeper().sweep())
        assertEquals(listOf(6L), moved)
    }

    @Test
    fun `a failed move is left for the next sweep`() = runTest {
        stream(mutableSetOf(1L, 2L), floor = 2L)

        val count = sweeper { seq -> if (seq == 1L) error("NATS unavailable") else moved += seq }.sweep()

        assertEquals(1, count)
        assertEquals(listOf(2L), moved)
    }

    @Test
    fun `caps the jobs per sweep`() = runTest {
        stream(mutableSetOf(1L, 2L, 3L), floor = 3L)

        assertEquals(2, sweeper(maxPerSweep = 2).sweep())
    }

    @Test
    fun `a missing consumer or stream is nothing to sweep, other errors are not`() = runTest {
        every { jsm.getConsumerInfo(any(), any()) } throws notFound(10014)
        assertEquals(0, sweeper().sweep())

        every { jsm.getConsumerInfo(any(), any()) } throws notFound(10059)
        assertEquals(0, sweeper().sweep())

        every { jsm.getConsumerInfo(any(), any()) } throws notFound(10008)
        assertThrows<JetStreamApiException> { sweeper().sweep() }
    }

    @Test
    fun `nothing below an empty ack floor`() = runTest {
        stream(mutableSetOf(1L), floor = 0L)

        assertEquals(0, sweeper().sweep())
    }

    @Test
    fun `sweeps on an interval and survives a failed sweep`() = runTest {
        var calls = 0
        every { jsm.getConsumerInfo(any(), any()) } answers {
            calls++
            if (calls == 1) throw notFound(10008)
            throw notFound(10014)
        }

        val job = sweeper().start(this, 1.minutes)
        advanceTimeBy(3.minutes + 1.seconds)
        job.cancel()

        assertEquals(3, calls, "first sweep failed, the loop kept going")
    }
}
