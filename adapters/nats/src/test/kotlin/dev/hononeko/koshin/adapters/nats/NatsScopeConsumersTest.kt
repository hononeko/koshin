package dev.hononeko.koshin.adapters.nats

import dev.hononeko.koshin.core.model.CredentialScope
import dev.hononeko.koshin.core.model.Platform
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import io.nats.client.JetStream
import io.nats.client.JetStreamApiException
import io.nats.client.JetStreamManagement
import io.nats.client.PublishOptions
import io.nats.client.api.AckPolicy
import io.nats.client.api.ConsumerConfiguration
import io.nats.client.api.ConsumerInfo
import io.nats.client.api.Error
import io.nats.client.api.MessageInfo
import io.nats.client.api.SequenceInfo
import io.nats.client.api.StreamInfo
import io.nats.client.api.StreamInfoOptions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.seconds

class NatsScopeConsumersTest {
    private val config = NatsConfig(ackWait = 30.seconds, maxDeliver = 4)
    private val jsm = mockk<JetStreamManagement>(relaxed = true)
    private val js = mockk<JetStream>(relaxed = true)
    private val now = Instant.parse("2026-11-03T12:00:00Z")
    private val consumers = NatsScopeConsumers(jsm, config, js) { now }
    private val inst42 = CredentialScope.Installation(42)
    private val github = CredentialScope.Token(Platform.GITHUB)
    private val gitlab = CredentialScope.Token(Platform.GITLAB)

    private class ApiError(private val code: Int) : JetStreamApiException(Error.JsNoMessageFoundErr) {
        override fun getApiErrorCode() = code
    }

    private fun ago(duration: Duration): ZonedDateTime =
        ZonedDateTime.ofInstant(now.minusSeconds(duration.inWholeSeconds), ZoneOffset.UTC)

    @Suppress("LongParameterList")
    private fun consumer(
        scope: CredentialScope,
        idleFor: Duration = 30.days,
        pending: Long = 0,
        ackPending: Long = 0,
        waiting: Long = 0,
        name: String = config.consumerNameFor(scope),
    ) = mockk<ConsumerInfo> {
        every { this@mockk.name } returns name
        every { consumerConfiguration } returns mockk<ConsumerConfiguration> {
            every { filterSubject } returns config.jobsSubjectFor(scope)
        }
        every { creationTime } returns ago(60.days)
        every { delivered } returns mockk<SequenceInfo> { every { lastActive } returns ago(idleFor) }
        every { ackFloor } returns mockk<SequenceInfo> { every { lastActive } returns null }
        every { numPending } returns pending
        every { numAckPending } returns ackPending
        every { numWaiting } returns waiting
    }

    private fun message(seq: Long, data: String) = mockk<MessageInfo> {
        every { this@mockk.seq } returns seq
        every { this@mockk.data } returns data.toByteArray()
        every { isMessage } returns true
    }

    @Test
    fun `ensure creates the scope's durable consumer on its subject`() {
        val created = slot<ConsumerConfiguration>()
        every { jsm.addOrUpdateConsumer(config.jobsStreamName, capture(created)) } returns mockk()

        consumers.ensure(inst42)

        with(created.captured) {
            assertEquals("koshin-workers-github-inst-42", durable)
            assertEquals("koshin-workers-github-inst-42", name)
            assertEquals("renovate.jobs.github.inst-42", filterSubject)
            assertEquals(AckPolicy.Explicit, ackPolicy)
            assertEquals(java.time.Duration.ofSeconds(30), ackWait)
            assertEquals(4L, maxDeliver)
        }
    }

    @Test
    fun `creates the missing consumers of scopes with stored jobs`() {
        every { jsm.getConsumerNames(config.jobsStreamName) } returns listOf("koshin-workers-github-pat")
        every { jsm.getStreamInfo(config.jobsStreamName, any<StreamInfoOptions>()) } returns mockk<StreamInfo> {
            every { streamState.subjectMap } returns mapOf(
                "renovate.jobs.github.pat" to 2L,
                "renovate.jobs.github.inst-42" to 1L,
                "renovate.jobs.gitlab.pat" to 3L,
                "renovate.jobs.github" to 5L,
            )
        }

        assertEquals(2, consumers.ensureForStoredJobs())

        verify(exactly = 1) { jsm.addOrUpdateConsumer(any(), match { it.name == "koshin-workers-github-inst-42" }) }
        verify(exactly = 1) { jsm.addOrUpdateConsumer(any(), match { it.name == "koshin-workers-gitlab-pat" }) }
        verify(exactly = 2) { jsm.addOrUpdateConsumer(any(), any()) }
    }

    @Test
    fun `deletes only scope consumers idle past the TTL`() {
        every { jsm.getConsumers(config.jobsStreamName) } returns listOf(
            consumer(inst42),
            consumer(CredentialScope.Installation(7), idleFor = 1.days),
            consumer(CredentialScope.Installation(8), pending = 1),
            consumer(CredentialScope.Installation(9), ackPending = 1),
            consumer(CredentialScope.Installation(10), waiting = 1),
            consumer(gitlab),
            consumer(github, name = "not-ours"),
        )
        every { jsm.deleteConsumer(any(), any()) } returns true
        every { jsm.deleteConsumer(config.jobsStreamName, "koshin-workers-gitlab-pat") } throws ApiError(10014)

        assertEquals(1, consumers.collectIdle(7.days), "gitlab's was already gone")

        verify { jsm.deleteConsumer(config.jobsStreamName, "koshin-workers-github-inst-42") }
        verify(exactly = 2) { jsm.deleteConsumer(any(), any()) }
        assertEquals(0, consumers.collectIdle(Duration.ZERO), "a zero TTL keeps them all")
    }

    @Test
    fun `moves jobs off the legacy per-platform subject into their scopes`() {
        val legacyJob = """{"repo":{"platform":"GITHUB","repository":"o/r"},""" +
            """"deliveryId":"d","triggerReason":"WEBHOOK"}"""
        every { jsm.getNextMessage(config.jobsStreamName, any(), "renovate.jobs.github") } answers {
            when (secondArg<Long>()) {
                1L -> message(3, legacyJob)
                4L -> message(5, "not json")
                else -> throw ApiError(10037)
            }
        }
        every { jsm.getNextMessage(config.jobsStreamName, any(), "renovate.jobs.gitlab") } throws ApiError(10037)
        every { jsm.deleteConsumer(config.jobsStreamName, "koshin-workers-gitlab") } throws ApiError(10014)
        every { jsm.deleteMessage(config.jobsStreamName, 5) } throws ApiError(10043) // another replica moved it
        val options = mutableListOf<PublishOptions>()

        assertEquals(2, consumers.migrateLegacy(Platform.entries))

        verify { jsm.deleteConsumer(config.jobsStreamName, "koshin-workers-github") }
        verify(exactly = 2) { js.publish("renovate.jobs.github.pat", any<ByteArray>(), capture(options)) }
        assertEquals(listOf("migrated:RENOVATE_JOBS:3", "migrated:RENOVATE_JOBS:5"), options.map { it.messageId })
        verify { jsm.deleteMessage(config.jobsStreamName, 3) }
        verify { jsm.deleteMessage(config.jobsStreamName, 5) }
        verify(exactly = 1) { jsm.addOrUpdateConsumer(any(), match { it.name == "koshin-workers-github-pat" }) }
    }

    @Test
    fun `moving jobs needs a JetStream context, and other errors propagate`() {
        every { jsm.getNextMessage(any(), any(), any()) } returns message(1, "{}")
        assertThrows<IllegalStateException> { NatsScopeConsumers(jsm, config).migrateLegacy(listOf(Platform.GITHUB)) }

        every { jsm.deleteConsumer(any(), any()) } throws ApiError(10071)
        assertThrows<JetStreamApiException> { consumers.migrateLegacy(listOf(Platform.GITHUB)) }

        every { jsm.deleteConsumer(any(), any()) } returns false
        every { jsm.getNextMessage(any(), any(), any()) } returns message(1, "{}") andThenThrows ApiError(10037)
        every { jsm.deleteMessage(any(), any()) } throws ApiError(10071)
        assertThrows<JetStreamApiException> { consumers.migrateLegacy(listOf(Platform.GITHUB)) }

        every { jsm.getNextMessage(any(), any(), any()) } throws ApiError(10071)
        assertThrows<JetStreamApiException> { consumers.migrateLegacy(listOf(Platform.GITHUB)) }
    }
}
