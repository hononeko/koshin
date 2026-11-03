package dev.hononeko.koshin.adapters.nats

import dev.hononeko.koshin.core.model.CredentialScope
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.ports.QueueDepth
import io.mockk.every
import io.mockk.mockk
import io.nats.client.JetStreamApiException
import io.nats.client.JetStreamManagement
import io.nats.client.api.ConsumerConfiguration
import io.nats.client.api.ConsumerInfo
import io.nats.client.api.Error
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class NatsQueueDepthTest {
    private val config = NatsConfig()
    private val jsm = mockk<JetStreamManagement>()
    private val depth = NatsQueueDepth(jsm, config)

    private class ApiError(private val code: Int) : JetStreamApiException(Error.JsNoMessageFoundErr) {
        override fun getApiErrorCode() = code
    }

    private fun consumer(name: String, filter: String?, pending: Long = 0, ackPending: Long = 0) = mockk<ConsumerInfo> {
        every { this@mockk.name } returns name
        every { consumerConfiguration } returns mockk<ConsumerConfiguration> {
            every { filterSubject } returns filter
        }
        every { numPending } returns pending
        every { numAckPending } returns ackPending
    }

    @Test
    fun `reads each scope consumer's pending and ack-pending counts in one listing`() = runTest {
        every { jsm.getConsumers(config.jobsStreamName) } returns listOf(
            consumer("koshin-workers-gitlab-pat", "renovate.jobs.gitlab.pat", pending = 4, ackPending = 2),
            consumer("koshin-workers-github-inst-42", "renovate.jobs.github.inst-42", pending = 1),
            consumer("koshin-workers-github", "renovate.jobs.github", pending = 9),
            consumer("someone-elses", "renovate.jobs.github.pat", pending = 9),
            consumer("multi-filter", null, pending = 9),
        )

        assertEquals(
            mapOf(
                CredentialScope.Token(Platform.GITLAB) to QueueDepth(4, 2),
                CredentialScope.Installation(42) to QueueDepth(1, 0),
            ),
            depth.depths(Platform.entries),
        )
        assertEquals(setOf(CredentialScope.Installation(42)), depth.depths(listOf(Platform.GITHUB)).keys)
    }

    @Test
    fun `a missing stream has no jobs, other errors propagate`() = runTest {
        every { jsm.getConsumers(any()) } throws ApiError(10059) andThenThrows ApiError(10071)
        assertEquals(emptyMap<CredentialScope, QueueDepth>(), depth.depths(Platform.entries))
        assertThrows<JetStreamApiException> { depth.depths(Platform.entries) }
    }
}
