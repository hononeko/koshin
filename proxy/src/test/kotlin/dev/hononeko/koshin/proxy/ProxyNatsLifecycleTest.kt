package dev.hononeko.koshin.proxy

import dev.hononeko.koshin.adapters.nats.NatsConfig
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.model.TriggerReason
import dev.hononeko.koshin.core.ports.JobQueuePublisher
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.nats.client.Connection
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.IOException
import kotlin.time.Duration.Companion.milliseconds

@OptIn(ExperimentalCoroutinesApi::class)
class ProxyNatsLifecycleTest {

    private val sampleEvent = RenovateJobEvent(
        repo = RepoRef(Platform.GITHUB, "hononeko/koshin"),
        deliveryId = "deliv-1",
        triggerReason = TriggerReason.WEBHOOK,
    )

    @Test
    fun `retries initial NATS connection and stream bootstrap asynchronously, then reports ready`() = runTest {
        val mockConn = mockk<Connection>(relaxed = true)
        every { mockConn.status } returns Connection.Status.CONNECTED
        val mockPub = mockk<JobQueuePublisher>(relaxed = true)

        var connectAttempts = 0
        var bootstrapCalled = 0

        val lifecycle = ProxyNatsLifecycle(
            natsUrl = "nats://localhost:4222",
            natsConfig = NatsConfig(),
            retryDelay = 1.milliseconds,
            ioDispatcher = UnconfinedTestDispatcher(testScheduler),
            connectionFactory = {
                connectAttempts++
                if (connectAttempts == 1) {
                    throw IOException("NATS not up yet")
                }
                mockConn
            },
            bootstrapStreams = { _, _ ->
                bootstrapCalled++
            },
            publisherFactory = { _, _ -> mockPub },
        )

        assertFalse(lifecycle.isReady())
        assertThrows<IOException> {
            lifecycle.publish(sampleEvent)
        }

        lifecycle.startAsync()
        testScheduler.advanceUntilIdle()

        assertEquals(2, connectAttempts)
        assertEquals(1, bootstrapCalled)
        assertTrue(lifecycle.isReady())

        lifecycle.publish(sampleEvent)
        lifecycle.publishToDeadLetter(sampleEvent)
        coVerify(exactly = 1) { mockPub.publish(sampleEvent) }
        coVerify(exactly = 1) { mockPub.publishToDeadLetter(sampleEvent) }

        every { mockConn.status } returns Connection.Status.DISCONNECTED
        assertFalse(lifecycle.isReady())

        lifecycle.close()
        verify(exactly = 1) { mockConn.close() }
        assertFalse(lifecycle.isReady())
    }
}
