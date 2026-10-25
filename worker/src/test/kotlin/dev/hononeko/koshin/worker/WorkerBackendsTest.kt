package dev.hononeko.koshin.worker

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.IOException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class WorkerBackendsTest {

    @Test
    fun `retries both connections, exposes ports, reports readiness, and closes them`() = runBlocking {
        val natsAttempts = AtomicInteger()
        val valkeyAttempts = AtomicInteger()
        val natsHealthy = AtomicBoolean(true)
        val valkeyHealthy = AtomicBoolean(true)
        val closed = mutableListOf<String>()
        val consumer = QueueConsumer()
        val publisher = RecordingPublisher()
        val coordination = InMemoryCoordination()

        val backends = WorkerBackends(
            connectNats = {
                if (natsAttempts.incrementAndGet() < 3) throw IOException("nats down")
                NatsBackend(consumer, publisher, natsHealthy::get) { closed += "nats" }
            },
            connectValkey = {
                if (valkeyAttempts.incrementAndGet() < 2) throw IOException("valkey down")
                ValkeyBackend(coordination, valkeyHealthy::get) { closed += "valkey" }
            },
            retryDelay = 5.milliseconds,
        )

        assertFalse(backends.isReady())
        backends.startAsync()
        val ports = withTimeout(5.seconds) { backends.awaitPorts() }

        assertSame(consumer, ports.consumer)
        assertSame(publisher, ports.publisher)
        assertSame(coordination, ports.coordination)
        assertEquals(3, natsAttempts.get())
        assertEquals(2, valkeyAttempts.get())
        assertTrue(backends.isReady())

        valkeyHealthy.set(false)
        assertFalse(backends.isReady())
        valkeyHealthy.set(true)
        natsHealthy.set(false)
        assertFalse(backends.isReady())

        backends.close()
        assertEquals(listOf("nats", "valkey"), closed)
        assertFalse(backends.isReady())
    }
}
