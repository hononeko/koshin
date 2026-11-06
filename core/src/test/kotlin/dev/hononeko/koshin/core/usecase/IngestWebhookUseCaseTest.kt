package dev.hononeko.koshin.core.usecase

import dev.hononeko.koshin.core.model.CredentialScope
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.model.TriggerReason
import dev.hononeko.koshin.core.ports.EnqueueDecision
import dev.hononeko.koshin.core.ports.JobQueuePublisher
import dev.hononeko.koshin.core.ports.RepoCoordinationPort
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import io.mockk.slot
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.IOException
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class IngestWebhookUseCaseTest {

    private val publisher = mockk<JobQueuePublisher>()
    private val useCase = IngestWebhookUseCase(
        publisher = publisher,
        uuidGenerator = { "fixed-uuid-1234" },
    )

    @Test
    fun `publishes event with existing delivery id for GitHub repo`() = runTest {
        val slot = slot<RenovateJobEvent>()
        coEvery { publisher.publish(capture(slot)) } returns Unit

        val result = useCase.execute(
            platform = Platform.GITHUB,
            repository = "hononeko/koshin",
            deliveryId = "gh-deliv-1",
            triggerReason = TriggerReason.WEBHOOK,
        )

        assertEquals("hononeko/koshin", result.event.repo.repository)
        assertEquals(Platform.GITHUB, result.event.repo.platform)
        assertEquals("gh-deliv-1", result.event.deliveryId)
        assertEquals(TriggerReason.WEBHOOK, result.event.triggerReason)
        assertEquals(0, result.event.rateLimitAttempt)
        assertTrue(result.published)
        coVerify(exactly = 1) { publisher.publish(any()) }
    }

    @Test
    fun `a job for an installation is published in that installation's scope`() = runTest {
        val slot = slot<RenovateJobEvent>()
        coEvery { publisher.publish(capture(slot)) } returns Unit

        useCase.execute(Platform.GITHUB, "hononeko/koshin", "d", TriggerReason.WEBHOOK, installationId = 42)

        assertEquals(42L, slot.captured.installationId)
        assertEquals(CredentialScope.Installation(42), slot.captured.scope)
    }

    @Test
    fun `generates fresh UUID when delivery id is null or blank`() = runTest {
        val slot = slot<RenovateJobEvent>()
        coEvery { publisher.publish(capture(slot)) } returns Unit

        val resultNull = useCase.execute(
            platform = Platform.GITHUB,
            repository = "hononeko/koshin",
            deliveryId = null,
        )
        assertEquals("fixed-uuid-1234", resultNull.event.deliveryId)

        val resultBlank = useCase.execute(
            platform = Platform.GITHUB,
            repository = "hononeko/koshin",
            deliveryId = "   ",
        )
        assertEquals("fixed-uuid-1234", resultBlank.event.deliveryId)
    }

    @Test
    fun `accepts valid GitLab subgroup project format`() = runTest {
        coEvery { publisher.publish(any()) } returns Unit

        val result = useCase.execute(
            platform = Platform.GITLAB,
            repository = "gitlab-org/subgroup/koshin",
            deliveryId = "gl-123",
        )
        assertEquals("gitlab-org/subgroup/koshin", result.event.repo.repository)
    }

    private val repo = RepoRef(Platform.GITHUB, "hononeko/koshin")
    private val coordination = mockk<RepoCoordinationPort>()

    /** What the observer saw: the call and its cause's message (stack-trace recovery may copy the exception). */
    private val failures = mutableListOf<Pair<String, String?>>()
    private val ingestConfig = IngestConfig(queuedTtl = 2.hours, dirtyTtl = 1.hours, coalesceTimeout = 50.milliseconds)
    private val coalescing = IngestWebhookUseCase(
        publisher = publisher,
        coordination = coordination,
        config = ingestConfig,
        observer = object : IngestObserver {
            override fun failedOpen(repo: RepoRef, cause: Throwable?) {
                failures += "failedOpen" to cause?.message
            }

            override fun queuedMarkKept(repo: RepoRef, cause: Throwable?) {
                failures += "queuedMarkKept" to cause?.message
            }
        },
        uuidGenerator = { "fixed-uuid-1234" },
    )

    private suspend fun ingest() = coalescing.execute(Platform.GITHUB, "hononeko/koshin", "deliv-1")

    @Test
    fun `publishes an idle repo once it's marked queued`() = runTest {
        coEvery { coordination.enqueueOrCoalesce(repo, 2.hours, 1.hours) } returns EnqueueDecision.ENQUEUE
        coEvery { publisher.publish(any()) } returns Unit

        val result = ingest()

        assertEquals(EnqueueDecision.ENQUEUE, result.decision)
        assertTrue(result.published)
        coVerify(exactly = 1) { publisher.publish(result.event) }
        assertTrue(failures.isEmpty())
    }

    @Test
    fun `publishes nothing for a repo already queued or running`() = runTest {
        for (decision in listOf(EnqueueDecision.ALREADY_QUEUED, EnqueueDecision.MARKED_DIRTY)) {
            coEvery { coordination.enqueueOrCoalesce(any(), any(), any()) } returns decision

            val result = ingest()

            assertEquals(decision, result.decision)
            assertFalse(result.published)
        }
        coVerify(exactly = 0) { publisher.publish(any()) }
    }

    @Test
    fun `fails open and publishes when Valkey fails`() = runTest {
        val error = IllegalStateException("connection refused")
        coEvery { coordination.enqueueOrCoalesce(any(), any(), any()) } throws error
        coEvery { publisher.publish(any()) } returns Unit

        val result = ingest()

        assertTrue(result.published)
        coVerify(exactly = 1) { publisher.publish(any()) }
        assertEquals(listOf("failedOpen" to error.message), failures)
    }

    @Test
    fun `fails open and publishes when Valkey is slower than the timeout`() = runTest {
        coEvery { coordination.enqueueOrCoalesce(any(), any(), any()) } coAnswers {
            delay(1.seconds)
            EnqueueDecision.ALREADY_QUEUED
        }
        coEvery { publisher.publish(any()) } returns Unit

        val result = ingest()

        assertTrue(result.published)
        coVerify(exactly = 1) { publisher.publish(any()) }
        assertEquals(listOf<Pair<String, String?>>("failedOpen" to null), failures)
    }

    @Test
    fun `clears the queued mark when the publish fails`() = runTest {
        coEvery { coordination.enqueueOrCoalesce(any(), any(), any()) } returns EnqueueDecision.ENQUEUE
        coEvery { coordination.clearQueued(repo) } returns Unit
        coEvery { publisher.publish(any()) } throws IOException("NATS down")

        assertTrue(runCatching { ingest() }.exceptionOrNull() is IOException)

        coVerify(exactly = 1) { coordination.clearQueued(repo) }
        assertTrue(failures.isEmpty())
    }

    @Test
    fun `reports a queued mark it couldn't clear after a failed publish`() = runTest {
        val error = IllegalStateException("Valkey down too")
        coEvery { coordination.enqueueOrCoalesce(any(), any(), any()) } returns EnqueueDecision.ENQUEUE
        coEvery { coordination.clearQueued(repo) } throws error
        coEvery { publisher.publish(any()) } throws IOException("NATS down")

        assertTrue(runCatching { ingest() }.exceptionOrNull() is IOException)

        assertEquals(listOf("queuedMarkKept" to error.message), failures)
    }

    @Test
    fun `reports a queued mark whose clearing timed out after a failed publish`() = runTest {
        coEvery { coordination.enqueueOrCoalesce(any(), any(), any()) } returns EnqueueDecision.ENQUEUE
        coEvery { coordination.clearQueued(repo) } coAnswers { delay(1.seconds) }
        coEvery { publisher.publish(any()) } throws IOException("NATS down")

        assertTrue(runCatching { ingest() }.exceptionOrNull() is IOException)

        assertEquals(listOf<Pair<String, String?>>("queuedMarkKept" to null), failures)
    }

    @Test
    fun `leaves the mark alone when a fail-open publish fails`() = runTest {
        coEvery { coordination.enqueueOrCoalesce(any(), any(), any()) } throws IllegalStateException("down")
        coEvery { publisher.publish(any()) } throws IOException("NATS down")

        assertTrue(runCatching { ingest() }.exceptionOrNull() is IOException)

        coVerify(exactly = 0) { coordination.clearQueued(any()) }
    }

    @Test
    fun `ingest config rejects TTLs under a second and a non-positive timeout`() {
        assertThrows(IllegalArgumentException::class.java) { IngestConfig(queuedTtl = 500.milliseconds) }
        assertThrows(IllegalArgumentException::class.java) { IngestConfig(queuedTtl = Duration.INFINITE) }
        assertThrows(IllegalArgumentException::class.java) { IngestConfig(dirtyTtl = Duration.ZERO) }
        assertThrows(IllegalArgumentException::class.java) { IngestConfig(dirtyTtl = Duration.INFINITE) }
        assertThrows(IllegalArgumentException::class.java) { IngestConfig(coalesceTimeout = Duration.ZERO) }
        assertThrows(IllegalArgumentException::class.java) { IngestConfig(coalesceTimeout = Duration.INFINITE) }
        assertThrows(IllegalArgumentException::class.java) { IngestConfig(coalesceTimeout = (-1).seconds) }
    }

    @Test
    fun `rejects invalid repository formats`() {
        assertThrows(IllegalArgumentException::class.java) {
            IngestWebhookUseCase.validateRepositoryFormat(Platform.GITHUB, "")
        }
        assertThrows(IllegalArgumentException::class.java) {
            IngestWebhookUseCase.validateRepositoryFormat(Platform.GITHUB, "/hononeko/koshin")
        }
        assertThrows(IllegalArgumentException::class.java) {
            IngestWebhookUseCase.validateRepositoryFormat(Platform.GITHUB, "hononeko/koshin/")
        }
        assertThrows(IllegalArgumentException::class.java) {
            IngestWebhookUseCase.validateRepositoryFormat(Platform.GITHUB, "hononeko..koshin")
        }
        assertThrows(IllegalArgumentException::class.java) {
            IngestWebhookUseCase.validateRepositoryFormat(Platform.GITHUB, "hononeko koshin")
        }
        assertThrows(IllegalArgumentException::class.java) {
            IngestWebhookUseCase.validateRepositoryFormat(Platform.GITHUB, "single-segment")
        }
        assertThrows(IllegalArgumentException::class.java) {
            IngestWebhookUseCase.validateRepositoryFormat(Platform.GITHUB, "too/many/segments/for/github")
        }
        assertThrows(IllegalArgumentException::class.java) {
            IngestWebhookUseCase.validateRepositoryFormat(Platform.GITLAB, "single-segment-gitlab")
        }
    }
}
