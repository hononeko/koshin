package dev.hononeko.koshin.core.usecase

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.model.TriggerReason
import dev.hononeko.koshin.core.ports.JobQueuePublisher
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import io.mockk.slot
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

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

        assertEquals("hononeko/koshin", result.repo.repository)
        assertEquals(Platform.GITHUB, result.repo.platform)
        assertEquals("gh-deliv-1", result.deliveryId)
        assertEquals(TriggerReason.WEBHOOK, result.triggerReason)
        assertEquals(0, result.rateLimitAttempt)
        coVerify(exactly = 1) { publisher.publish(any()) }
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
        assertEquals("fixed-uuid-1234", resultNull.deliveryId)

        val resultBlank = useCase.execute(
            platform = Platform.GITHUB,
            repository = "hononeko/koshin",
            deliveryId = "   ",
        )
        assertEquals("fixed-uuid-1234", resultBlank.deliveryId)
    }

    @Test
    fun `accepts valid GitLab subgroup project format`() = runTest {
        coEvery { publisher.publish(any()) } returns Unit

        val result = useCase.execute(
            platform = Platform.GITLAB,
            repository = "gitlab-org/subgroup/koshin",
            deliveryId = "gl-123",
        )
        assertEquals("gitlab-org/subgroup/koshin", result.repo.repository)
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
