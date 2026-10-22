package dev.hononeko.koshin.core.usecase

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.model.TriggerReason
import dev.hononeko.koshin.core.ports.JobQueuePublisher
import java.util.UUID

class IngestWebhookUseCase(
    private val publisher: JobQueuePublisher,
    private val uuidGenerator: () -> String = { UUID.randomUUID().toString() },
) {
    suspend fun execute(
        platform: Platform,
        repository: String,
        deliveryId: String? = null,
        triggerReason: TriggerReason = TriggerReason.WEBHOOK,
    ): RenovateJobEvent {
        val trimmed = repository.trim()
        val repo = RepoRef(platform, trimmed)

        val resolvedDeliveryId = if (deliveryId.isNullOrBlank()) {
            uuidGenerator()
        } else {
            deliveryId.trim()
        }

        val event = RenovateJobEvent(
            repo = repo,
            deliveryId = resolvedDeliveryId,
            triggerReason = triggerReason,
            rateLimitAttempt = 0,
        )
        publisher.publish(event)
        return event
    }

    companion object {
        fun validateRepositoryFormat(platform: Platform, repository: String) {
            RepoRef(platform, repository)
        }
    }
}
