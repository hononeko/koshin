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
        validateRepositoryFormat(platform, trimmed)

        val resolvedDeliveryId = if (deliveryId.isNullOrBlank()) {
            uuidGenerator()
        } else {
            deliveryId.trim()
        }

        val event = RenovateJobEvent(
            repo = RepoRef(platform, trimmed),
            deliveryId = resolvedDeliveryId,
            triggerReason = triggerReason,
            rateLimitAttempt = 0,
        )
        publisher.publish(event)
        return event
    }

    companion object {
        fun validateRepositoryFormat(platform: Platform, repository: String) {
            require(repository.isNotBlank()) { "Repository cannot be blank" }
            require(!repository.startsWith("/") && !repository.endsWith("/")) {
                "Repository cannot start or end with a slash: '$repository'"
            }
            require(!repository.contains("..") && !repository.contains(" ")) {
                "Repository contains invalid characters: '$repository'"
            }

            val parts = repository.split("/")
            when (platform) {
                Platform.GITHUB -> {
                    require(parts.size == 2 && parts.all { it.isNotBlank() }) {
                        "Invalid GitHub repository format '$repository'. Expected 'owner/repo'."
                    }
                }

                Platform.GITLAB -> {
                    require(parts.size >= 2 && parts.all { it.isNotBlank() }) {
                        "Invalid GitLab project format '$repository'. " +
                            "Expected 'group/project' or 'group/subgroup/project'."
                    }
                }
            }
        }
    }
}
