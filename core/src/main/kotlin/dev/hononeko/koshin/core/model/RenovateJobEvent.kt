package dev.hononeko.koshin.core.model

import kotlinx.serialization.Serializable

/** [installationId] is the GitHub App installation the job runs as, when it's known (KSH-24). */
@Serializable
data class RenovateJobEvent(
    val repo: RepoRef,
    val deliveryId: String,
    val triggerReason: TriggerReason,
    val rateLimitAttempt: Int = 0,
    val installationId: Long? = null,
) {
    init {
        require(deliveryId.isNotBlank()) { "Delivery ID cannot be blank" }
        require(rateLimitAttempt >= 0) { "rateLimitAttempt must be non-negative" }
        require(installationId == null || (installationId > 0 && repo.platform == Platform.GITHUB)) {
            "installationId must be positive and on a GitHub repository: $installationId ($repo)"
        }
    }

    /** The credential scope the job runs in: its queue, run slots and rate-limit cooldown. */
    val scope: CredentialScope
        get() = CredentialScope.of(repo.platform, installationId)
}
