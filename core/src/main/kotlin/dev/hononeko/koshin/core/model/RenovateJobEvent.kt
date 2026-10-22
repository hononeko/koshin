package dev.hononeko.koshin.core.model

import kotlinx.serialization.Serializable

@Serializable
data class RenovateJobEvent(
    val repo: RepoRef,
    val deliveryId: String,
    val triggerReason: TriggerReason,
    val rateLimitAttempt: Int = 0,
) {
    init {
        require(deliveryId.isNotBlank()) { "Delivery ID cannot be blank" }
        require(rateLimitAttempt >= 0) { "rateLimitAttempt must be non-negative" }
    }
}
