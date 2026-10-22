package dev.hononeko.koshin.core.model

import kotlinx.serialization.Serializable

@Serializable
enum class TriggerReason {
    WEBHOOK,
    SCHEDULE,
    DIRTY_FOLLOWUP,
    RATE_LIMIT_REQUEUE,
}
