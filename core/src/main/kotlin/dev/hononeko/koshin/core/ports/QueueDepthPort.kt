package dev.hononeko.koshin.core.ports

import dev.hononeko.koshin.core.model.Platform

/** Jobs waiting for a first delivery ([pending]), and delivered but not yet settled ([ackPending]). */
data class QueueDepth(val pending: Long, val ackPending: Long) {
    init {
        require(pending >= 0 && ackPending >= 0) { "Queue depths cannot be negative: $this" }
    }
}

interface QueueDepthPort {
    suspend fun depth(platform: Platform): QueueDepth
}
