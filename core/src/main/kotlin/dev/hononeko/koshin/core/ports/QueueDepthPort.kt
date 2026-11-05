package dev.hononeko.koshin.core.ports

import dev.hononeko.koshin.core.model.CredentialScope
import dev.hononeko.koshin.core.model.Platform

/** Jobs waiting for a first delivery ([pending]), and delivered but not yet settled ([ackPending]). */
data class QueueDepth(val pending: Long, val ackPending: Long) {
    init {
        require(pending >= 0 && ackPending >= 0) { "Queue depths cannot be negative: $this" }
    }

    val isEmpty: Boolean
        get() = pending == 0L && ackPending == 0L
}

interface QueueDepthPort {
    /** The depth of every credential scope of [platforms] that has a queue, empty ones included. */
    suspend fun depths(platforms: Collection<Platform>): Map<CredentialScope, QueueDepth>
}
