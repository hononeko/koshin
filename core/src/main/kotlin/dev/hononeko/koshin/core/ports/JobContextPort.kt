package dev.hononeko.koshin.core.ports

import dev.hononeko.koshin.core.model.RenovateJobEvent

/** Runs everything done for one delivery in a context of its own, e.g. to tag the logs of the job. */
interface JobContextPort {
    suspend fun within(event: RenovateJobEvent, deliveryAttempt: Int, block: suspend () -> Unit)

    /** No context of its own. */
    object None : JobContextPort {
        override suspend fun within(event: RenovateJobEvent, deliveryAttempt: Int, block: suspend () -> Unit) = block()
    }
}
