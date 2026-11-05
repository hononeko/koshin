package dev.hononeko.koshin.core.ports

import dev.hononeko.koshin.core.model.CredentialScope
import kotlin.time.Duration

interface JobQueueConsumer {
    /** The next job of [scope]'s queue, waiting up to [timeout] for one. */
    suspend fun fetchNext(scope: CredentialScope, timeout: Duration): JobDelivery?
}
