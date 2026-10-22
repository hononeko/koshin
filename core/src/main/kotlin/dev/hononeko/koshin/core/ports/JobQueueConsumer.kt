package dev.hononeko.koshin.core.ports

import dev.hononeko.koshin.core.model.Platform
import kotlin.time.Duration

interface JobQueueConsumer {
    suspend fun fetchNext(platform: Platform, timeout: Duration): JobDelivery?
}
