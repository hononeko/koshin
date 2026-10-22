package dev.hononeko.koshin.core.ports

import dev.hononeko.koshin.core.model.RenovateJobEvent
import kotlin.time.Duration

interface JobDelivery {
    val event: RenovateJobEvent
    val deliveryAttempt: Int
    suspend fun ack()
    suspend fun nack(delay: Duration = Duration.ZERO)
    suspend fun heartbeat()
}
