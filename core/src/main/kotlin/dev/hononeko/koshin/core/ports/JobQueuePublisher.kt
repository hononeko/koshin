package dev.hononeko.koshin.core.ports

import dev.hononeko.koshin.core.model.RenovateJobEvent

interface JobQueuePublisher {
    suspend fun publish(event: RenovateJobEvent)
    suspend fun publishToDeadLetter(event: RenovateJobEvent)
}
