package dev.hononeko.koshin.adapters.nats

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.ports.QueueDepth
import dev.hononeko.koshin.core.ports.QueueDepthPort
import io.nats.client.JetStreamApiException
import io.nats.client.JetStreamManagement
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

/** A platform consumer's `num_pending` and `num_ack_pending`; zero while the consumer doesn't exist yet. */
class NatsQueueDepth(
    private val jsm: JetStreamManagement,
    private val config: NatsConfig = NatsConfig(),
    private val ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
) : QueueDepthPort {
    override suspend fun depth(platform: Platform): QueueDepth = withContext(ioDispatcher) {
        try {
            val info = jsm.getConsumerInfo(config.jobsStreamName, config.consumerNameFor(platform))
            QueueDepth(pending = info.numPending, ackPending = info.numAckPending)
        } catch (e: JetStreamApiException) {
            if (e.apiErrorCode !in NOT_FOUND_CODES && e.errorCode !in NOT_FOUND_CODES) throw e
            QueueDepth(0, 0)
        }
    }

    private companion object {
        /** No consumer, no stream. */
        val NOT_FOUND_CODES = setOf(10014, 10059)
    }
}
