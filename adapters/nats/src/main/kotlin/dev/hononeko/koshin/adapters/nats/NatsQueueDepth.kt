package dev.hononeko.koshin.adapters.nats

import dev.hononeko.koshin.core.model.CredentialScope
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.ports.QueueDepth
import dev.hononeko.koshin.core.ports.QueueDepthPort
import io.nats.client.JetStreamApiException
import io.nats.client.JetStreamManagement
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

/** Each scope consumer's `num_pending` and `num_ack_pending`, in one listing; none while there's no stream. */
class NatsQueueDepth(
    private val jsm: JetStreamManagement,
    private val config: NatsConfig = NatsConfig(),
    private val ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
    private val consumers: NatsScopeConsumers = NatsScopeConsumers(jsm, config),
) : QueueDepthPort {
    override suspend fun depths(platforms: Collection<Platform>): Map<CredentialScope, QueueDepth> =
        withContext(ioDispatcher) {
            try {
                consumers.list(platforms).mapValues { (_, info) -> QueueDepth(info.numPending, info.numAckPending) }
            } catch (e: JetStreamApiException) {
                if (e.apiErrorCode != STREAM_NOT_FOUND && e.errorCode != STREAM_NOT_FOUND) throw e
                emptyMap()
            }
        }

    private companion object {
        const val STREAM_NOT_FOUND = 10059
    }
}
