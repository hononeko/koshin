package dev.hononeko.koshin.adapters.nats

import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.ports.JobQueuePublisher
import io.nats.client.Connection
import io.nats.client.JetStream
import io.nats.client.PublishOptions
import io.nats.client.impl.Headers
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory

class NatsJobQueuePublisher(
    private val js: JetStream,
    private val config: NatsConfig = NatsConfig(),
    private val json: Json = Json { ignoreUnknownKeys = true },
    private val ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
) : JobQueuePublisher {
    constructor(
        connection: Connection,
        config: NatsConfig = NatsConfig(),
        json: Json = Json { ignoreUnknownKeys = true },
        ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
    ) : this(connection.jetStream(), config, json, ioDispatcher)

    private val logger = LoggerFactory.getLogger(NatsJobQueuePublisher::class.java)

    override suspend fun publish(event: RenovateJobEvent) {
        val platform = event.repo.platform.name.lowercase()
        val subject = config.jobsSubjectFor(event.repo.platform)
        val messageId = "$platform:${event.repo.repository}:${event.deliveryId}"
        publishInternal(subject, messageId, event)
    }

    override suspend fun publishToDeadLetter(event: RenovateJobEvent) {
        val platform = event.repo.platform.name.lowercase()
        val subject = config.dlqSubjectFor(event.repo.platform)
        val messageId = "$platform:${event.repo.repository}:${event.deliveryId}"
        publishInternal(subject, messageId, event)
    }

    private suspend fun publishInternal(subject: String, messageId: String, event: RenovateJobEvent) {
        val payload = json.encodeToString(RenovateJobEvent.serializer(), event).toByteArray(Charsets.UTF_8)
        val headers = Headers().add("Nats-Msg-Id", messageId)
        val publishOptions = PublishOptions.builder()
            .messageId(messageId)
            .build()

        logger.debug("Publishing job to {} with messageId {}", subject, messageId)
        withContext(ioDispatcher) {
            js.publish(subject, headers, payload, publishOptions)
        }
    }
}
