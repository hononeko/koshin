package dev.hononeko.koshin.adapters.nats

import dev.hononeko.koshin.core.model.CredentialScope
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.ports.JobQueuePublisher
import io.nats.client.Connection
import io.nats.client.JetStream
import io.nats.client.PublishOptions
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.TimeMark
import kotlin.time.TimeSource

/**
 * Publishes each job to its credential scope's subject, creating the scope's consumer first so workers see
 * the job. A consumer it created is taken to exist for [ensuredFor]; one deleted meanwhile is created again by the
 * proxy's housekeeping.
 */
@Suppress("LongParameterList") // All but the first have defaults
class NatsJobQueuePublisher(
    private val js: JetStream,
    private val config: NatsConfig = NatsConfig(),
    private val json: Json = Json { ignoreUnknownKeys = true },
    private val ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
    private val ensureConsumer: (CredentialScope) -> Unit = {},
    private val ensuredFor: Duration = DEFAULT_ENSURED_FOR,
    private val timeSource: TimeSource = TimeSource.Monotonic,
) : JobQueuePublisher {
    constructor(
        connection: Connection,
        config: NatsConfig = NatsConfig(),
        json: Json = Json { ignoreUnknownKeys = true },
        ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
    ) : this(
        js = connection.jetStream(),
        config = config,
        json = json,
        ioDispatcher = ioDispatcher,
        ensureConsumer = NatsScopeConsumers(connection.jetStreamManagement(), config)
            .let { consumers -> { scope -> consumers.ensure(scope) } },
    )

    private val logger = LoggerFactory.getLogger(NatsJobQueuePublisher::class.java)
    private val ensured = ConcurrentHashMap<CredentialScope, TimeMark>()

    override suspend fun publish(event: RenovateJobEvent) {
        val platform = event.repo.platform.name.lowercase()
        val scope = event.scope
        val messageId = "$platform:${event.repo.repository}:${event.deliveryId}"
        withContext(ioDispatcher) { ensureConsumerOf(scope) }
        publishInternal(config.jobsSubjectFor(scope), messageId, event)
    }

    override suspend fun publishToDeadLetter(event: RenovateJobEvent) {
        val platform = event.repo.platform.name.lowercase()
        val subject = config.dlqSubjectFor(event.repo.platform)
        val messageId = "$platform:${event.repo.repository}:${event.deliveryId}"
        publishInternal(subject, messageId, event)
    }

    private fun ensureConsumerOf(scope: CredentialScope) {
        if (ensured[scope]?.let { it.elapsedNow() < ensuredFor } == true) return
        ensureConsumer(scope)
        ensured[scope] = timeSource.markNow()
    }

    private suspend fun publishInternal(subject: String, messageId: String, event: RenovateJobEvent) {
        val payload = json.encodeToString(RenovateJobEvent.serializer(), event).toByteArray(Charsets.UTF_8)
        val publishOptions = PublishOptions.builder()
            .messageId(messageId)
            .build()

        logger.debug("Publishing job to {} with messageId {}", subject, messageId)
        val ack = withContext(ioDispatcher) {
            js.publish(subject, payload, publishOptions)
        }
        if (ack.isDuplicate) {
            logger.debug("Duplicate job publish detected for messageId {} on {}", messageId, subject)
        }
    }

    companion object {
        val DEFAULT_ENSURED_FOR: Duration = 1.minutes
    }
}
