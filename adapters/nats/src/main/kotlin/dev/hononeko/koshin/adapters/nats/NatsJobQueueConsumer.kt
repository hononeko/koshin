package dev.hononeko.koshin.adapters.nats

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.ports.JobDelivery
import dev.hononeko.koshin.core.ports.JobQueueConsumer
import io.nats.client.Connection
import io.nats.client.JetStream
import io.nats.client.JetStreamSubscription
import io.nats.client.Message
import io.nats.client.PullSubscribeOptions
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration
import kotlin.time.toJavaDuration

class NatsJobQueueConsumer(
    private val js: JetStream,
    private val config: NatsConfig = NatsConfig(),
    private val json: Json = Json { ignoreUnknownKeys = true },
    private val ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
) : JobQueueConsumer,
    AutoCloseable {
    constructor(
        connection: Connection,
        config: NatsConfig = NatsConfig(),
        json: Json = Json { ignoreUnknownKeys = true },
        ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
    ) : this(connection.jetStream(), config, json, ioDispatcher)

    private val logger = LoggerFactory.getLogger(NatsJobQueueConsumer::class.java)
    private val subscriptions = ConcurrentHashMap<Platform, JetStreamSubscription>()

    override suspend fun fetchNext(platform: Platform, timeout: Duration): JobDelivery? {
        if (timeout <= Duration.ZERO) {
            return null
        }

        val sub = getSubscription(platform)
        val msg = withContext(ioDispatcher) {
            sub.fetch(1, timeout.toJavaDuration())
        }.firstOrNull()

        return msg?.let { parseDelivery(it) }
    }

    private fun parseDelivery(msg: Message): NatsJobDelivery {
        val payloadString = msg.data.decodeToString()
        val event = try {
            json.decodeFromString(RenovateJobEvent.serializer(), payloadString)
        } catch (e: SerializationException) {
            logger.error("Failed to deserialize RenovateJobEvent from payload: {}", payloadString, e)
            throw e
        } catch (e: IllegalArgumentException) {
            logger.error("Invalid RenovateJobEvent data in payload: {}", payloadString, e)
            throw e
        }

        return NatsJobDelivery(msg, event, ioDispatcher)
    }

    private fun getSubscription(platform: Platform): JetStreamSubscription = subscriptions.computeIfAbsent(platform) {
        val durable = config.consumerNameFor(platform)
        val filterSubject = config.jobsSubjectFor(platform)
        val pso = PullSubscribeOptions.builder()
            .stream(config.jobsStreamName)
            .durable(durable)
            .bind(true)
            .build()

        logger.debug(
            "Binding pull subscription to stream {} durable {} on subject {}",
            config.jobsStreamName,
            durable,
            filterSubject,
        )
        js.subscribe(filterSubject, pso)
    }

    override fun close() {
        subscriptions.values.forEach { sub ->
            runCatching {
                if (sub.isActive) {
                    sub.unsubscribe()
                }
            }.onFailure { e ->
                logger.warn("Failed to unsubscribe pull subscription", e)
            }
        }
        subscriptions.clear()
    }
}
