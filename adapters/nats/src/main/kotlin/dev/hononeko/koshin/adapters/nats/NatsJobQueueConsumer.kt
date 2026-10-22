package dev.hononeko.koshin.adapters.nats

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.ports.JobDelivery
import dev.hononeko.koshin.core.ports.JobQueueConsumer
import io.nats.client.Connection
import io.nats.client.JetStream
import io.nats.client.JetStreamSubscription
import io.nats.client.Message
import io.nats.client.PublishOptions
import io.nats.client.PullSubscribeOptions
import io.nats.client.impl.Headers
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

        return withContext(ioDispatcher) {
            val sub = getSubscription(platform)
            val msg = sub.fetch(1, timeout.toJavaDuration()).firstOrNull() ?: return@withContext null
            parseDeliveryOrHandlePoisonPill(msg, platform)
        }
    }

    private fun parseDeliveryOrHandlePoisonPill(msg: Message, platform: Platform): NatsJobDelivery? {
        val payloadString = msg.data.decodeToString()
        val event = try {
            json.decodeFromString(RenovateJobEvent.serializer(), payloadString)
        } catch (e: SerializationException) {
            logger.error("Poison pill detected on platform {}: {}", platform, payloadString, e)
            handlePoisonPill(msg, platform, e)
            null
        } catch (e: IllegalArgumentException) {
            logger.error("Poison pill detected on platform {}: {}", platform, payloadString, e)
            handlePoisonPill(msg, platform, e)
            null
        } ?: return null

        return NatsJobDelivery(msg, event, ioDispatcher)
    }

    private fun handlePoisonPill(msg: Message, platform: Platform, error: Throwable) {
        val dlqSubject = config.dlqSubjectFor(platform)
        val streamSeq: Long = runCatching { msg.metaData().streamSequence() }.getOrDefault(0L)
        val stream: String = runCatching { msg.metaData().stream }.getOrDefault(config.jobsStreamName)
        val dlqMsgId = "dlq:poison:$stream:$streamSeq"

        val headers = if (msg.headers != null) Headers(msg.headers) else Headers()
        val originalMsgId = headers.getFirst("Nats-Msg-Id")
        if (originalMsgId != null) {
            headers.put("Koshin-Original-Msg-Id", originalMsgId)
        }
        headers.put("Koshin-Poison-Pill", "true")
        val errorMessage = (error.message ?: error.javaClass.simpleName)
            .replace("\r", " ")
            .replace("\n", " ")
            .take(MAX_ERROR_HEADER_LENGTH)
        headers.put("Koshin-Error", errorMessage)
        if (streamSeq > 0) {
            headers.put("Koshin-Stream-Seq", streamSeq.toString())
        }

        val publishOptions = PublishOptions.builder()
            .messageId(dlqMsgId)
            .build()

        runCatching {
            val ack = js.publish(dlqSubject, headers, msg.data, publishOptions)
            if (ack.isDuplicate) {
                logger.debug("Poison pill DLQ publish duplicate for {}", dlqMsgId)
            }
        }.onFailure { ex ->
            logger.error("Failed to forward poison pill to DLQ subject {}", dlqSubject, ex)
        }

        runCatching {
            msg.term()
        }.onFailure { ex ->
            logger.error("Failed to terminate poison pill message {}", streamSeq, ex)
        }
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

    companion object {
        private const val MAX_ERROR_HEADER_LENGTH = 200
    }
}
