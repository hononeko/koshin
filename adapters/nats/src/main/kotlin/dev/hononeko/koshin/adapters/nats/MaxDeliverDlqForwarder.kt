package dev.hononeko.koshin.adapters.nats

import dev.hononeko.koshin.core.model.RenovateJobEvent
import io.nats.client.Connection
import io.nats.client.Dispatcher
import io.nats.client.JetStream
import io.nats.client.JetStreamApiException
import io.nats.client.JetStreamManagement
import io.nats.client.Message
import io.nats.client.PublishOptions
import io.nats.client.api.MessageInfo
import io.nats.client.impl.Headers
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import java.io.IOException

class MaxDeliverDlqForwarder(
    private val connection: Connection,
    private val config: NatsConfig = NatsConfig(),
    private val jsm: JetStreamManagement = connection.jetStreamManagement(),
    private val js: JetStream = connection.jetStream(),
    private val json: Json = Json { ignoreUnknownKeys = true },
    autoStart: Boolean = true,
) : AutoCloseable {
    private val logger = LoggerFactory.getLogger(MaxDeliverDlqForwarder::class.java)
    private var dispatcher: Dispatcher? = null

    val isRunning: Boolean
        get() = synchronized(this) { dispatcher != null }

    init {
        if (autoStart) {
            start()
        }
    }

    fun start() {
        synchronized(this) {
            if (dispatcher != null) {
                return
            }
            val subject = "\$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.${config.jobsStreamName}.*"
            logger.info(
                "Starting MaxDeliverDlqForwarder on subject {} with queue group {}",
                subject,
                config.dlqForwarderQueueGroup,
            )
            val d = connection.createDispatcher { msg ->
                handleAdvisory(msg)
            }
            d.subscribe(subject, config.dlqForwarderQueueGroup)
            dispatcher = d
        }
    }

    internal fun handleAdvisory(msg: Message) {
        val advisory = parseAdvisoryOrNull(msg.data.decodeToString()) ?: return
        if (advisory.stream != null && advisory.stream != config.jobsStreamName) {
            logger.debug("Ignoring MaxDeliver advisory for unrelated stream: {}", advisory.stream)
            return
        }

        processExhaustedMessage(advisory.streamSeq)
    }

    private fun processExhaustedMessage(streamSeq: Long) {
        val msgInfo = fetchMessageOrNull(streamSeq) ?: return
        val platform = extractPlatform(msgInfo)
        forwardToDlq(streamSeq, msgInfo, platform)
        deleteFromWorkQueue(streamSeq)
    }

    private fun parseAdvisoryOrNull(payloadString: String): MaxDeliveriesAdvisory? = try {
        json.decodeFromString<MaxDeliveriesAdvisory>(payloadString)
    } catch (e: SerializationException) {
        logger.warn("Failed to parse MaxDeliver advisory: {}", payloadString, e)
        null
    } catch (e: IllegalArgumentException) {
        logger.warn("Invalid MaxDeliver advisory payload: {}", payloadString, e)
        null
    }

    private fun fetchMessageOrNull(streamSeq: Long): MessageInfo? = try {
        val msgInfo = jsm.getMessage(config.jobsStreamName, streamSeq)
        if (msgInfo == null || !msgInfo.isMessage || msgInfo.data == null) {
            logger.info("Message at sequence {} in {} is null or has no data", streamSeq, config.jobsStreamName)
            null
        } else {
            msgInfo
        }
    } catch (e: JetStreamApiException) {
        if (e.errorCode == MESSAGE_NOT_FOUND_ERROR_CODE || e.apiErrorCode == MESSAGE_NOT_FOUND_ERROR_CODE) {
            logger.info(
                "Message at sequence {} in stream {} not found (already handled)",
                streamSeq,
                config.jobsStreamName,
            )
            null
        } else {
            logger.error("Failed to retrieve message at sequence {} from {}", streamSeq, config.jobsStreamName, e)
            throw e
        }
    }

    private fun extractPlatform(msgInfo: MessageInfo): String {
        val subject = msgInfo.subject.orEmpty()
        val prefix = "${config.jobsSubjectPrefix}."
        if (subject.startsWith(prefix)) {
            return subject.removePrefix(prefix)
        }
        return try {
            val payload = msgInfo.data?.decodeToString().orEmpty()
            val event = json.decodeFromString(RenovateJobEvent.serializer(), payload)
            event.repo.platform.name.lowercase()
        } catch (_: SerializationException) {
            "unknown"
        } catch (_: IllegalArgumentException) {
            "unknown"
        }
    }

    private fun forwardToDlq(streamSeq: Long, msgInfo: MessageInfo, platform: String) {
        val dlqSubject = "${config.dlqSubjectPrefix}.$platform"
        val dlqMsgId = "dlq:${config.jobsStreamName}:$streamSeq"
        val headers = if (msgInfo.headers != null) Headers(msgInfo.headers) else Headers()
        headers.put("Nats-Msg-Id", dlqMsgId)

        val publishOptions = PublishOptions.builder()
            .messageId(dlqMsgId)
            .build()

        logger.info(
            "Forwarding exhausted message seq {} from {} to DLQ subject {}",
            streamSeq,
            config.jobsStreamName,
            dlqSubject,
        )
        js.publish(dlqSubject, headers, msgInfo.data, publishOptions)
    }

    private fun deleteFromWorkQueue(streamSeq: Long) {
        try {
            jsm.deleteMessage(config.jobsStreamName, streamSeq)
            logger.info("Successfully evicted sequence {} from {}", streamSeq, config.jobsStreamName)
        } catch (e: JetStreamApiException) {
            if (e.errorCode == MESSAGE_NOT_FOUND_ERROR_CODE || e.apiErrorCode == MESSAGE_NOT_FOUND_ERROR_CODE) {
                logger.info("Message {} was already deleted from {}", streamSeq, config.jobsStreamName)
            } else {
                logger.warn("Failed to delete message {} from {}", streamSeq, config.jobsStreamName, e)
            }
        }
    }

    override fun close() {
        synchronized(this) {
            dispatcher?.let { d ->
                try {
                    connection.closeDispatcher(d)
                } catch (e: IOException) {
                    logger.warn("Failed to close advisory dispatcher", e)
                } catch (e: IllegalStateException) {
                    logger.warn("Failed to close advisory dispatcher", e)
                }
            }
            dispatcher = null
        }
    }

    companion object {
        private const val MESSAGE_NOT_FOUND_ERROR_CODE = 10037
    }
}
