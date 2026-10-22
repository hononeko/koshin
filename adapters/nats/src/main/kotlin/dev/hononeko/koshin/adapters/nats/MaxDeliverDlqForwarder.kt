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
import java.util.concurrent.atomic.AtomicInteger

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

    internal val handledAdvisoriesCount = AtomicInteger(0)

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
        if (!isValidAdvisory(advisory)) {
            return
        }

        handledAdvisoriesCount.incrementAndGet()
        processExhaustedMessageWithRetry(advisory)
    }

    private fun isValidAdvisory(advisory: MaxDeliveriesAdvisory): Boolean = when {
        advisory.stream != null && advisory.stream != config.jobsStreamName -> {
            logger.debug("Ignoring MaxDeliver advisory for unrelated stream: {}", advisory.stream)
            false
        }

        advisory.streamSeq <= 0L -> {
            logger.warn("Ignoring MaxDeliver advisory with invalid streamSeq: {}", advisory.streamSeq)
            false
        }

        else -> true
    }

    private fun processExhaustedMessageWithRetry(
        advisory: MaxDeliveriesAdvisory,
        maxRetries: Int = DEFAULT_MAX_RETRIES,
    ) {
        var attempt = 0
        while (attempt < maxRetries) {
            attempt++
            try {
                val msgInfo = fetchMessageOrNull(advisory.streamSeq) ?: return
                val platform = extractPlatform(msgInfo)
                forwardToDlq(advisory.streamSeq, msgInfo, platform, advisory.deliveries)
                deleteFromWorkQueueWithRetry(advisory.streamSeq)
                return
            } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
                logger.warn(
                    "Attempt {}/{} failed to process exhausted message seq {} from {}: {}",
                    attempt,
                    maxRetries,
                    advisory.streamSeq,
                    config.jobsStreamName,
                    e.message,
                )
                if (attempt >= maxRetries) {
                    logger.error(
                        "Exhausted retries processing advisory for seq {} in stream {}",
                        advisory.streamSeq,
                        config.jobsStreamName,
                        e,
                    )
                    throw e
                }
                Thread.sleep((RETRY_BACKOFF_BASE_MS * attempt).coerceAtMost(RETRY_BACKOFF_MAX_MS))
            }
        }
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

    private fun forwardToDlq(streamSeq: Long, msgInfo: MessageInfo, platform: String, deliveries: Long = 0L) {
        val dlqSubject = "${config.dlqSubjectPrefix}.$platform"
        val dlqMsgId = "dlq:${config.jobsStreamName}:$streamSeq"
        val headers = if (msgInfo.headers != null) Headers(msgInfo.headers) else Headers()

        val originalMsgId = headers.getFirst("Nats-Msg-Id")
        if (originalMsgId != null) {
            headers.put("Koshin-Original-Msg-Id", originalMsgId)
        }
        headers.put("Koshin-Stream-Seq", streamSeq.toString())
        if (deliveries > 0L) {
            headers.put("Koshin-Deliveries", deliveries.toString())
        }

        val publishOptions = PublishOptions.builder()
            .messageId(dlqMsgId)
            .build()

        logger.info(
            "Forwarding exhausted message seq {} from {} to DLQ subject {}",
            streamSeq,
            config.jobsStreamName,
            dlqSubject,
        )
        val ack = js.publish(dlqSubject, headers, msgInfo.data, publishOptions)
        if (ack.isDuplicate) {
            logger.debug("DLQ message {} for sequence {} was detected as duplicate", dlqMsgId, streamSeq)
        }
    }

    private fun deleteFromWorkQueueWithRetry(streamSeq: Long, maxRetries: Int = DEFAULT_MAX_RETRIES) {
        var attempt = 0
        while (attempt < maxRetries) {
            attempt++
            try {
                jsm.deleteMessage(config.jobsStreamName, streamSeq)
                logger.info("Successfully evicted sequence {} from {}", streamSeq, config.jobsStreamName)
                return
            } catch (e: JetStreamApiException) {
                if (e.errorCode == MESSAGE_NOT_FOUND_ERROR_CODE || e.apiErrorCode == MESSAGE_NOT_FOUND_ERROR_CODE) {
                    logger.info("Message {} was already deleted from {}", streamSeq, config.jobsStreamName)
                    return
                } else {
                    logger.warn(
                        "Attempt {}/{} failed to delete message {} from {}: {}",
                        attempt,
                        maxRetries,
                        streamSeq,
                        config.jobsStreamName,
                        e.message,
                    )
                    if (attempt >= maxRetries) {
                        logger.error("Failed to delete message {} after {} attempts", streamSeq, maxRetries, e)
                    } else {
                        Thread.sleep((RETRY_BACKOFF_BASE_MS * attempt).coerceAtMost(RETRY_BACKOFF_MAX_MS))
                    }
                }
            } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
                logger.warn("Attempt {}/{} error deleting message {}: {}", attempt, maxRetries, streamSeq, e.message)
                if (attempt >= maxRetries) {
                    logger.error("Failed to delete message {} after {} attempts", streamSeq, maxRetries, e)
                } else {
                    Thread.sleep((RETRY_BACKOFF_BASE_MS * attempt).coerceAtMost(RETRY_BACKOFF_MAX_MS))
                }
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
        private const val DEFAULT_MAX_RETRIES = 5
        private const val RETRY_BACKOFF_BASE_MS = 100L
        private const val RETRY_BACKOFF_MAX_MS = 1000L
    }
}
