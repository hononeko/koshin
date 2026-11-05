package dev.hononeko.koshin.adapters.nats

import io.nats.client.Connection
import io.nats.client.JetStreamApiException
import io.nats.client.JetStreamManagement
import io.nats.client.api.RetentionPolicy
import io.nats.client.api.StreamConfiguration
import io.nats.client.api.StreamInfo
import org.slf4j.LoggerFactory
import kotlin.time.toJavaDuration

class NatsJetStreamBootstrap(private val jsm: JetStreamManagement, private val config: NatsConfig = NatsConfig()) {
    constructor(connection: Connection, config: NatsConfig = NatsConfig()) : this(
        connection.jetStreamManagement(),
        config,
    )

    private val logger = LoggerFactory.getLogger(NatsJetStreamBootstrap::class.java)

    /**
     * Ensures the streams. The consumers are per credential scope: publishers create them as they publish,
     * and the proxy's housekeeping tidies them ([NatsScopeConsumers]).
     */
    fun bootstrap() {
        logger.info("Bootstrapping NATS JetStream streams...")
        ensureJobsStream()
        ensureDlqStream()
        logger.info("NATS JetStream bootstrap completed successfully.")
    }

    fun ensureJobsStream(): StreamInfo {
        val streamConfig = StreamConfiguration.builder()
            .name(config.jobsStreamName)
            .subjects(config.jobsSubject)
            .retentionPolicy(RetentionPolicy.WorkQueue)
            .storageType(config.storageType)
            .duplicateWindow(config.duplicateWindow.toJavaDuration())
            .replicas(config.replicas)
            .build()

        return createOrUpdateStream(streamConfig)
    }

    fun ensureDlqStream(): StreamInfo {
        val streamConfig = StreamConfiguration.builder()
            .name(config.dlqStreamName)
            .subjects(config.dlqSubject)
            .retentionPolicy(RetentionPolicy.Limits)
            .storageType(config.storageType)
            .duplicateWindow(config.dlqDuplicateWindow.toJavaDuration())
            .maxAge(config.dlqMaxAge.toJavaDuration())
            .replicas(config.replicas)
            .build()

        return createOrUpdateStream(streamConfig)
    }

    private fun createOrUpdateStream(streamConfig: StreamConfiguration): StreamInfo {
        val streamName = streamConfig.name
        val existing = getStreamInfoOrNull(streamName)
        return if (existing == null) {
            logger.info("Creating stream {} (replicas: {})", streamName, streamConfig.replicas)
            jsm.addStream(streamConfig)
        } else {
            logger.info("Updating existing stream {} (replicas: {})", streamName, streamConfig.replicas)
            jsm.updateStream(streamConfig)
        }
    }

    private fun getStreamInfoOrNull(streamName: String): StreamInfo? = try {
        jsm.getStreamInfo(streamName)
    } catch (e: JetStreamApiException) {
        if (e.errorCode == STREAM_NOT_FOUND_ERROR_CODE || e.apiErrorCode == STREAM_NOT_FOUND_ERROR_CODE) {
            null
        } else {
            throw e
        }
    }

    companion object {
        private const val STREAM_NOT_FOUND_ERROR_CODE = 10059
    }
}
