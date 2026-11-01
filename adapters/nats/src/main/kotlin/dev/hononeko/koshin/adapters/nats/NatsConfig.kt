package dev.hononeko.koshin.adapters.nats

import dev.hononeko.koshin.core.model.Platform
import io.nats.client.api.StorageType
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

data class NatsConfig(
    val jobsStreamName: String = DEFAULT_JOBS_STREAM,
    val dlqStreamName: String = DEFAULT_DLQ_STREAM,
    val jobsSubjectPrefix: String = DEFAULT_JOBS_SUBJECT_PREFIX,
    val dlqSubjectPrefix: String = DEFAULT_DLQ_SUBJECT_PREFIX,
    val replicas: Int = 1,
    val ackWait: Duration = 60.seconds,
    val maxDeliver: Long = 5L,
    val duplicateWindow: Duration = 2.minutes,
    val dlqMaxAge: Duration = 7.days,
    val dlqForwarderQueueGroup: String = DEFAULT_DLQ_QUEUE_GROUP,
    val consumerPrefix: String = DEFAULT_CONSUMER_PREFIX,
    val storageType: StorageType = StorageType.File,
    /** How often workers look for exhausted jobs whose MaxDeliver advisory nobody handled; zero turns it off. */
    val exhaustedSweepInterval: Duration = DEFAULT_EXHAUSTED_SWEEP_SECONDS.seconds,
) {
    val jobsSubject: String
        get() = "$jobsSubjectPrefix.>"

    val dlqSubject: String
        get() = "$dlqSubjectPrefix.>"

    fun jobsSubjectFor(platform: Platform): String = "$jobsSubjectPrefix.${platform.name.lowercase()}"

    fun dlqSubjectFor(platform: Platform): String = "$dlqSubjectPrefix.${platform.name.lowercase()}"

    fun consumerNameFor(platform: Platform): String = "$consumerPrefix${platform.name.lowercase()}"

    init {
        require(jobsStreamName.isNotBlank()) { "jobsStreamName cannot be blank" }
        require(dlqStreamName.isNotBlank()) { "dlqStreamName cannot be blank" }
        require(jobsSubjectPrefix.isNotBlank()) { "jobsSubjectPrefix cannot be blank" }
        require(dlqSubjectPrefix.isNotBlank()) { "dlqSubjectPrefix cannot be blank" }
        require(replicas >= 1) { "replicas must be at least 1" }
        require(maxDeliver >= 1) { "maxDeliver must be at least 1" }
        require(ackWait.isPositive()) { "ackWait must be positive" }
        require(duplicateWindow.isPositive()) { "duplicateWindow must be positive" }
        require(dlqMaxAge.isPositive()) { "dlqMaxAge must be positive" }
        require(dlqForwarderQueueGroup.isNotBlank()) { "dlqForwarderQueueGroup cannot be blank" }
        require(consumerPrefix.isNotBlank()) { "consumerPrefix cannot be blank" }
        require(!exhaustedSweepInterval.isNegative()) { "exhaustedSweepInterval cannot be negative" }
    }

    companion object {
        const val DEFAULT_JOBS_STREAM = "RENOVATE_JOBS"
        const val DEFAULT_DLQ_STREAM = "RENOVATE_DLQ"
        const val DEFAULT_JOBS_SUBJECT_PREFIX = "renovate.jobs"
        const val DEFAULT_DLQ_SUBJECT_PREFIX = "renovate.dlq"
        const val DEFAULT_DLQ_QUEUE_GROUP = "koshin-dlq-forwarders"
        const val DEFAULT_CONSUMER_PREFIX = "koshin-workers-"
        const val DEFAULT_REPLICAS = 1
        const val DEFAULT_ACK_WAIT_SECONDS = 60L
        const val DEFAULT_MAX_DELIVER = 5L
        const val DEFAULT_DUPLICATE_WINDOW_MINUTES = 2L
        const val DEFAULT_DLQ_MAX_AGE_DAYS = 7L
        const val DEFAULT_EXHAUSTED_SWEEP_SECONDS = 300L

        fun fromEnv(env: Map<String, String> = System.getenv()): NatsConfig = NatsConfig(
            jobsStreamName = env["NATS_STREAM_NAME"] ?: env["NATS_JOBS_STREAM"] ?: DEFAULT_JOBS_STREAM,
            dlqStreamName = env["NATS_DLQ_STREAM"] ?: DEFAULT_DLQ_STREAM,
            jobsSubjectPrefix = env["NATS_JOBS_SUBJECT_PREFIX"] ?: DEFAULT_JOBS_SUBJECT_PREFIX,
            dlqSubjectPrefix = env["NATS_DLQ_SUBJECT_PREFIX"] ?: DEFAULT_DLQ_SUBJECT_PREFIX,
            replicas = parseEnvInt(env, "NATS_STREAM_REPLICAS", DEFAULT_REPLICAS),
            ackWait = parseEnvLong(env, "NATS_ACK_WAIT_SECONDS", DEFAULT_ACK_WAIT_SECONDS).seconds,
            maxDeliver = parseEnvLong(env, "NATS_MAX_DELIVER", DEFAULT_MAX_DELIVER),
            duplicateWindow = parseEnvLong(
                env,
                "NATS_DUPLICATE_WINDOW_MINUTES",
                DEFAULT_DUPLICATE_WINDOW_MINUTES,
            ).minutes,
            dlqMaxAge = parseEnvLong(
                env,
                "NATS_DLQ_MAX_AGE_DAYS",
                DEFAULT_DLQ_MAX_AGE_DAYS,
            ).days,
            dlqForwarderQueueGroup = env["NATS_DLQ_FORWARDER_QUEUE_GROUP"] ?: DEFAULT_DLQ_QUEUE_GROUP,
            consumerPrefix = env["NATS_CONSUMER_PREFIX"] ?: DEFAULT_CONSUMER_PREFIX,
            storageType = parseEnvStorageType(env["NATS_STORAGE_TYPE"]),
            exhaustedSweepInterval = parseEnvLong(
                env,
                "NATS_EXHAUSTED_SWEEP_SECONDS",
                DEFAULT_EXHAUSTED_SWEEP_SECONDS,
            ).seconds,
        )

        private fun parseEnvInt(env: Map<String, String>, key: String, default: Int): Int = env[key]?.let { raw ->
            requireNotNull(raw.toIntOrNull()) { "Invalid integer for $key: '$raw'" }
        } ?: default

        private fun parseEnvLong(env: Map<String, String>, key: String, default: Long): Long = env[key]?.let { raw ->
            requireNotNull(raw.toLongOrNull()) { "Invalid long for $key: '$raw'" }
        } ?: default

        private fun parseEnvStorageType(raw: String?): StorageType = raw?.let {
            requireNotNull(StorageType.get(it)) { "Invalid NATS_STORAGE_TYPE: '$it'" }
        } ?: StorageType.File
    }
}
