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
    }

    companion object {
        const val DEFAULT_JOBS_STREAM = "RENOVATE_JOBS"
        const val DEFAULT_DLQ_STREAM = "RENOVATE_DLQ"
        const val DEFAULT_JOBS_SUBJECT_PREFIX = "renovate.jobs"
        const val DEFAULT_DLQ_SUBJECT_PREFIX = "renovate.dlq"
        const val DEFAULT_DLQ_QUEUE_GROUP = "koshin-dlq-forwarders"
        const val DEFAULT_CONSUMER_PREFIX = "koshin-workers-"

        fun fromEnv(env: Map<String, String> = System.getenv()): NatsConfig = NatsConfig(
            jobsStreamName = env["NATS_JOBS_STREAM"] ?: DEFAULT_JOBS_STREAM,
            dlqStreamName = env["NATS_DLQ_STREAM"] ?: DEFAULT_DLQ_STREAM,
            jobsSubjectPrefix = env["NATS_JOBS_SUBJECT_PREFIX"] ?: DEFAULT_JOBS_SUBJECT_PREFIX,
            dlqSubjectPrefix = env["NATS_DLQ_SUBJECT_PREFIX"] ?: DEFAULT_DLQ_SUBJECT_PREFIX,
            replicas = env["NATS_STREAM_REPLICAS"]?.toIntOrNull() ?: 1,
            ackWait = env["NATS_ACK_WAIT_SECONDS"]?.toLongOrNull()?.seconds ?: 60.seconds,
            maxDeliver = env["NATS_MAX_DELIVER"]?.toLongOrNull() ?: 5L,
            duplicateWindow = env["NATS_DUPLICATE_WINDOW_MINUTES"]?.toLongOrNull()?.minutes ?: 2.minutes,
            dlqMaxAge = env["NATS_DLQ_MAX_AGE_DAYS"]?.toLongOrNull()?.days ?: 7.days,
            dlqForwarderQueueGroup = env["NATS_DLQ_FORWARDER_QUEUE_GROUP"] ?: DEFAULT_DLQ_QUEUE_GROUP,
            consumerPrefix = env["NATS_CONSUMER_PREFIX"] ?: DEFAULT_CONSUMER_PREFIX,
            storageType = env["NATS_STORAGE_TYPE"]?.let { StorageType.valueOf(it) } ?: StorageType.File,
        )
    }
}
