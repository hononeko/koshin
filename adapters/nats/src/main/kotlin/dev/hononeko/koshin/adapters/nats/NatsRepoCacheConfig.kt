package dev.hononeko.koshin.adapters.nats

import io.nats.client.api.StorageType
import kotlin.time.Duration

/** JetStream Object Store bucket holding Renovate repository caches. */
data class NatsRepoCacheConfig(
    val bucket: String = DEFAULT_BUCKET,
    val replicas: Int = 1,
    val storageType: StorageType = StorageType.File,
    /** Bucket size cap; once full, uploads fail (and are skipped) until old caches expire. `null` = unlimited. */
    val maxBytes: Long? = null,
    /** Drops caches of repos that haven't had a successful run for this long. `null` = keep forever. */
    val ttl: Duration? = null,
) {
    init {
        require(BUCKET_NAME.matches(bucket)) { "Invalid repo cache bucket name: '$bucket'" }
        require(replicas >= 1) { "replicas must be at least 1" }
        require(maxBytes == null || maxBytes > 0) { "maxBytes must be positive: $maxBytes" }
        require(ttl == null || ttl.isPositive()) { "ttl must be positive: $ttl" }
    }

    companion object {
        const val DEFAULT_BUCKET = "koshin-repo-cache"
        private val BUCKET_NAME = Regex("[A-Za-z0-9_-]+")
    }
}
