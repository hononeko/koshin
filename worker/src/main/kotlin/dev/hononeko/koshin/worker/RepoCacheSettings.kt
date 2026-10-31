package dev.hononeko.koshin.worker

import dev.hononeko.koshin.adapters.nats.NatsConfig
import dev.hononeko.koshin.adapters.nats.NatsRepoCacheConfig
import java.nio.file.Path
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days

enum class RepoCacheProvider {
    /** koshin syncs Renovate's local cache file with a NATS Object Store bucket. */
    NATS,

    /** Renovate reads and writes the cache in S3 itself (`repositoryCacheType=s3://…`); koshin does no sync. */
    S3,

    /** The cache stays in the pod's `emptyDir`. */
    LOCAL,
    ;

    companion object {
        fun fromString(raw: String): RepoCacheProvider = entries.firstOrNull { it.name.equals(raw.trim(), true) }
            ?: throw IllegalArgumentException("Invalid REPO_CACHE_PROVIDER: '$raw' (nats, s3, local)")
    }
}

/** Renovate repository cache (KSH-10). */
data class RepoCacheSettings(
    val provider: RepoCacheProvider = RepoCacheProvider.NATS,
    val bucket: String = NatsRepoCacheConfig.DEFAULT_BUCKET,
    val maxBytes: Long? = null,
    val ttl: Duration? = null,
    /** `s3://<bucket>/<prefix>/`, required for [RepoCacheProvider.S3]. */
    val s3Url: String? = null,
    val s3Endpoint: String? = null,
    val s3PathStyle: Boolean = true,
) {
    init {
        require(provider != RepoCacheProvider.S3 || s3Url?.startsWith("s3://") == true) {
            "REPO_CACHE_PROVIDER=s3 requires RENOVATE_REPOSITORY_CACHE_TYPE=s3://<bucket>/<prefix>/: '$s3Url'"
        }
        if (provider == RepoCacheProvider.NATS) {
            NatsRepoCacheConfig(bucket = bucket, maxBytes = maxBytes, ttl = ttl)
        }
    }

    /** Object Store settings when koshin syncs the cache through NATS, else `null` (nothing to sync). */
    fun natsConfig(nats: NatsConfig): NatsRepoCacheConfig? = if (provider == RepoCacheProvider.NATS) {
        NatsRepoCacheConfig(bucket, nats.replicas, nats.storageType, maxBytes, ttl)
    } else {
        null
    }

    /** Renovate's repository cache options for every run; they override any inherited values. */
    fun renovateEnv(cacheDir: Path): Map<String, String> = buildMap {
        put("RENOVATE_REPOSITORY_CACHE", "enabled")
        when (provider) {
            RepoCacheProvider.NATS, RepoCacheProvider.LOCAL -> {
                put("RENOVATE_REPOSITORY_CACHE_TYPE", "local")
                // Pinned, so the NATS sync and Renovate agree on where the cache file lives.
                put("RENOVATE_CACHE_DIR", cacheDir.toString())
            }

            RepoCacheProvider.S3 -> {
                put("RENOVATE_REPOSITORY_CACHE_TYPE", checkNotNull(s3Url))
                s3Endpoint?.let { put("RENOVATE_S3_ENDPOINT", it) }
                put("RENOVATE_S3_PATH_STYLE", s3PathStyle.toString())
            }
        }
    }

    companion object {
        fun fromEnv(env: Map<String, String>): RepoCacheSettings = RepoCacheSettings(
            provider = env.nonBlank("REPO_CACHE_PROVIDER")?.let(RepoCacheProvider::fromString)
                ?: RepoCacheProvider.NATS,
            bucket = env.nonBlank("REPO_CACHE_BUCKET") ?: NatsRepoCacheConfig.DEFAULT_BUCKET,
            maxBytes = env.long("REPO_CACHE_MAX_BYTES"),
            ttl = env.long("REPO_CACHE_TTL_DAYS")?.days,
            s3Url = env.nonBlank("RENOVATE_REPOSITORY_CACHE_TYPE"),
            // Renovate still reads the older RENOVATE_X_S3_* names as aliases.
            s3Endpoint = env.nonBlank("RENOVATE_S3_ENDPOINT") ?: env.nonBlank("RENOVATE_X_S3_ENDPOINT"),
            s3PathStyle = env.boolean("RENOVATE_S3_PATH_STYLE") ?: env.boolean("RENOVATE_X_S3_PATH_STYLE") ?: true,
        )
    }
}
