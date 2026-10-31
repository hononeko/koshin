package dev.hononeko.koshin.worker

import dev.hononeko.koshin.adapters.nats.NatsConfig
import dev.hononeko.koshin.adapters.nats.NatsRepoCacheConfig
import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.PlatformCredentials
import dev.hononeko.koshin.core.model.RepoRef
import io.nats.client.api.StorageType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import kotlin.io.path.Path
import kotlin.time.Duration.Companion.days

class RepoCacheSettingsTest {

    private val baseDir = Path("/tmp/renovate")
    private val repo = RepoRef(Platform.GITHUB, "hononeko/koshin")
    private val creds = PlatformCredentials("ghp_secret")

    private fun environment(repoCache: RepoCacheSettings, inherited: Map<String, String> = emptyMap()) =
        RenovateProcessExecutor(baseDir = baseDir, inheritedEnv = inherited, repoCache = repoCache)
            .buildEnvironment(repo, creds)

    @Test
    fun `nats mode syncs through the bucket and pins Renovate to its local cache file`() {
        val settings = RepoCacheSettings(bucket = "caches", maxBytes = 1_000, ttl = 7.days)
        assertEquals(
            NatsRepoCacheConfig(
                bucket = "caches",
                replicas = 3,
                storageType = StorageType.Memory,
                maxBytes = 1_000,
                ttl = 7.days,
            ),
            settings.natsConfig(NatsConfig(replicas = 3, storageType = StorageType.Memory)),
        )

        val env = environment(settings, mapOf("RENOVATE_REPOSITORY_CACHE_TYPE" to "s3://ignored/"))
        assertEquals("enabled", env["RENOVATE_REPOSITORY_CACHE"])
        assertEquals("local", env["RENOVATE_REPOSITORY_CACHE_TYPE"])
        assertEquals("/tmp/renovate/cache", env["RENOVATE_CACHE_DIR"])
    }

    @Test
    fun `local mode keeps the cache in the pod without sync`() {
        val settings = RepoCacheSettings(provider = RepoCacheProvider.LOCAL)
        assertNull(settings.natsConfig(NatsConfig()))
        assertEquals("local", environment(settings)["RENOVATE_REPOSITORY_CACHE_TYPE"])
    }

    @Test
    fun `s3 mode passes Renovate the S3 cache settings and does no sync`() {
        val settings = RepoCacheSettings(
            provider = RepoCacheProvider.S3,
            s3Url = "s3://renovate/repo-cache/",
            s3Endpoint = "http://seaweedfs:8333",
        )
        assertNull(settings.natsConfig(NatsConfig()))

        val env = environment(
            settings,
            mapOf(
                "AWS_ACCESS_KEY_ID" to "key",
                "AWS_SECRET_ACCESS_KEY" to "secret",
                "AWS_REGION" to "us-east-1",
                "RENOVATE_X_S3_ENDPOINT" to "http://legacy:8333",
                "RENOVATE_X_S3_PATH_STYLE" to "false",
            ),
        )
        assertEquals("enabled", env["RENOVATE_REPOSITORY_CACHE"])
        assertEquals("s3://renovate/repo-cache/", env["RENOVATE_REPOSITORY_CACHE_TYPE"])
        assertEquals("http://seaweedfs:8333", env["RENOVATE_S3_ENDPOINT"])
        assertEquals("true", env["RENOVATE_S3_PATH_STYLE"])
        assertEquals("key", env["AWS_ACCESS_KEY_ID"])
        assertEquals("secret", env["AWS_SECRET_ACCESS_KEY"])
        assertEquals("us-east-1", env["AWS_REGION"])
        // Renovate would let these aliases override the values above
        assertFalse(env.containsKey("RENOVATE_X_S3_ENDPOINT"))
        assertFalse(env.containsKey("RENOVATE_X_S3_PATH_STYLE"))
        assertFalse(env.containsKey("RENOVATE_CACHE_DIR"))
    }
}
