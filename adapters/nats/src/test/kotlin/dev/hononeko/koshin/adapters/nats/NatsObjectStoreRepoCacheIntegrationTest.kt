package dev.hononeko.koshin.adapters.nats

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RepoRef
import io.nats.client.Connection
import io.nats.client.Nats
import io.nats.client.Options
import io.nats.client.api.StorageType
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import kotlin.io.path.createDirectories
import kotlin.io.path.createSymbolicLinkPointingTo
import kotlin.io.path.exists
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.readBytes
import kotlin.io.path.readText
import kotlin.io.path.writeBytes
import kotlin.io.path.writeText
import kotlin.random.Random
import kotlin.time.Duration.Companion.days
import kotlin.time.toJavaDuration

class NatsObjectStoreRepoCacheIntegrationTest {
    companion object {
        private lateinit var connection: Connection
        private val ids = AtomicInteger()

        @BeforeAll
        @JvmStatic
        fun setUp() {
            connection = Nats.connect(Options.Builder().server(SharedNatsContainer.natsUrl).build())
        }

        @AfterAll
        @JvmStatic
        fun tearDown() {
            connection.close()
        }
    }

    @TempDir
    lateinit var workerA: Path

    @TempDir
    lateinit var workerB: Path

    private val repo = RepoRef(Platform.GITHUB, "hononeko/koshin")
    private val config = NatsRepoCacheConfig(
        bucket = "repo-cache-${ids.incrementAndGet()}",
        storageType = StorageType.Memory,
    )

    private fun cache(cacheDir: Path, cfg: NatsRepoCacheConfig = config) =
        NatsObjectStoreRepoCache(connection, cfg, cacheDir).also { it.ensureBucket() }

    @Test
    fun `cache file follows Renovate's local layout and object name is platform-prefixed`() {
        val cache = NatsObjectStoreRepoCache(connection, config, workerA)
        assertEquals(workerA.resolve("renovate/repository/github/hononeko/koshin.json"), cache.localFile(repo))
        assertEquals("github/hononeko/koshin.json", cache.objectName(repo))
        val gitlab = RepoRef(Platform.GITLAB, "group/sub/project")
        assertEquals(workerA.resolve("renovate/repository/gitlab/group/sub/project.json"), cache.localFile(gitlab))
        assertEquals("gitlab/group/sub/project.json", cache.objectName(gitlab))
    }

    @Test
    fun `cache persisted by worker A is restored identically by worker B`() = runBlocking {
        val a = cache(workerA)
        val b = cache(workerB)
        // Several 128 KiB chunks, so the multi-chunk path is covered
        val content = Random(42).nextBytes(300_000)
        a.localFile(repo).apply { parent.createDirectories() }.writeBytes(content)

        a.persist(repo)

        assertTrue(b.restore(repo))
        assertArrayEquals(content, b.localFile(repo).readBytes())
        assertEquals(listOf(b.localFile(repo)), b.localFile(repo).parent.listDirectoryEntries())
    }

    @Test
    fun `restore overwrites a stale local file and persist replaces the stored object`() = runBlocking {
        val a = cache(workerA)
        val b = cache(workerB)
        a.localFile(repo).apply { parent.createDirectories() }.writeText("v1")
        a.persist(repo)
        b.localFile(repo).apply { parent.createDirectories() }.writeText("stale")

        assertTrue(b.restore(repo))
        assertEquals("v1", String(b.localFile(repo).readBytes()))

        b.localFile(repo).writeText("v2")
        b.persist(repo)
        assertTrue(a.restore(repo))
        assertEquals("v2", String(a.localFile(repo).readBytes()))
    }

    @Test
    fun `missing object restores nothing and drops a leftover local file so the run proceeds cold`() = runBlocking {
        val b = cache(workerB)
        b.localFile(repo).apply { parent.createDirectories() }.writeText("left by a failed run")

        assertFalse(b.restore(repo))
        assertFalse(b.localFile(repo).exists())
    }

    @Test
    fun `symlinks in the cache dir are never followed`() = runBlocking {
        val a = cache(workerA)
        a.localFile(repo).apply { parent.createDirectories() }.writeText("v1")
        a.persist(repo)
        val outside = workerB.resolve("outside").createDirectories()
        outside.resolve("koshin.json").writeText("secret")
        val b = cache(workerB.resolve("cache"))
        val ownerDir = b.localFile(repo).parent
        ownerDir.parent.createDirectories()
        ownerDir.createSymbolicLinkPointingTo(outside)

        assertFalse(b.restore(repo))
        assertEquals("secret", outside.resolve("koshin.json").readText())
        b.persist(repo)
        assertTrue(a.restore(repo))
        assertEquals("v1", a.localFile(repo).readText())
    }

    @Test
    fun `persist without a cache file uploads nothing`() = runBlocking {
        val a = cache(workerA)

        a.persist(repo)

        assertNull(connection.objectStore(config.bucket).getInfo(a.objectName(repo)))
    }

    @Test
    fun `missing bucket fails soft`() = runBlocking {
        val missing = NatsObjectStoreRepoCache(connection, config.copy(bucket = "no-such-bucket"), workerA)
        missing.localFile(repo).apply { parent.createDirectories() }.writeText("{}")

        assertFalse(missing.restore(repo))
        missing.persist(repo)
    }

    @Test
    fun `ensureBucket is idempotent and applies size cap and TTL changes`() {
        cache(workerA)
        cache(workerA, config.copy(maxBytes = 10_000_000, ttl = 30.days))

        val status = connection.objectStoreManagement().getStatus(config.bucket)
        assertEquals(10_000_000, status.maxBucketSize)
        assertEquals(30.days.toJavaDuration(), status.ttl)
    }
}
