package dev.hononeko.koshin.adapters.nats

import dev.hononeko.koshin.core.model.CredentialScope
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
    private val scope = CredentialScope.Installation(42)
    private val config = NatsRepoCacheConfig(
        bucket = "repo-cache-${ids.incrementAndGet()}",
        storageType = StorageType.Memory,
    )

    private fun cache(cacheDir: Path, cfg: NatsRepoCacheConfig = config) =
        NatsObjectStoreRepoCache(connection, cfg, cacheDir).also { it.ensureBucket() }

    @Test
    fun `cache file follows Renovate's local layout and object name is platform- and scope-prefixed`() {
        val cache = NatsObjectStoreRepoCache(connection, config, workerA)
        assertEquals(workerA.resolve("renovate/repository/github/hononeko/koshin.json"), cache.localFile(repo))
        assertEquals("github/inst-42/hononeko/koshin.json", cache.objectName(repo, scope))
        val gitlab = RepoRef(Platform.GITLAB, "group/sub/project")
        assertEquals(workerA.resolve("renovate/repository/gitlab/group/sub/project.json"), cache.localFile(gitlab))
        val gitlabScope = CredentialScope.Token(Platform.GITLAB)
        assertEquals("gitlab/pat/group/sub/project.json", cache.objectName(gitlab, gitlabScope))
    }

    @Test
    fun `two installations with the same owner and repo never read each other's cache (KSH-26)`() = runBlocking {
        val a = cache(workerA)
        val b = cache(workerB)
        val other = CredentialScope.Installation(43)
        a.localFile(repo).apply { parent.createDirectories() }.writeText("installation 42's dependency tree")
        a.persist(repo, scope)

        // The account was renamed and its name taken, or the repo transferred: installation 43 now has the slug
        assertFalse(b.restore(repo, other))
        assertFalse(b.localFile(repo).exists())

        b.localFile(repo).apply { parent.createDirectories() }.writeText("installation 43's")
        b.persist(repo, other)
        assertTrue(a.restore(repo, scope))
        assertEquals("installation 42's dependency tree", a.localFile(repo).readText())
        assertTrue(b.restore(repo, other))
        assertEquals("installation 43's", b.localFile(repo).readText())
    }

    @Test
    fun `cache persisted by worker A is restored identically by worker B`() = runBlocking {
        val a = cache(workerA)
        val b = cache(workerB)
        // Several 128 KiB chunks, so the multi-chunk path is covered
        val content = Random(42).nextBytes(300_000)
        a.localFile(repo).apply { parent.createDirectories() }.writeBytes(content)

        a.persist(repo, scope)

        assertTrue(b.restore(repo, scope))
        assertArrayEquals(content, b.localFile(repo).readBytes())
        assertEquals(listOf(b.localFile(repo)), b.localFile(repo).parent.listDirectoryEntries())
    }

    @Test
    fun `restore overwrites a stale local file and persist replaces the stored object`() = runBlocking {
        val a = cache(workerA)
        val b = cache(workerB)
        a.localFile(repo).apply { parent.createDirectories() }.writeText("v1")
        a.persist(repo, scope)
        b.localFile(repo).apply { parent.createDirectories() }.writeText("stale")

        assertTrue(b.restore(repo, scope))
        assertEquals("v1", String(b.localFile(repo).readBytes()))

        b.localFile(repo).writeText("v2")
        b.persist(repo, scope)
        assertTrue(a.restore(repo, scope))
        assertEquals("v2", String(a.localFile(repo).readBytes()))
    }

    @Test
    fun `missing object restores nothing and drops a leftover local file so the run proceeds cold`() = runBlocking {
        val b = cache(workerB)
        b.localFile(repo).apply { parent.createDirectories() }.writeText("left by a failed run")

        assertFalse(b.restore(repo, scope))
        assertFalse(b.localFile(repo).exists())
    }

    @Test
    fun `symlinks in the cache dir are never followed`() = runBlocking {
        val a = cache(workerA)
        a.localFile(repo).apply { parent.createDirectories() }.writeText("v1")
        a.persist(repo, scope)
        val outside = workerB.resolve("outside").createDirectories()
        outside.resolve("koshin.json").writeText("secret")
        val b = cache(workerB.resolve("cache"))
        val ownerDir = b.localFile(repo).parent
        ownerDir.parent.createDirectories()
        ownerDir.createSymbolicLinkPointingTo(outside)

        assertFalse(b.restore(repo, scope))
        assertEquals("secret", outside.resolve("koshin.json").readText())
        b.persist(repo, scope)
        assertTrue(a.restore(repo, scope))
        assertEquals("v1", a.localFile(repo).readText())
    }

    @Test
    fun `persist without a cache file uploads nothing`() = runBlocking {
        val a = cache(workerA)

        a.persist(repo, scope)

        assertNull(connection.objectStore(config.bucket).getInfo(a.objectName(repo, scope)))
    }

    @Test
    fun `missing bucket fails soft`() = runBlocking {
        val missing = NatsObjectStoreRepoCache(connection, config.copy(bucket = "no-such-bucket"), workerA)
        missing.localFile(repo).apply { parent.createDirectories() }.writeText("{}")

        assertFalse(missing.restore(repo, scope))
        missing.persist(repo, scope)
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
