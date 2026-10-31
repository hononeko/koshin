package dev.hononeko.koshin.adapters.nats

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RepoRef
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import io.nats.client.Connection
import io.nats.client.JetStreamApiException
import io.nats.client.JetStreamManagement
import io.nats.client.ObjectStore
import io.nats.client.api.ObjectInfo
import io.nats.client.api.ObjectMeta
import io.nats.client.api.StreamConfiguration
import io.nats.client.api.StreamInfo
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.nio.file.Path
import kotlin.io.path.createDirectories
import kotlin.io.path.createSymbolicLinkPointingTo
import kotlin.io.path.exists
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.readBytes
import kotlin.io.path.writeText
import kotlin.time.Duration.Companion.days

class NatsObjectStoreRepoCacheUnitTest {

    @TempDir
    lateinit var cacheDir: Path

    private val repo = RepoRef(Platform.GITLAB, "group/sub/project")
    private val objectName = "gitlab/group/sub/project.json"
    private val store = mockk<ObjectStore>()
    private val jsm = mockk<JetStreamManagement>()
    private val connection = mockk<Connection> {
        every { objectStore("caches") } returns store
        every { jetStreamManagement() } returns jsm
    }
    private val config = NatsRepoCacheConfig(bucket = "caches", replicas = 3, maxBytes = 1_000_000, ttl = 30.days)
    private val cache by lazy { NatsObjectStoreRepoCache(connection, config, cacheDir) }

    private fun info(size: Long) = mockk<ObjectInfo> { every { this@mockk.size } returns size }

    private fun apiError(code: Int) = mockk<JetStreamApiException> {
        every { apiErrorCode } returns code
        every { errorCode } returns code
    }

    @Test
    fun `restore downloads the object into Renovate's cache file`() = runBlocking {
        val content = "{\"repository\":\"group/sub/project\"}".toByteArray()
        every { store.getInfo(objectName) } returns info(content.size.toLong())
        every { store.get(objectName, any()) } answers {
            secondArg<OutputStream>().write(content)
            info(content.size.toLong())
        }

        assertTrue(cache.restore(repo))

        val file = cacheDir.resolve("renovate/repository/gitlab/group/sub/project.json")
        assertArrayEquals(content, file.readBytes())
        assertEquals(listOf(file), file.parent.listDirectoryEntries(), "no partial download left behind")
    }

    @Test
    fun `restore miss deletes a leftover file and returns false`() = runBlocking {
        every { store.getInfo(objectName) } returns null
        val file = cache.localFile(repo).apply { parent.createDirectories() }
        file.writeText("left by a failed run")

        assertFalse(cache.restore(repo))
        assertFalse(file.exists())
    }

    @Test
    fun `restore failure leaves no partial file and returns false`() = runBlocking {
        every { store.getInfo(objectName) } returns info(10)
        every { store.get(objectName, any()) } answers {
            secondArg<OutputStream>().write("par".toByteArray())
            throw IOException("connection lost")
        }

        assertFalse(cache.restore(repo))
        assertEquals(emptyList<Path>(), cache.localFile(repo).parent.listDirectoryEntries())
    }

    @Test
    fun `restore and persist refuse a symlinked directory`() = runBlocking {
        val outside = cacheDir.resolve("outside").createDirectories()
        cacheDir.resolve("renovate/repository/gitlab").createDirectories()
        cacheDir.resolve("renovate/repository/gitlab/group").createSymbolicLinkPointingTo(outside)

        assertFalse(cache.restore(repo))
        cache.persist(repo)

        verify(exactly = 0) { store.getInfo(any()) }
        verify(exactly = 0) { store.put(any<ObjectMeta>(), any<InputStream>()) }
    }

    @Test
    fun `restore rethrows cancellation`() {
        every { store.getInfo(objectName) } throws CancellationException("stopping")

        assertThrows<CancellationException> { runBlocking { cache.restore(repo) } }
    }

    @Test
    fun `persist uploads Renovate's cache file under the platform-prefixed name`() = runBlocking {
        cache.localFile(repo).apply { parent.createDirectories() }.writeText("cache")
        val meta = slot<ObjectMeta>()
        val uploaded = slot<String>()
        every { store.put(capture(meta), any<InputStream>()) } answers {
            uploaded.captured = secondArg<InputStream>().readBytes().decodeToString()
            info(5)
        }

        cache.persist(repo)

        assertEquals(objectName, meta.captured.objectName)
        assertEquals("cache", uploaded.captured)
    }

    @Test
    fun `persist skips a missing file and swallows upload errors`() = runBlocking {
        cache.persist(repo)
        verify(exactly = 0) { store.put(any<ObjectMeta>(), any<InputStream>()) }

        cache.localFile(repo).apply { parent.createDirectories() }.writeText("cache")
        every { store.put(any<ObjectMeta>(), any<InputStream>()) } throws IOException("bucket full")
        cache.persist(repo)
        verify(exactly = 1) { store.put(any<ObjectMeta>(), any<InputStream>()) }
    }

    @Test
    fun `persist rethrows cancellation`() {
        cache.localFile(repo).apply { parent.createDirectories() }.writeText("cache")
        every { store.put(any<ObjectMeta>(), any<InputStream>()) } throws CancellationException("stopping")

        assertThrows<CancellationException> { runBlocking { cache.persist(repo) } }
    }

    @Test
    fun `ensureBucket creates the backing stream with the configured limits`() {
        val created = slot<StreamConfiguration>()
        every { jsm.getStreamInfo("OBJ_caches") } throws apiError(10059)
        every { jsm.addStream(capture(created)) } returns mockk<StreamInfo>()

        cache.ensureBucket()

        assertEquals("OBJ_caches", created.captured.name)
        assertEquals(3, created.captured.replicas)
        assertEquals(1_000_000, created.captured.maxBytes)
        assertEquals(java.time.Duration.ofDays(30), created.captured.maxAge)
    }

    @Test
    fun `ensureBucket updates an existing bucket`() {
        every { jsm.getStreamInfo("OBJ_caches") } returns mockk<StreamInfo>()
        every { jsm.updateStream(any()) } returns mockk<StreamInfo>()

        cache.ensureBucket()

        verify(exactly = 1) { jsm.updateStream(any()) }
        verify(exactly = 0) { jsm.addStream(any()) }
    }

    @Test
    fun `ensureBucket updates when another worker created the bucket first`() {
        every { jsm.getStreamInfo("OBJ_caches") } throws apiError(10059)
        every { jsm.addStream(any()) } throws apiError(10058)
        every { jsm.updateStream(any()) } returns mockk<StreamInfo>()

        cache.ensureBucket()

        verify(exactly = 1) { jsm.updateStream(any()) }
    }

    @Test
    fun `ensureBucket propagates other JetStream errors`() {
        every { jsm.getStreamInfo("OBJ_caches") } throws apiError(10039)
        assertThrows<JetStreamApiException> { cache.ensureBucket() }

        every { jsm.getStreamInfo("OBJ_caches") } throws apiError(10059)
        every { jsm.addStream(any()) } throws apiError(10047)
        assertThrows<JetStreamApiException> { cache.ensureBucket() }
    }
}
