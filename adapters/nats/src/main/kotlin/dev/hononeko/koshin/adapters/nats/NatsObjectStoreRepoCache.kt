package dev.hononeko.koshin.adapters.nats

import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.ports.RepoCachePort
import io.nats.client.Connection
import io.nats.client.JetStreamApiException
import io.nats.client.api.ObjectMeta
import io.nats.client.api.ObjectStoreConfiguration
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import kotlin.io.path.createDirectories
import kotlin.io.path.createTempFile
import kotlin.io.path.deleteIfExists
import kotlin.io.path.exists
import kotlin.io.path.inputStream
import kotlin.io.path.isRegularFile
import kotlin.io.path.isSymbolicLink
import kotlin.io.path.moveTo
import kotlin.io.path.outputStream
import kotlin.time.TimeSource
import kotlin.time.toJavaDuration

/**
 * Keeps Renovate's local repository cache in a JetStream Object Store bucket, so it follows the repo across pods.
 *
 * Renovate (`repositoryCacheType=local`) reads and writes `<cacheDir>/renovate/repository/<platform>/<repo>.json`.
 * [restore] replaces that file with `<platform>/<repo>.json` before a run (or deletes it, so a leftover from an
 * unpersisted run is never used), and [persist] uploads it after a successful one. Both run under the repo lock, so
 * there's a single writer. Cache errors never fail a run: they're logged and the run goes ahead cold, or the upload is
 * skipped. Renovate can run repo code, so no path below `cacheDir` may be a symlink.
 */
class NatsObjectStoreRepoCache(
    private val connection: Connection,
    private val config: NatsRepoCacheConfig,
    cacheDir: Path,
    private val ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
) : RepoCachePort {

    private val logger = LoggerFactory.getLogger(NatsObjectStoreRepoCache::class.java)
    private val cacheRoot: Path = cacheDir.toAbsolutePath().normalize()
    private val repoCacheDir: Path = cacheRoot.resolve(RENOVATE_REPO_CACHE_DIR)

    /** Creates the bucket, or updates its replicas / size cap / TTL. Run on every (re)connect. */
    fun ensureBucket() {
        val backing = ObjectStoreConfiguration.builder(config.bucket)
            .description("Renovate repository caches (koshin)")
            .storageType(config.storageType)
            .replicas(config.replicas)
            .apply {
                config.maxBytes?.let { maxBucketSize(it) }
                config.ttl?.let { ttl(it.toJavaDuration()) }
            }
            .build()
            .backingConfig
        val jsm = connection.jetStreamManagement()
        val exists = try {
            jsm.getStreamInfo(backing.name)
            true
        } catch (e: JetStreamApiException) {
            if (!e.hasCode(STREAM_NOT_FOUND_ERROR_CODE)) throw e
            false
        }
        if (exists) {
            jsm.updateStream(backing)
        } else {
            try {
                jsm.addStream(backing)
            } catch (e: JetStreamApiException) {
                // Another worker created it first, with a different config
                if (!e.hasCode(STREAM_NAME_IN_USE_ERROR_CODE)) throw e
                jsm.updateStream(backing)
            }
        }
        logger.info("Ensured repo cache bucket {} (replicas: {})", config.bucket, config.replicas)
    }

    fun objectName(repo: RepoRef): String = "${repo.platform.name.lowercase()}/${repo.repository}.json"

    fun localFile(repo: RepoRef): Path {
        val file = repoCacheDir.resolve(repo.platform.name.lowercase()).resolve("${repo.repository}.json").normalize()
        require(file.startsWith(repoCacheDir)) { "Repo cache file for $repo escapes $repoCacheDir" }
        return file
    }

    override suspend fun restore(repo: RepoRef): Boolean = withContext(ioDispatcher) {
        val started = TimeSource.Monotonic.markNow()
        try {
            val target = localFile(repo)
            requireNoSymlinks(target.parent)
            // The bucket is the only source: never run on what a failed or timed-out run left behind.
            target.deleteIfExists()
            val store = connection.objectStore(config.bucket)
            val name = objectName(repo)
            val info = store.getInfo(name)
            if (info == null) {
                logger.atInfo().addKeyValue("repo", repo.toString()).addKeyValue("cache", "miss")
                    .log("No shared repo cache for {}; running cold", repo)
                return@withContext false
            }
            target.parent.createDirectories()
            // Download beside the target and rename, so Renovate never sees a partial file.
            val partial = createTempFile(target.parent, ".koshin-", ".part")
            try {
                partial.outputStream().use { store.get(name, it) }
                partial.moveTo(target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
            } finally {
                partial.deleteIfExists()
            }
            logger.atInfo().addKeyValue("repo", repo.toString()).addKeyValue("cache", "hit")
                .addKeyValue("bytes", info.size).addKeyValue("durationMs", started.elapsedNow().inWholeMilliseconds)
                .log("Restored repo cache for {} ({} bytes)", repo, info.size)
            true
        } catch (e: CancellationException) {
            throw e
        } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
            logger.atWarn().addKeyValue("repo", repo.toString()).addKeyValue("cache", "error").setCause(e)
                .log("Could not restore repo cache for {}; running cold", repo)
            false
        }
    }

    override suspend fun persist(repo: RepoRef) = withContext(ioDispatcher) {
        val started = TimeSource.Monotonic.markNow()
        try {
            val source = localFile(repo)
            requireNoSymlinks(source.parent)
            if (!source.isRegularFile(LinkOption.NOFOLLOW_LINKS)) {
                logger.debug("Renovate wrote no repo cache for {}; nothing to persist", repo)
                return@withContext
            }
            val store = connection.objectStore(config.bucket)
            val info = source.inputStream().use { store.put(ObjectMeta.objectName(objectName(repo)), it) }
            logger.atInfo().addKeyValue("repo", repo.toString()).addKeyValue("cache", "persisted")
                .addKeyValue("bytes", info.size).addKeyValue("durationMs", started.elapsedNow().inWholeMilliseconds)
                .log("Persisted repo cache for {} ({} bytes)", repo, info.size)
        } catch (e: CancellationException) {
            throw e
        } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
            logger.atWarn().addKeyValue("repo", repo.toString()).addKeyValue("cache", "error").setCause(e)
                .log("Could not persist repo cache for {}", repo)
        }
    }

    /** Fails if any existing path from below `cacheDir` down to [dir] is a symlink. */
    private fun requireNoSymlinks(dir: Path) {
        var current = cacheRoot
        for (segment in cacheRoot.relativize(dir)) {
            current = current.resolve(segment)
            if (!current.exists(LinkOption.NOFOLLOW_LINKS)) return
            check(!current.isSymbolicLink()) { "Refusing to follow symlink $current in the repo cache dir" }
        }
    }

    private fun JetStreamApiException.hasCode(code: Int) = apiErrorCode == code || errorCode == code

    companion object {
        /** Renovate's local repository cache layout under `cacheDir` (verified against Renovate 44.178.3). */
        const val RENOVATE_REPO_CACHE_DIR = "renovate/repository"
        private const val STREAM_NOT_FOUND_ERROR_CODE = 10059
        private const val STREAM_NAME_IN_USE_ERROR_CODE = 10058
    }
}
