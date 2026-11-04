package dev.hononeko.koshin.worker

import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.ports.WorkspaceCleanerPort
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import java.nio.file.LinkOption
import java.nio.file.Path
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.createDirectories
import kotlin.io.path.deleteIfExists
import kotlin.io.path.deleteRecursively
import kotlin.io.path.exists
import kotlin.io.path.isDirectory
import kotlin.io.path.isSymbolicLink
import kotlin.io.path.listDirectoryEntries

/**
 * Deletes Renovate clones under `<baseDir>/repos/<platform>/<repository>` (Renovate's default `localDir` layout), and
 * empties Renovate's `cacheDir` after every run (KSH-26): the repository cache file, cached HTTP bodies of private
 * presets and the file package cache would otherwise be there for the next run on the pod, maybe another tenant's.
 * Recursive deletes never follow symlinks.
 */
@OptIn(ExperimentalPathApi::class)
class WorkspaceCleaner(
    baseDir: Path,
    cacheDir: Path = baseDir.resolve(WorkerSettings.RENOVATE_CACHE_SUBDIR),
    private val ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
) : WorkspaceCleanerPort {

    private val logger = LoggerFactory.getLogger(WorkspaceCleaner::class.java)
    private val baseDir: Path = baseDir.toAbsolutePath().normalize()
    private val cacheDir: Path = cacheDir.toAbsolutePath().normalize()
    private val reposDir: Path = this.baseDir.resolve(REPOS_DIR)

    init {
        require(this.baseDir.nameCount > 0) { "Refusing to use filesystem root as workspace base dir" }
        require(this.cacheDir == this.baseDir || !this.baseDir.startsWith(this.cacheDir)) {
            "RENOVATE_CACHE_DIR ${this.cacheDir} contains RENOVATE_BASE_DIR, and is emptied after every run"
        }
    }

    fun workspaceFor(repo: RepoRef): Path {
        val dir = reposDir.resolve(repo.platform.name.lowercase()).resolve(repo.repository).normalize()
        require(dir.startsWith(reposDir) && dir != reposDir) { "Workspace for $repo escapes $reposDir" }
        return dir
    }

    override suspend fun cleanup(repo: RepoRef) {
        val dir = workspaceFor(repo)
        withContext(ioDispatcher) {
            try {
                if (dir.exists(LinkOption.NOFOLLOW_LINKS)) {
                    dir.deleteRecursively()
                    logger.debug("Deleted workspace {} for {}", dir, repo)
                }
                pruneEmptyParents(dir.parent)
            } finally {
                wipeCacheDir()
            }
        }
    }

    /** Wipes everything under the base dir and the cache dir (left over by a previous container in the same pod). */
    override suspend fun purgeAll() {
        withContext(ioDispatcher) {
            baseDir.createDirectories()
            val entries = baseDir.listDirectoryEntries()
            entries.forEach { entry ->
                runCatching { entry.deleteRecursively() }
                    .onFailure { logger.warn("Could not purge {}; leaving it in place", entry, it) }
            }
            if (entries.isNotEmpty()) {
                logger.info("Purged leftover entries from {}", baseDir)
            }
            wipeCacheDir()
        }
    }

    /**
     * Empties the cache dir but keeps it, since it may be a mount point. A symlink in its place, or in place of a dir
     * between the base dir and it, is deleted rather than followed: a run may have left it there.
     */
    private fun wipeCacheDir() {
        val planted = plantedSymlink()
        when {
            planted != null -> runCatching { planted.deleteIfExists() }
                .onSuccess { logger.warn("Deleted symlink {} in place of the Renovate cache dir", planted) }
                .onFailure { logger.warn("Could not delete symlink {} in the Renovate cache dir", planted, it) }

            cacheDir.isDirectory(LinkOption.NOFOLLOW_LINKS) -> {
                cacheDir.listDirectoryEntries().forEach { entry ->
                    runCatching { entry.deleteRecursively() }
                        .onFailure { logger.warn("Could not delete {} from the Renovate cache dir", entry, it) }
                }
                logger.debug("Emptied Renovate cache dir {}", cacheDir)
            }
        }
    }

    /** The first symlink from below the base dir (Renovate's runs can write there) down to the cache dir. */
    private fun plantedSymlink(): Path? = if (cacheDir.startsWith(baseDir)) {
        baseDir.relativize(cacheDir)
            .runningFold(baseDir) { dir, segment -> dir.resolve(segment) }
            .drop(1)
            .firstOrNull { it.isSymbolicLink() }
    } else {
        cacheDir.takeIf { it.isSymbolicLink() }
    }

    private fun pruneEmptyParents(start: Path) {
        var current: Path? = start
        while (current != null && current.startsWith(reposDir) && current != reposDir) {
            if (!current.isDirectory() || current.listDirectoryEntries().isNotEmpty()) return
            current.deleteIfExists()
            current = current.parent
        }
    }

    companion object {
        const val REPOS_DIR = "repos"
    }
}
