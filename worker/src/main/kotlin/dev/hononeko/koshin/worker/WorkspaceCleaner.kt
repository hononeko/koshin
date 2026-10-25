package dev.hononeko.koshin.worker

import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.ports.WorkspaceCleanerPort
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import java.nio.file.Path
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.createDirectories
import kotlin.io.path.deleteIfExists
import kotlin.io.path.deleteRecursively
import kotlin.io.path.exists
import kotlin.io.path.isDirectory
import kotlin.io.path.listDirectoryEntries

/**
 * Deletes Renovate clones under `<baseDir>/repos/<platform>/<repository>` (Renovate's default `localDir` layout).
 * Recursive deletes never follow symlinks.
 */
@OptIn(ExperimentalPathApi::class)
class WorkspaceCleaner(baseDir: Path, private val ioDispatcher: CoroutineDispatcher = Dispatchers.IO) :
    WorkspaceCleanerPort {

    private val logger = LoggerFactory.getLogger(WorkspaceCleaner::class.java)
    private val baseDir: Path = baseDir.toAbsolutePath().normalize()
    private val reposDir: Path = this.baseDir.resolve(REPOS_DIR)

    init {
        require(this.baseDir.nameCount > 0) { "Refusing to use filesystem root as workspace base dir" }
    }

    fun workspaceFor(repo: RepoRef): Path {
        val dir = reposDir.resolve(repo.platform.name.lowercase()).resolve(repo.repository).normalize()
        require(dir.startsWith(reposDir) && dir != reposDir) { "Workspace for $repo escapes $reposDir" }
        return dir
    }

    override suspend fun cleanup(repo: RepoRef) {
        val dir = workspaceFor(repo)
        withContext(ioDispatcher) {
            if (dir.exists()) {
                dir.deleteRecursively()
                logger.debug("Deleted workspace {} for {}", dir, repo)
            }
            pruneEmptyParents(dir.parent)
        }
    }

    /** Wipes everything under the base dir (left over by a previous container in the same pod). */
    override suspend fun purgeAll() {
        withContext(ioDispatcher) {
            baseDir.createDirectories()
            val entries = baseDir.listDirectoryEntries()
            entries.forEach { it.deleteRecursively() }
            if (entries.isNotEmpty()) {
                logger.info("Purged {} leftover entries from {}", entries.size, baseDir)
            }
        }
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
