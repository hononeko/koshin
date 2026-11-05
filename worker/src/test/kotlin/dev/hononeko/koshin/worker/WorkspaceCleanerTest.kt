package dev.hononeko.koshin.worker

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RepoRef
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import kotlin.io.path.Path
import kotlin.io.path.createDirectories
import kotlin.io.path.createSymbolicLinkPointingTo
import kotlin.io.path.createTempDirectory
import kotlin.io.path.exists
import kotlin.io.path.isDirectory
import kotlin.io.path.isSymbolicLink
import kotlin.io.path.listDirectoryEntries
import kotlin.io.path.writeText

class WorkspaceCleanerTest {

    @TempDir
    lateinit var baseDir: Path

    private val githubRepo = RepoRef(Platform.GITHUB, "hononeko/koshin")
    private val siblingRepo = RepoRef(Platform.GITHUB, "hononeko/other")
    private val gitlabRepo = RepoRef(Platform.GITLAB, "group/sub/project")

    private fun seed(repo: RepoRef, cleaner: WorkspaceCleaner): Path {
        val dir = cleaner.workspaceFor(repo)
        dir.resolve(".git").createDirectories()
        dir.resolve("package.json").writeText("{}")
        return dir
    }

    @Test
    fun `workspace follows Renovate localDir layout`() {
        val cleaner = WorkspaceCleaner(baseDir)
        assertEquals(baseDir.resolve("repos/github/hononeko/koshin"), cleaner.workspaceFor(githubRepo))
        assertEquals(baseDir.resolve("repos/gitlab/group/sub/project"), cleaner.workspaceFor(gitlabRepo))
    }

    @Test
    fun `cleanup deletes only the repo's workspace and prunes empty parents`() = runTest {
        val cleaner = WorkspaceCleaner(baseDir)
        val target = seed(githubRepo, cleaner)
        val sibling = seed(siblingRepo, cleaner)
        val gitlab = seed(gitlabRepo, cleaner)

        cleaner.cleanup(githubRepo)
        assertFalse(target.exists())
        assertTrue(sibling.exists())

        cleaner.cleanup(gitlabRepo)
        assertFalse(gitlab.exists())
        assertFalse(baseDir.resolve("repos/gitlab").exists())
        assertTrue(baseDir.resolve("repos").exists())
    }

    @Test
    fun `cleanup of a missing workspace is a no-op`() = runTest {
        WorkspaceCleaner(baseDir).cleanup(githubRepo)
        assertFalse(baseDir.resolve("repos").exists())
    }

    @Test
    fun `cleanup does not follow symlinks out of the workspace`() = runTest {
        val cleaner = WorkspaceCleaner(baseDir.resolve("renovate"))
        val outside = baseDir.resolve("outside").createDirectories()
        outside.resolve("keep.txt").writeText("keep")
        val workspace = seed(githubRepo, cleaner)
        workspace.resolve("link").createSymbolicLinkPointingTo(outside)

        cleaner.cleanup(githubRepo)

        assertFalse(workspace.exists())
        assertTrue(outside.resolve("keep.txt").exists())
    }

    @Test
    fun `purgeAll wipes leftovers but keeps the base dir`() = runTest {
        val cleaner = WorkspaceCleaner(baseDir)
        seed(githubRepo, cleaner)
        seed(gitlabRepo, cleaner)
        baseDir.resolve("cache/renovate").createDirectories()
        baseDir.resolve("stray.log").writeText("x")

        cleaner.purgeAll()

        assertTrue(baseDir.exists())
        assertTrue(baseDir.listDirectoryEntries().isEmpty())
    }

    @Test
    fun `purgeAll skips entries it cannot delete`() = runTest {
        val locked = baseDir.resolve("lost+found").createDirectories()
        locked.resolve("file").writeText("x")
        locked.toFile().setWritable(false)
        baseDir.resolve("stray.log").writeText("x")
        try {
            WorkspaceCleaner(baseDir).purgeAll()
            assertFalse(baseDir.resolve("stray.log").exists())
        } finally {
            locked.toFile().setWritable(true)
        }
    }

    @Test
    fun `purgeAll creates a missing base dir`() = runTest {
        val missing = baseDir.resolve("not/yet/created")
        WorkspaceCleaner(missing).purgeAll()
        assertTrue(missing.exists())
    }

    @Test
    fun `refuses the filesystem root`() {
        assertThrows<IllegalArgumentException> { WorkspaceCleaner(Path("/")) }
    }

    /** Fills [cacheDir] as a run would: the repo cache file, cached HTTP bodies, the file package cache. */
    private fun fillCache(cacheDir: Path) {
        cacheDir.resolve("renovate/repository/github/hononeko").createDirectories()
            .resolve("koshin.json").writeText("{}")
        cacheDir.resolve("renovate/http-cache").createDirectories().resolve("preset").writeText("private")
        cacheDir.resolve("others").createDirectories().resolve("pkg").writeText("x")
    }

    @Test
    fun `cleanup empties the cache dir and keeps it`() = runTest {
        val cleaner = WorkspaceCleaner(baseDir)
        val cacheDir = baseDir.resolve("cache")
        fillCache(cacheDir)
        val sibling = seed(siblingRepo, cleaner)

        cleaner.cleanup(githubRepo)

        assertTrue(cacheDir.isDirectory())
        assertEquals(emptyList<Path>(), cacheDir.listDirectoryEntries())
        assertTrue(sibling.exists(), "only the run's workspace goes")
    }

    @Test
    fun `cleanup empties a cache dir outside the base dir too`() = runTest {
        val cacheDir = createTempDirectory("koshin-cache-")
        try {
            fillCache(cacheDir)

            WorkspaceCleaner(baseDir, cacheDir).cleanup(githubRepo)

            assertEquals(emptyList<Path>(), cacheDir.listDirectoryEntries())
        } finally {
            cacheDir.toFile().deleteRecursively()
        }
    }

    @Test
    fun `a symlink a run left in place of the cache dir is deleted, not followed`() = runTest {
        val outside = baseDir.resolve("outside").createDirectories()
        outside.resolve("keep.txt").writeText("keep")
        val workspace = baseDir.resolve("renovate")
        val nested = WorkspaceCleaner(workspace, workspace.resolve("cache/nested"))
        workspace.resolve("cache").createDirectories()
        workspace.resolve("cache/nested").createSymbolicLinkPointingTo(outside)
        val direct = WorkspaceCleaner(workspace)

        nested.cleanup(githubRepo)
        assertFalse(workspace.resolve("cache/nested").exists())

        workspace.resolve("cache").toFile().deleteRecursively()
        workspace.resolve("cache").createSymbolicLinkPointingTo(outside)
        direct.cleanup(githubRepo)

        assertFalse(workspace.resolve("cache").isSymbolicLink() || workspace.resolve("cache").exists())
        assertTrue(outside.resolve("keep.txt").exists())
    }

    @Test
    fun `a symlink in place of a cache dir outside the base dir is deleted, not followed`() = runTest {
        val target = baseDir.resolve("target").createDirectories()
        target.resolve("keep.txt").writeText("keep")
        val link = createTempDirectory("koshin-link-").resolve("cache")
        try {
            link.createSymbolicLinkPointingTo(target)

            WorkspaceCleaner(baseDir.resolve("renovate"), link).cleanup(githubRepo)

            assertFalse(link.isSymbolicLink() || link.exists())
            assertTrue(target.resolve("keep.txt").exists())
        } finally {
            link.parent.toFile().deleteRecursively()
        }
    }

    @Test
    fun `the cache dir is emptied even when the workspace can't be deleted`() = runTest {
        val cleaner = WorkspaceCleaner(baseDir)
        val workspace = seed(githubRepo, cleaner)
        fillCache(baseDir.resolve("cache"))
        workspace.toFile().setWritable(false)
        try {
            // Fails unless the tests run as root
            runCatching { cleaner.cleanup(githubRepo) }
        } finally {
            workspace.toFile().setWritable(true)
        }

        assertEquals(emptyList<Path>(), baseDir.resolve("cache").listDirectoryEntries())
    }

    @Test
    fun `purgeAll empties a cache dir outside the base dir`() = runTest {
        val cacheDir = createTempDirectory("koshin-cache-")
        try {
            fillCache(cacheDir)

            WorkspaceCleaner(baseDir, cacheDir).purgeAll()

            assertEquals(emptyList<Path>(), cacheDir.listDirectoryEntries())
        } finally {
            cacheDir.toFile().deleteRecursively()
        }
    }

    @Test
    fun `refuses a cache dir that contains the base dir`() {
        assertThrows<IllegalArgumentException> { WorkspaceCleaner(baseDir.resolve("renovate"), baseDir) }
        WorkspaceCleaner(baseDir, baseDir)
    }
}
