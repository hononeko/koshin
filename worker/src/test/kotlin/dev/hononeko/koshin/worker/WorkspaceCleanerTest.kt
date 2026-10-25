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
import kotlin.io.path.exists
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
    fun `purgeAll creates a missing base dir`() = runTest {
        val missing = baseDir.resolve("not/yet/created")
        WorkspaceCleaner(missing).purgeAll()
        assertTrue(missing.exists())
    }

    @Test
    fun `refuses the filesystem root`() {
        assertThrows<IllegalArgumentException> { WorkspaceCleaner(Path("/")) }
    }
}
