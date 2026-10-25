package dev.hononeko.koshin.proxy

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.RepoRef
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

class StaticFileRepositoryDiscoveryTest {

    @TempDir
    lateinit var tempDir: Path

    @Test
    fun `returns empty list when filePath is null or blank or file is empty`() = runTest {
        assertTrue(StaticFileRepositoryDiscovery(null as String?).discover().isEmpty())
        assertTrue(StaticFileRepositoryDiscovery("   ").discover().isEmpty())

        val emptyFile = tempDir.resolve("empty.txt")
        Files.writeString(emptyFile, "   \n  ")
        assertTrue(StaticFileRepositoryDiscovery(emptyFile).discover().isEmpty())
    }

    @Test
    fun `parses JSON array of objects and string specs`() = runTest {
        val jsonFile = tempDir.resolve("repos.json")
        Files.writeString(
            jsonFile,
            """
            [
              {"platform": "github", "repository": "hononeko/koshin"},
              {"platform": "GITLAB", "repository": "gitlab-org/subgroup/project"},
              {"repository": "hononeko/actions"},
              "github:octocat/hello-world",
              "gitlab:group/repo",
              "owner/default-platform-repo"
            ]
            """.trimIndent(),
        )

        val discovered = StaticFileRepositoryDiscovery(jsonFile.toString()).discover()
        assertEquals(
            listOf(
                RepoRef(Platform.GITHUB, "hononeko/koshin"),
                RepoRef(Platform.GITLAB, "gitlab-org/subgroup/project"),
                RepoRef(Platform.GITHUB, "hononeko/actions"),
                RepoRef(Platform.GITHUB, "octocat/hello-world"),
                RepoRef(Platform.GITLAB, "group/repo"),
                RepoRef(Platform.GITHUB, "owner/default-platform-repo"),
            ),
            discovered,
        )
    }

    @Test
    fun `parses line-delimited repositories and strips comments`() = runTest {
        val txtFile = tempDir.resolve("repos.txt")
        Files.writeString(
            txtFile,
            """
            # Sweep list
            hononeko/koshin
            github:hononeko/actions # inline comment
            gitlab:gitlab-org/subgroup/repo

            """.trimIndent(),
        )

        val discovered = StaticFileRepositoryDiscovery(txtFile).discover()
        assertEquals(
            listOf(
                RepoRef(Platform.GITHUB, "hononeko/koshin"),
                RepoRef(Platform.GITHUB, "hononeko/actions"),
                RepoRef(Platform.GITLAB, "gitlab-org/subgroup/repo"),
            ),
            discovered,
        )
    }

    @Test
    fun `throws IllegalArgumentException for missing file or invalid entries`() = runTest {
        val missing = tempDir.resolve("missing.json")
        assertThrows<IllegalArgumentException> {
            StaticFileRepositoryDiscovery(missing).discover()
        }

        val badJson = tempDir.resolve("bad.json")
        Files.writeString(badJson, """[{"platform": "unknown", "repository": "a/b"}]""")
        assertThrows<IllegalArgumentException> {
            StaticFileRepositoryDiscovery(badJson).discover()
        }

        val missingRepoField = tempDir.resolve("missing-repo.json")
        Files.writeString(missingRepoField, """[{"platform": "github"}]""")
        assertThrows<IllegalArgumentException> {
            StaticFileRepositoryDiscovery(missingRepoField).discover()
        }

        val nestedArray = tempDir.resolve("nested.json")
        Files.writeString(nestedArray, """[[]]""")
        assertThrows<IllegalArgumentException> {
            StaticFileRepositoryDiscovery(nestedArray).discover()
        }

        assertThrows<IllegalArgumentException> {
            StaticFileRepositoryDiscovery.parseSpecString("   ")
        }
    }
}
