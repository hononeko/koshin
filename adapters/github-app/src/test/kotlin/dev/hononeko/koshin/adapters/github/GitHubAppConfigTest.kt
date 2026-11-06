package dev.hononeko.koshin.adapters.github

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.io.path.Path

class GitHubAppConfigTest {

    @Test
    fun `DisallowedAction fromString parses valid actions and rejects invalid`() {
        assertEquals(DisallowedAction.IGNORE, DisallowedAction.fromString("ignore"))
        assertEquals(DisallowedAction.IGNORE, DisallowedAction.fromString(" IGNORE "))
        assertEquals(DisallowedAction.SUSPEND, DisallowedAction.fromString("suspend"))
        assertEquals(DisallowedAction.UNINSTALL, DisallowedAction.fromString("uninstall"))

        assertThrows<IllegalArgumentException> {
            DisallowedAction.fromString("invalid_action")
        }
    }

    @Test
    fun `constructor validates appId and apiUrl`() {
        assertThrows<IllegalArgumentException> {
            GitHubAppConfig(appId = " ", privateKeyPath = Path("/tmp/key.pem"))
        }
        assertThrows<IllegalArgumentException> {
            GitHubAppConfig(appId = "123", privateKeyPath = Path("/tmp/key.pem"), apiUrl = "ftp://invalid")
        }
        assertThrows<IllegalArgumentException> {
            GitHubAppConfig(appId = "123", privateKeyPath = Path("/tmp/key.pem"), apiUrl = "https://")
        }
    }

    @Test
    fun `computed properties derive GHES endpoint and noreply domain`() {
        val dotCom = GitHubAppConfig(appId = "123", privateKeyPath = Path("/tmp/key.pem"))
        assertEquals("https://api.github.com", dotCom.apiBase)
        assertNull(dotCom.renovateEndpoint)
        assertEquals("users.noreply.github.com", dotCom.noreplyDomain)

        val ghes = GitHubAppConfig(
            appId = "123",
            privateKeyPath = Path("/tmp/key.pem"),
            apiUrl = "https://ghe.mycorp.internal/api/v3/",
        )
        assertEquals("https://ghe.mycorp.internal/api/v3", ghes.apiBase)
        assertEquals("https://ghe.mycorp.internal/api/v3", ghes.renovateEndpoint)
        assertEquals("users.noreply.ghe.mycorp.internal", ghes.noreplyDomain)
    }

    @Test
    fun `fromEnv parses complete configuration and handles partial configuration`() {
        assertNull(GitHubAppConfig.fromEnv(emptyMap()))

        assertThrows<IllegalArgumentException> {
            GitHubAppConfig.fromEnv(mapOf("GITHUB_APP_ID" to "123"))
        }
        assertThrows<IllegalArgumentException> {
            GitHubAppConfig.fromEnv(mapOf("GITHUB_APP_PRIVATE_KEY_PATH" to "/tmp/key.pem"))
        }

        val config = GitHubAppConfig.fromEnv(
            mapOf(
                "GITHUB_APP_ID" to "123",
                "GITHUB_APP_PRIVATE_KEY_PATH" to "/tmp/key.pem",
                "GITHUB_API_URL" to "https://api.github.com",
                "GITHUB_APP_ALLOWED_ACCOUNTS" to " org-a , 42 , ",
                "GITHUB_APP_DISALLOWED_ACTION" to "suspend",
            ),
        )
        checkNotNull(config)
        assertEquals("123", config.appId)
        assertEquals(Path("/tmp/key.pem"), config.privateKeyPath)
        assertEquals(listOf("org-a", "42"), config.allowedAccounts)
        assertEquals(DisallowedAction.SUSPEND, config.disallowedAction)
    }

    @Test
    fun `fromEnv handles blank values and fallbacks`() {
        assertNull(GitHubAppConfig.fromEnv(mapOf("GITHUB_APP_ID" to "   ", "GITHUB_APP_PRIVATE_KEY_PATH" to "   ")))
        assertThrows<IllegalArgumentException> {
            GitHubAppConfig.fromEnv(mapOf("GITHUB_APP_ID" to "123", "GITHUB_APP_PRIVATE_KEY_PATH" to "   "))
        }

        val config = GitHubAppConfig.fromEnv(
            mapOf(
                "GITHUB_APP_ID" to "123",
                "GITHUB_APP_PRIVATE_KEY_PATH" to "/tmp/key.pem",
                "GITHUB_API_URL" to "   ",
                "GITHUB_APP_DISALLOWED_ACTION" to "   ",
            ),
        )
        checkNotNull(config)
        assertEquals(GitHubAppConfig.DEFAULT_API_URL, config.apiUrl)
        assertEquals(DisallowedAction.IGNORE, config.disallowedAction)
        assertEquals(emptyList<String>(), config.allowedAccounts)
    }
}
