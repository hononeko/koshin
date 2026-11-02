package dev.hononeko.koshin.adapters.github

import java.net.URI
import java.nio.file.Path
import kotlin.io.path.Path

/** GitHub App settings: `GITHUB_APP_ID`, `GITHUB_APP_PRIVATE_KEY_PATH` and, for GHES, `GITHUB_API_URL`. */
data class GitHubAppConfig(val appId: String, val privateKeyPath: Path, val apiUrl: String = DEFAULT_API_URL) {
    init {
        require(appId.isNotBlank()) { "GITHUB_APP_ID cannot be blank" }
        require(apiUrl.startsWith("https://") || apiUrl.startsWith("http://")) {
            "GITHUB_API_URL must be an http(s) URL: $apiUrl"
        }
    }

    /** The API base without a trailing slash, e.g. `https://ghe.example.com/api/v3`. */
    val apiBase: String = apiUrl.trimEnd('/')

    /** Renovate's `endpoint`: `null` on github.com, where Renovate's default applies. */
    val renovateEndpoint: String? = apiBase.takeIf { it != DEFAULT_API_URL }

    /** The domain of bot noreply emails: `users.noreply.github.com`, or `users.noreply.<host>` on GHES. */
    val noreplyDomain: String = if (renovateEndpoint == null) {
        "users.noreply.github.com"
    } else {
        "users.noreply.${URI(apiBase).host}"
    }

    companion object {
        const val DEFAULT_API_URL = "https://api.github.com"

        /** `null` when the App isn't configured, so the PAT (`GITHUB_TOKEN`) applies. */
        fun fromEnv(env: Map<String, String> = System.getenv()): GitHubAppConfig? {
            val appId = env["GITHUB_APP_ID"]?.trim()?.takeIf { it.isNotEmpty() }
            val keyPath = env["GITHUB_APP_PRIVATE_KEY_PATH"]?.trim()?.takeIf { it.isNotEmpty() }
            if (appId == null && keyPath == null) return null
            require(appId != null && keyPath != null) {
                "GITHUB_APP_ID and GITHUB_APP_PRIVATE_KEY_PATH must be set together"
            }
            val apiUrl = env["GITHUB_API_URL"]?.trim()?.takeIf { it.isNotEmpty() } ?: DEFAULT_API_URL
            return GitHubAppConfig(appId, Path(keyPath), apiUrl)
        }
    }
}
