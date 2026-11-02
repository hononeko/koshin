package dev.hononeko.koshin.adapters.github

import java.net.URI
import java.nio.file.Path
import kotlin.io.path.Path

/** What koshin does with an installation by an account that isn't allowed (`GITHUB_APP_DISALLOWED_ACTION`). */
enum class DisallowedAction {
    /** Leave it installed, and never run it. */
    IGNORE,

    /** Suspend it: the owner still sees it, and the App's owner can unsuspend it. */
    SUSPEND,

    /** Uninstall it. */
    UNINSTALL,
    ;

    companion object {
        fun fromString(raw: String): DisallowedAction = entries.firstOrNull { it.name.equals(raw.trim(), true) }
            ?: throw IllegalArgumentException(
                "Invalid GITHUB_APP_DISALLOWED_ACTION: '$raw' (ignore, suspend, uninstall)",
            )
    }
}

/**
 * GitHub App settings: `GITHUB_APP_ID`, `GITHUB_APP_PRIVATE_KEY_PATH`, `GITHUB_API_URL` (GHES), and which accounts may
 * use the App, `GITHUB_APP_ALLOWED_ACCOUNTS` (logins or numeric IDs; empty allows all), with
 * `GITHUB_APP_DISALLOWED_ACTION` for the others.
 */
data class GitHubAppConfig(
    val appId: String,
    val privateKeyPath: Path,
    val apiUrl: String = DEFAULT_API_URL,
    val allowedAccounts: List<String> = emptyList(),
    val disallowedAction: DisallowedAction = DisallowedAction.IGNORE,
) {
    init {
        require(appId.isNotBlank()) { "GITHUB_APP_ID cannot be blank" }
        require(apiUrl.startsWith("https://") || apiUrl.startsWith("http://")) {
            "GITHUB_API_URL must be an http(s) URL: $apiUrl"
        }
        require(!runCatching { URI(apiUrl).host }.getOrNull().isNullOrBlank()) {
            "GITHUB_API_URL has no host: $apiUrl"
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
            val allowed = env["GITHUB_APP_ALLOWED_ACCOUNTS"].orEmpty()
                .split(',')
                .map { it.trim() }
                .filter { it.isNotEmpty() }
            val action = env["GITHUB_APP_DISALLOWED_ACTION"]?.takeIf { it.isNotBlank() }
                ?.let { DisallowedAction.fromString(it) }
                ?: DisallowedAction.IGNORE
            return GitHubAppConfig(appId, Path(keyPath), apiUrl, allowed, action)
        }
    }
}
