package dev.hononeko.koshin.core.model

/**
 * The credential a job runs with, and so the API rate-limit bucket it spends: a GitHub App installation
 * (`github:inst-<id>`) or the platform's token (`github:pat`, `gitlab:pat`). Run slots, rate-limit cooldowns and the
 * job queue are partitioned by it, so one installation's rate limit never holds up another's jobs.
 */
sealed interface CredentialScope {
    val platform: Platform

    /** The scope within its platform, safe in a NATS subject token and a consumer name: `inst-<id>` or `pat`. */
    val token: String

    /** A GitHub App installation, with its own rate-limit bucket. */
    data class Installation(val installationId: Long) : CredentialScope {
        init {
            require(installationId > 0) { "installationId must be positive: $installationId" }
        }

        override val platform: Platform
            get() = Platform.GITHUB

        override val token: String
            get() = "$INSTALLATION_PREFIX$installationId"

        override fun toString(): String = "${platform.name.lowercase()}:$token"
    }

    /** The platform's own token (`GITHUB_TOKEN`, `GITLAB_TOKEN`), shared by all its jobs. */
    data class Token(override val platform: Platform) : CredentialScope {
        override val token: String
            get() = TOKEN

        override fun toString(): String = "${platform.name.lowercase()}:$token"
    }

    companion object {
        const val TOKEN = "pat"
        private const val INSTALLATION_PREFIX = "inst-"

        /** The scope of a job for [platform], run as GitHub App installation [installationId] if there is one. */
        fun of(platform: Platform, installationId: Long?): CredentialScope = when {
            installationId == null -> Token(platform)
            platform == Platform.GITHUB -> Installation(installationId)
            else -> throw IllegalArgumentException("Only GitHub jobs run as an installation: $platform")
        }

        /** The scope [token] (`inst-<id>` or `pat`) names on [platform], or `null` if it names none. */
        fun parse(platform: Platform, token: String): CredentialScope? = when {
            token == TOKEN -> Token(platform)

            platform == Platform.GITHUB && token.startsWith(INSTALLATION_PREFIX) ->
                token.removePrefix(INSTALLATION_PREFIX).toLongOrNull()?.takeIf { it > 0 }?.let(::Installation)
                    ?.takeIf { it.token == token }

            else -> null
        }
    }
}
