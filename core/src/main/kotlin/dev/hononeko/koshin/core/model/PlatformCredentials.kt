package dev.hononeko.koshin.core.model

/**
 * What a Renovate run authenticates with. [username] and [gitAuthor] are the bot identity of a GitHub App run
 * (`RENOVATE_USERNAME`, `RENOVATE_GIT_AUTHOR`); Renovate works them out itself for a PAT. [installationId] is the
 * GitHub App installation [token] belongs to, `null` for a platform token.
 */
data class PlatformCredentials(
    val token: String,
    val endpoint: String? = null,
    val username: String? = null,
    val gitAuthor: String? = null,
    val installationId: Long? = null,
) {
    init {
        require(token.isNotBlank()) { "Platform token cannot be blank" }
        require(endpoint == null || endpoint.isNotBlank()) { "Platform endpoint cannot be blank" }
        require(installationId == null || installationId > 0) { "installationId must be positive: $installationId" }
    }

    /** The scope of [token] on [platform]: whose caches a run with it may read and write. */
    fun scope(platform: Platform): CredentialScope = CredentialScope.of(platform, installationId)

    override fun toString(): String = "PlatformCredentials(token=***, endpoint=$endpoint, username=$username, " +
        "gitAuthor=$gitAuthor, installationId=$installationId)"
}
