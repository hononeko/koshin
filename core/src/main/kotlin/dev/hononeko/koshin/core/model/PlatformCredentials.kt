package dev.hononeko.koshin.core.model

/**
 * What a Renovate run authenticates with. [username] and [gitAuthor] are the bot identity of a GitHub App run
 * (`RENOVATE_USERNAME`, `RENOVATE_GIT_AUTHOR`); Renovate works them out itself for a PAT.
 */
data class PlatformCredentials(
    val token: String,
    val endpoint: String? = null,
    val username: String? = null,
    val gitAuthor: String? = null,
) {
    init {
        require(token.isNotBlank()) { "Platform token cannot be blank" }
        require(endpoint == null || endpoint.isNotBlank()) { "Platform endpoint cannot be blank" }
    }

    override fun toString(): String =
        "PlatformCredentials(token=***, endpoint=$endpoint, username=$username, gitAuthor=$gitAuthor)"
}
