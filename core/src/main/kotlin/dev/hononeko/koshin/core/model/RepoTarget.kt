package dev.hononeko.koshin.core.model

/** A repository to run, and the GitHub App installation covering it when that's known (KSH-24). */
data class RepoTarget(val repo: RepoRef, val installationId: Long? = null) {
    init {
        require(installationId == null || (installationId > 0 && repo.platform == Platform.GITHUB)) {
            "installationId must be positive and on a GitHub repository: $installationId ($repo)"
        }
    }

    override fun toString(): String = if (installationId == null) "$repo" else "$repo (installation $installationId)"
}
