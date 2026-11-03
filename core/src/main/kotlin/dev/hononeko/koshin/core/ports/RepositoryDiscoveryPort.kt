package dev.hononeko.koshin.core.ports

import dev.hononeko.koshin.core.model.RepoTarget

interface RepositoryDiscoveryPort {
    /** The repositories to sweep, with the GitHub App installation covering each when it's known (KSH-24). */
    suspend fun discover(): List<RepoTarget>
}
