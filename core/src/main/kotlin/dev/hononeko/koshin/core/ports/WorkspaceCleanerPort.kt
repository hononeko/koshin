package dev.hononeko.koshin.core.ports

import dev.hononeko.koshin.core.model.RepoRef

interface WorkspaceCleanerPort {
    /** Deletes [repo]'s workspace and the run's local caches, which the next run may be another tenant's. */
    suspend fun cleanup(repo: RepoRef)
    suspend fun purgeAll()
}
