package dev.hononeko.koshin.core.ports

import dev.hononeko.koshin.core.model.RepoRef

interface WorkspaceCleanerPort {
    suspend fun cleanup(repo: RepoRef)
    suspend fun purgeAll()
}
