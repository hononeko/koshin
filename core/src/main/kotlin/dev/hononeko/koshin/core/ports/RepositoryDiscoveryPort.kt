package dev.hononeko.koshin.core.ports

import dev.hononeko.koshin.core.model.RepoRef

interface RepositoryDiscoveryPort {
    suspend fun discover(): List<RepoRef>
}
