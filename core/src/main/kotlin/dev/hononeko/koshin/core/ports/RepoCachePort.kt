package dev.hononeko.koshin.core.ports

import dev.hononeko.koshin.core.model.RepoRef

interface RepoCachePort {
    suspend fun restore(repo: RepoRef): Boolean
    suspend fun persist(repo: RepoRef)
}
