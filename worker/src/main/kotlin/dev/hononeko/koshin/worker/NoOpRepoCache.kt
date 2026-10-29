package dev.hononeko.koshin.worker

import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.ports.RepoCachePort

/** No shared repository cache. Replaced in KSH-10. */
object NoOpRepoCache : RepoCachePort {
    override suspend fun restore(repo: RepoRef): Boolean = false
    override suspend fun persist(repo: RepoRef) = Unit
}
