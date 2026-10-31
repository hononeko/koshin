package dev.hononeko.koshin.worker

import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.ports.RepoCachePort

/** No koshin-side sync: the cache stays local, or Renovate keeps it in S3 itself. */
object NoOpRepoCache : RepoCachePort {
    override suspend fun restore(repo: RepoRef): Boolean = false
    override suspend fun persist(repo: RepoRef) = Unit
}
