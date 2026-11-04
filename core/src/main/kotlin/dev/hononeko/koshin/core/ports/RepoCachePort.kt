package dev.hononeko.koshin.core.ports

import dev.hononeko.koshin.core.model.CredentialScope
import dev.hononeko.koshin.core.model.RepoRef

/** Renovate's repository cache between runs, kept per credential [CredentialScope] (KSH-26). */
interface RepoCachePort {
    suspend fun restore(repo: RepoRef, scope: CredentialScope): Boolean
    suspend fun persist(repo: RepoRef, scope: CredentialScope)
}
