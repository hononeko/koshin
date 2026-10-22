package dev.hononeko.koshin.core.ports

import dev.hononeko.koshin.core.model.PlatformCredentials
import dev.hononeko.koshin.core.model.RepoRef

interface PlatformCredentialsPort {
    suspend fun credentialsFor(repo: RepoRef): PlatformCredentials
}
