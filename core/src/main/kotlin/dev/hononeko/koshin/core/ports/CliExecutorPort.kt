package dev.hononeko.koshin.core.ports

import dev.hononeko.koshin.core.model.JobExecutionResult
import dev.hononeko.koshin.core.model.PlatformCredentials
import dev.hononeko.koshin.core.model.RepoRef
import kotlin.time.Duration

interface CliExecutorPort {
    suspend fun execute(repo: RepoRef, creds: PlatformCredentials, timeout: Duration): JobExecutionResult
}
