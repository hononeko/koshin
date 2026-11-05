package dev.hononeko.koshin.worker

import dev.hononeko.koshin.core.model.Platform
import dev.hononeko.koshin.core.model.PlatformCredentials
import dev.hononeko.koshin.core.model.RepoRef
import dev.hononeko.koshin.core.ports.PlatformCredentialsPort

class StaticCredentialsProvider(private val credentials: Map<Platform, PlatformCredentials>) :
    PlatformCredentialsPort {

    constructor(settings: WorkerSettings) : this(
        buildMap {
            settings.githubToken?.let { put(Platform.GITHUB, PlatformCredentials(it, settings.githubEndpoint)) }
            settings.gitlabToken?.let { put(Platform.GITLAB, PlatformCredentials(it, settings.gitlabEndpoint)) }
        },
    )

    fun isConfigured(platform: Platform): Boolean = platform in credentials

    override suspend fun credentialsFor(repo: RepoRef): PlatformCredentials =
        checkNotNull(credentials[repo.platform]) { "No token configured for platform ${repo.platform}" }
}

/** GitHub runs use the GitHub App when it's configured; everything else, and GitHub without it, the tokens. */
class WorkerCredentials(
    private val tokens: StaticCredentialsProvider,
    private val gitHubApp: PlatformCredentialsPort?,
) : PlatformCredentialsPort {

    fun isConfigured(platform: Platform): Boolean =
        (platform == Platform.GITHUB && gitHubApp != null) || tokens.isConfigured(platform)

    override suspend fun credentialsFor(repo: RepoRef): PlatformCredentials =
        if (repo.platform == Platform.GITHUB && gitHubApp != null) {
            gitHubApp.credentialsFor(repo)
        } else {
            tokens.credentialsFor(repo)
        }
}
