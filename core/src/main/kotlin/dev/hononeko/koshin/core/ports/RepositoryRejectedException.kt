package dev.hononeko.koshin.core.ports

/**
 * Thrown by a [PlatformCredentialsPort] when koshin must not run Renovate for a repository at all, e.g. one whose
 * GitHub App installation belongs to an account that isn't allowed. The job is ACKed: no retry and no follow-up run.
 */
class RepositoryRejectedException(message: String) : Exception(message)
