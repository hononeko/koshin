package dev.hononeko.koshin.core.ports

/** Which accounts may use the GitHub App (KSH-17): a public App can be installed by anyone. */
interface InstallationPolicyPort {
    suspend fun isAllowed(accountId: Long, login: String): Boolean
}
