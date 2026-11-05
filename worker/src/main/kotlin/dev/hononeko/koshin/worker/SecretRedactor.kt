package dev.hononeko.koshin.worker

import net.logstash.logback.mask.ValueMasker
import tools.jackson.core.TokenStreamContext
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Scrubs credentials from what the worker logs. Renovate redacts its own log fields, but not what child tools
 * (`git`, `npm`, `gpg`) print, and koshin's own error messages could carry a token too. Replaces the tokens of the runs
 * in progress ([track]) and anything shaped like a GitHub or GitLab token.
 */
class SecretRedactor {
    /** Each secret with the number of runs using it: concurrent runs of one installation share its token. */
    private val active = ConcurrentHashMap<String, Int>()

    /** Redacts [secret] until the returned handle is closed. */
    fun track(secret: String): AutoCloseable {
        if (secret.length < MIN_SECRET_LENGTH) return AutoCloseable {}
        active.merge(secret, 1, Int::plus)
        val closed = AtomicBoolean()
        return AutoCloseable {
            if (closed.compareAndSet(false, true)) {
                active.computeIfPresent(secret) { _, runs -> (runs - 1).takeIf { it > 0 } }
            }
        }
    }

    fun containsSecret(text: String): Boolean = active.keys.any { it in text } || PATTERN.containsMatchIn(text)

    fun redact(text: String): String {
        // Longest first, so a secret that contains another is replaced whole
        val exact = active.keys.filter { it in text }.sortedByDescending { it.length }
            .fold(text) { redacted, secret -> redacted.replace(secret, REDACTED) }
        return PATTERN.replace(exact) { match -> match.groups[1]?.value.orEmpty() + REDACTED }
    }

    companion object {
        /** Renovate's own placeholder. */
        const val REDACTED = "**redacted**"

        /** Shorter isn't a real token, and replacing it everywhere would mangle the logs. */
        private const val MIN_SECRET_LENGTH = 8

        /** GitHub installation, classic and fine-grained tokens, GitLab PATs, and a token in a Git URL. */
        private val PATTERN = Regex(
            listOf(
                """(x-access-token:)[^@\s"']+""",
                "ghs_[A-Za-z0-9_]+",
                "ghp_[A-Za-z0-9_]+",
                "github_pat_[A-Za-z0-9_]+",
                "glpat-[A-Za-z0-9_-]+",
            ).joinToString("|"),
        )

        /** The worker's redactor, which [RenovateProcessExecutor] tracks run tokens with and the logs go through. */
        val shared = SecretRedactor()
    }
}

/** Runs every string value the worker logs through [SecretRedactor.shared] (`logback.xml`). */
class SecretValueMasker : ValueMasker {
    override fun mask(context: TokenStreamContext, value: Any?): Any? =
        (value as? String)?.let { text -> SecretRedactor.shared.redact(text).takeIf { it != text } }
}
