package dev.hononeko.koshin.core.model

import kotlinx.serialization.Serializable

@Serializable
enum class Platform {
    GITHUB,
    GITLAB,
    ;

    companion object {
        fun fromString(raw: String): Platform {
            val cleaned = raw.trim()
            return requireNotNull(
                entries.firstOrNull { it.name.equals(cleaned, ignoreCase = true) },
            ) { "Unknown platform: '$raw'" }
        }
    }
}
