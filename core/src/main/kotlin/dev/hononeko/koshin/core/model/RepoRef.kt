package dev.hononeko.koshin.core.model

import kotlinx.serialization.Serializable

@Serializable
data class RepoRef(val platform: Platform, val repository: String) {
    init {
        require(repository.isNotBlank()) { "Repository identifier cannot be blank" }
        require(!repository.any { it.isWhitespace() }) {
            "Repository identifier cannot contain whitespace: '$repository'"
        }
        require(!repository.startsWith("/") && !repository.endsWith("/")) {
            "Repository identifier cannot start or end with a slash: '$repository'"
        }
        require(!repository.contains("//")) {
            "Repository identifier cannot contain consecutive slashes: '$repository'"
        }
        val segments = repository.split("/")
        require(segments.none { it == "." || it == ".." }) {
            "Repository segments cannot be '.' or '..': '$repository'"
        }
        when (platform) {
            Platform.GITHUB -> require(segments.size == 2 && segments[0].isNotEmpty() && segments[1].isNotEmpty()) {
                "GitHub repository must be in owner/repo format: '$repository'"
            }

            Platform.GITLAB -> require(segments.size >= 2 && segments.all { it.isNotEmpty() }) {
                "GitLab repository must have at least group and project: '$repository'"
            }
        }
    }

    override fun toString(): String = "${platform.name.lowercase()}:$repository"
}
