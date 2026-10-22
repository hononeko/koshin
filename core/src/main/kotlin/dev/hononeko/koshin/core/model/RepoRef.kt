package dev.hononeko.koshin.core.model

import kotlinx.serialization.Serializable

@Serializable
data class RepoRef(val platform: Platform, val repository: String) {
    init {
        require(repository.isNotBlank()) { "Repository identifier cannot be blank" }
    }

    override fun toString(): String = "${platform.name.lowercase()}:$repository"
}
