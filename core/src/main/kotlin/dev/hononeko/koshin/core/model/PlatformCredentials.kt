package dev.hononeko.koshin.core.model

data class PlatformCredentials(val token: String) {
    init {
        require(token.isNotBlank()) { "Platform token cannot be blank" }
    }
}
