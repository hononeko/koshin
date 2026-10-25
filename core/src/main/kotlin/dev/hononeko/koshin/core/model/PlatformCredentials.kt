package dev.hononeko.koshin.core.model

data class PlatformCredentials(val token: String, val endpoint: String? = null) {
    init {
        require(token.isNotBlank()) { "Platform token cannot be blank" }
        require(endpoint == null || endpoint.isNotBlank()) { "Platform endpoint cannot be blank" }
    }

    override fun toString(): String = "PlatformCredentials(token=***, endpoint=$endpoint)"
}
