package dev.hononeko.koshin.adapters.valkey

import org.testcontainers.containers.GenericContainer

object SharedValkeyContainer {
    val container: GenericContainer<*> by lazy {
        GenericContainer("valkey/valkey:8-alpine")
            .withExposedPorts(6379)
            .apply { start() }
    }

    val valkeyUrl: String
        get() = "redis://${container.host}:${container.getMappedPort(6379)}"
}
