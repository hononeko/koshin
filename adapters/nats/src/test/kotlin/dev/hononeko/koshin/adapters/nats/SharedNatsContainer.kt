package dev.hononeko.koshin.adapters.nats

import org.testcontainers.containers.GenericContainer

object SharedNatsContainer {
    val container: GenericContainer<*> by lazy {
        GenericContainer("nats:2.10-alpine")
            .withCommand("-js")
            .withExposedPorts(4222)
            .apply { start() }
    }

    val natsUrl: String
        get() = "nats://${container.host}:${container.getMappedPort(4222)}"
}
