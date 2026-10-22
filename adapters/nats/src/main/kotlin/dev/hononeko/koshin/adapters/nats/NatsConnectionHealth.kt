package dev.hononeko.koshin.adapters.nats

import io.nats.client.Connection

fun Connection.isConnected(): Boolean = status == Connection.Status.CONNECTED

class NatsConnectionHealth(private val connection: Connection) {
    fun isConnected(): Boolean = connection.isConnected()
}
