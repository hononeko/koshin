package dev.hononeko.koshin.adapters.nats

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
internal data class MaxDeliveriesAdvisory(
    val type: String? = null,
    val id: String? = null,
    val timestamp: String? = null,
    val stream: String? = null,
    val consumer: String? = null,
    @SerialName("stream_seq")
    val streamSeq: Long = 0L,
    val deliveries: Long = 0L,
)
