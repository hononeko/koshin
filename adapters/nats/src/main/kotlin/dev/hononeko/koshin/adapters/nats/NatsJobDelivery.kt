package dev.hononeko.koshin.adapters.nats

import dev.hononeko.koshin.core.model.RenovateJobEvent
import dev.hononeko.koshin.core.ports.JobDelivery
import io.nats.client.Message
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlin.time.Duration
import kotlin.time.toJavaDuration

class NatsJobDelivery(
    private val msg: Message,
    override val event: RenovateJobEvent,
    private val ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
) : JobDelivery {
    override val deliveryAttempt: Int
        get() = msg.metaData().deliveredCount().toInt()

    override suspend fun ack() {
        withContext(ioDispatcher) {
            msg.ack()
        }
    }

    override suspend fun nack(delay: Duration) {
        withContext(ioDispatcher) {
            if (delay <= Duration.ZERO) {
                msg.nak()
            } else {
                msg.nakWithDelay(delay.toJavaDuration())
            }
        }
    }

    override suspend fun heartbeat() {
        withContext(ioDispatcher) {
            msg.inProgress()
        }
    }
}
