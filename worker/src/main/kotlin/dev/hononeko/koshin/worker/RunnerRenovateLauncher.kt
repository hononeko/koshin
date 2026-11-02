package dev.hononeko.koshin.worker

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.slf4j.MDCContext
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import java.io.IOException
import java.net.StandardProtocolFamily
import java.net.UnixDomainSocketAddress
import java.nio.channels.Channels
import java.nio.channels.SocketChannel
import java.nio.file.Path
import kotlin.time.Duration.Companion.seconds

/** A run the worker hands to the runner, as the first line on a connection. Closing that side kills the run. */
@Serializable
data class RunRequest(val command: List<String>, val env: Map<String, String>, val directory: String)

/** The runner's replies, one per line: output lines, then one [Exited] or [Failed]. */
@Serializable
sealed interface RunnerMessage {
    @Serializable
    @SerialName("output")
    data class Output(val stream: OutputStreamName, val line: String) : RunnerMessage

    /** Sent once Renovate and everything it started are gone. */
    @Serializable
    @SerialName("exited")
    data class Exited(val code: Int) : RunnerMessage

    @Serializable
    @SerialName("failed")
    data class Failed(val message: String) : RunnerMessage
}

/** Hands each run to the Renovate runner ([RenovateRunner]) in the pod's `renovate` container, over [socket]. */
class RunnerRenovateLauncher(
    private val socket: Path,
    private val ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
) : RenovateLauncher {

    override suspend fun launch(
        command: List<String>,
        env: Map<String, String>,
        directory: Path,
        onOutput: (OutputStreamName, String) -> Unit,
    ): RenovateRun {
        val channel = withContext(ioDispatcher) {
            val channel = SocketChannel.open(StandardProtocolFamily.UNIX)
            try {
                channel.connect(UnixDomainSocketAddress.of(socket))
                val request = Json.encodeToString(RunRequest(command, env, directory.toString())) + "\n"
                // Not closed: closing the stream closes the channel.
                Channels.newOutputStream(channel).apply { write(request.toByteArray()) }.flush()
                channel
            } catch (e: IOException) {
                channel.close()
                throw IOException("Could not hand the run to the Renovate runner at $socket", e)
            }
        }
        return RemoteRun(channel, onOutput)
    }

    private inner class RemoteRun(private val channel: SocketChannel, onOutput: (OutputStreamName, String) -> Unit) :
        RenovateRun {
        private val exit = CompletableDeferred<Int>()

        // Keeps the job's MDC, like the output of a local run.
        private val reader = CoroutineScope(ioDispatcher + MDCContext()).launch {
            try {
                exit.complete(read(onOutput))
            } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
                exit.completeExceptionally(e as? IOException ?: IOException("Unreadable reply from the runner", e))
            }
        }

        private fun read(onOutput: (OutputStreamName, String) -> Unit): Int {
            // Not closed: destroy() closes the channel.
            val lines = Channels.newInputStream(channel).bufferedReader()
            while (true) {
                val line = lines.readLine() ?: throw IOException("The runner hung up before Renovate exited")
                when (val message = Json.decodeFromString<RunnerMessage>(line)) {
                    is RunnerMessage.Output -> onOutput(message.stream, message.line)
                    is RunnerMessage.Exited -> return message.code
                    is RunnerMessage.Failed -> throw IOException("Runner could not start the run: ${message.message}")
                }
            }
        }

        override suspend fun awaitExit(): Int = exit.await()

        override suspend fun destroy() {
            if (!exit.isCompleted) {
                // Our side closing asks the runner to kill the run; it replies once everything is gone.
                withContext(ioDispatcher) { runCatching { channel.shutdownOutput() } }
                withTimeoutOrNull(KILL_CONFIRM_TIMEOUT) { exit.join() }
                    ?: logger.warn("The runner did not confirm the run was killed within {}", KILL_CONFIRM_TIMEOUT)
            }
            withContext(ioDispatcher) { channel.close() }
            reader.cancel()
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RunnerRenovateLauncher::class.java)
        private val KILL_CONFIRM_TIMEOUT = 15.seconds
    }
}
