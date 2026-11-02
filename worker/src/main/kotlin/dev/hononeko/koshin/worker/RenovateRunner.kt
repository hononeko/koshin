package dev.hononeko.koshin.worker

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import java.io.BufferedReader
import java.io.IOException
import java.io.OutputStream
import java.net.StandardProtocolFamily
import java.net.UnixDomainSocketAddress
import java.nio.channels.Channels
import java.nio.channels.ClosedChannelException
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel
import java.nio.file.Path
import kotlin.io.path.Path
import kotlin.io.path.createDirectories
import kotlin.io.path.deleteIfExists

/**
 * Runs Renovate for the worker in a container of its own (KSH-21), so neither Renovate nor the repo code it may run can
 * read the worker's secrets (its environment, through `/proc`) or its ServiceAccount token. Renovate gets exactly the
 * environment each [RunRequest] carries.
 *
 * One run at a time, over the Unix socket at [socket]. Once a run is over, [launcher] kills what's left of it (the
 * whole container, with [ProcessKiller.sweep]) and the socket is bound afresh before the exit code goes out, so no
 * process from one run can watch or intercept the next.
 */
class RenovateRunner(
    private val socket: Path,
    private val launcher: RenovateLauncher,
    private val ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
) : AutoCloseable {

    @Volatile
    private var server: ServerSocketChannel? = null

    @Volatile
    private var current: RenovateRun? = null

    @Volatile
    private var closed = false

    /** Serves runs until [close]. */
    fun serve() {
        bind()
        while (!closed) {
            val channel = try {
                checkNotNull(server).accept()
            } catch (e: ClosedChannelException) {
                if (closed) return
                throw e
            }
            channel.use { runBlocking { handle(it) } }
        }
    }

    /** Stops serving and kills the run in progress. */
    override fun close() {
        closed = true
        server?.close()
        current?.let { runBlocking { it.destroy() } }
    }

    private fun bind() {
        socket.parent?.createDirectories()
        socket.deleteIfExists()
        server = ServerSocketChannel.open(StandardProtocolFamily.UNIX).apply {
            bind(UnixDomainSocketAddress.of(socket))
        }
    }

    private suspend fun handle(channel: SocketChannel) {
        val input = Channels.newInputStream(channel).bufferedReader()
        val replies = Replies(Channels.newOutputStream(channel))
        val request = readRequest(input, replies) ?: return
        val run = try {
            launcher.launch(request.command, request.env, Path(request.directory)) { stream, line ->
                replies.send(RunnerMessage.Output(stream, line))
            }
        } catch (e: IOException) {
            logger.warn("Could not start {}", request.command.first(), e)
            replies.send(RunnerMessage.Failed(e.message ?: e.javaClass.simpleName))
            return
        }
        current = run
        val command = request.command.joinToString(" ")
        logger.info("Started {}", command)

        // The worker closing its side, or going away, means kill the run.
        val watcher = CoroutineScope(ioDispatcher).launch {
            try {
                // Nothing else is expected: this returns once the worker's side is closed.
                input.skip(Long.MAX_VALUE)
            } catch (@Suppress("SwallowedException") _: IOException) {
                // Gone
            }
            if (current === run) run.destroy()
        }
        val code = try {
            run.awaitExit()
        } finally {
            withContext(NonCancellable) {
                current = null
                run.destroy()
                // Stop the watcher before the next run: its kill would sweep that run too.
                runCatching { channel.shutdownInput() }
                watcher.join()
            }
        }
        logger.info("{} exited with code {}", command, code)
        // Before replying: a process from this run may have replaced the socket while it ran.
        if (!closed) {
            server?.close()
            bind()
        }
        replies.send(RunnerMessage.Exited(code))
    }

    private fun readRequest(input: BufferedReader, replies: Replies): RunRequest? {
        val line = try {
            input.readLine()
        } catch (@Suppress("SwallowedException") _: IOException) {
            null
        } ?: return null
        return try {
            Json.decodeFromString<RunRequest>(line).also { require(it.command.isNotEmpty()) { "Empty command" } }
        } catch (e: IllegalArgumentException) {
            // Including SerializationException
            replies.send(RunnerMessage.Failed("Invalid request: ${e.message}"))
            null
        }
    }

    /** Thread-safe replies; once the worker is gone, they're dropped. */
    private class Replies(output: OutputStream) {
        private val writer = output.bufferedWriter()
        private var broken = false

        @Synchronized
        fun send(message: RunnerMessage) {
            if (broken) return
            try {
                writer.write(Json.encodeToString<RunnerMessage>(message))
                writer.write("\n")
                writer.flush()
            } catch (@Suppress("SwallowedException") _: IOException) {
                broken = true
            }
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RenovateRunner::class.java)
        const val DEFAULT_SOCKET = "/run/koshin/runner.sock"
    }
}

/** Entry point of the `renovate` container. */
fun main() {
    checkLogLevels()
    val logger = LoggerFactory.getLogger(RenovateRunner::class.java)
    val socket = Path(System.getenv("RUNNER_SOCKET")?.takeIf { it.isNotBlank() } ?: RenovateRunner.DEFAULT_SOCKET)
    val killer = if (ProcessKiller.isContainerMain()) {
        ProcessKiller.sweep()
    } else {
        logger.warn("Not the main process of a container: only each run's own process tree is killed after it")
        ProcessKiller.TREE
    }
    val runner = RenovateRunner(socket, LocalRenovateLauncher(killer))
    Runtime.getRuntime().addShutdownHook(Thread(runner::close))
    logger.info("Renovate runner listening on {}", socket)
    runner.serve()
}
