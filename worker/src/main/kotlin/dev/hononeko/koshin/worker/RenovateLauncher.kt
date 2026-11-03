package dev.hononeko.koshin.worker

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import kotlinx.coroutines.slf4j.MDCContext
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.slf4j.LoggerFactory
import java.io.IOException
import java.io.InputStream
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicReference
import java.util.stream.Stream
import kotlin.io.path.createDirectories
import kotlin.time.Duration.Companion.seconds

@Serializable
enum class OutputStreamName {
    @SerialName("stdout")
    STDOUT,

    @SerialName("stderr")
    STDERR,
    ;

    val label: String get() = name.lowercase()
}

/** Starts Renovate, in this container ([LocalRenovateLauncher]) or the runner's ([RunnerRenovateLauncher]). */
fun interface RenovateLauncher {
    /** Starts [command] in [directory] with exactly [env]; [onOutput] gets each line Renovate prints. */
    suspend fun launch(
        command: List<String>,
        env: Map<String, String>,
        directory: Path,
        onOutput: (OutputStreamName, String) -> Unit,
    ): RenovateRun
}

interface RenovateRun {
    /** Suspends until Renovate exits and all its output went to `onOutput`; returns the exit code. */
    suspend fun awaitExit(): Int

    /** Kills Renovate and whatever it started, waiting (bounded) until they're gone. Safe to call again. */
    suspend fun destroy()
}

/**
 * Kills what's left of a run, waiting until it's gone: dead, and reaped if it became a child of this JVM, so no
 * zombie stays behind (KSH-23). Blocking. Assumes one run at a time, so whatever is left over belongs to that run.
 */
fun interface ProcessKiller {
    fun kill(process: Process)

    companion object {
        private val logger = LoggerFactory.getLogger(ProcessKiller::class.java)
        private val KILL_WAIT = 5.seconds
        private const val POLL_MILLIS = 10L
        private const val MAX_SWEEPS = 5

        /** Renovate and its descendants. A daemonized process is no longer a descendant, so it survives. */
        val TREE = ProcessKiller { process ->
            // Snapshot first: once the root dies its children are reparented and no longer reachable.
            killAndAwait(process, listOf(process.toHandle()) + process.descendants().toList())
        }

        /**
         * Everything in [others] as well as Renovate's tree, until nothing is left. Where Renovate has a container to
         * itself (the runner), [others] is every process but the runner and its ancestors, so nothing a run leaves
         * behind, daemonized or not, outlives it. The container's init (`dumb-init`, from containerbase's
         * entrypoint) reaps them; when the runner is PID 1 itself, it does.
         */
        fun sweep(others: () -> Stream<ProcessHandle> = ::othersInContainer) = ProcessKiller { process ->
            repeat(MAX_SWEEPS) {
                // Again and again: a process can fork between listing and killing.
                val targets = buildList {
                    add(process.toHandle())
                    addAll(process.descendants().toList())
                    addAll(others().toList())
                }.filter { handle -> handle.isAlive }.distinctBy { handle -> handle.pid() }
                if (targets.isEmpty()) return@ProcessKiller
                killAndAwait(process, targets)
            }
            logger.warn("Processes still alive after {} sweeps", MAX_SWEEPS)
        }

        /**
         * Where Renovate shares this JVM's container (no runner): on Linux this JVM becomes a child subreaper, so what
         * a run daemonizes (`setsid()`, e.g. a Bazel server or gpg-agent) is reparented to it rather than init, then
         * swept with the run and reaped. Elsewhere, [TREE].
         */
        fun adopting(): ProcessKiller = adoptingKiller

        private val adoptingKiller: ProcessKiller by lazy {
            if (Posix.becomeChildSubreaper()) {
                // Every descendant of this JVM is the run's: it runs one at a time and nothing else
                sweep { ProcessHandle.current().descendants() }
            } else {
                logger.info("Not a child subreaper (Linux only): processes a run daemonizes may outlive it")
                TREE
            }
        }

        private fun othersInContainer(): Stream<ProcessHandle> {
            val self = ProcessHandle.current()
            val spared = generateSequence(self) { it.parent().orElse(null) }.map { it.pid() }.toSet()
            return ProcessHandle.allProcesses().filter { it.pid() !in spared }
        }

        /**
         * Kills [targets] and waits until all are gone, so nothing still writes to the workspace while it's deleted.
         * Those that became this JVM's children (orphans it adopted as PID 1 or a subreaper) are reaped: to
         * [ProcessHandle], a zombie is alive. [process] is left to the JVM, which reaps the processes it starts.
         */
        private fun killAndAwait(process: Process, targets: List<ProcessHandle>) {
            targets.forEach { handle ->
                if (handle.pid() != process.pid()) {
                    logger.debug("Killing leftover pid {} ({})", handle.pid(), handle.info().command().orElse("?"))
                }
                handle.destroyForcibly()
            }
            val self = ProcessHandle.current().pid()
            val deadline = System.nanoTime() + KILL_WAIT.inWholeNanoseconds
            var pending = targets
            while (pending.isNotEmpty() && System.nanoTime() < deadline) {
                pending = pending.filterNot { handle -> isGone(handle, self, process.pid()) }
                if (pending.isNotEmpty()) Thread.sleep(POLL_MILLIS)
            }
        }

        private fun isGone(handle: ProcessHandle, self: Long, started: Long): Boolean {
            if (!handle.isAlive) return true
            val adopted = handle.pid() != started && handle.parent().map { it.pid() }.orElse(0L) == self
            return adopted && Posix.reap(handle.pid())
        }

        /** Whether this process is PID 1 or its direct child, i.e. the main process of its container. */
        fun isContainerMain(): Boolean = ProcessHandle.current().pid() == 1L ||
            ProcessHandle.current().parent().map { it.pid() }.orElse(0L) == 1L
    }
}

/** Runs Renovate as a child of this JVM. */
class LocalRenovateLauncher(
    private val killer: ProcessKiller = ProcessKiller.adopting(),
    private val ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
) : RenovateLauncher {

    override suspend fun launch(
        command: List<String>,
        env: Map<String, String>,
        directory: Path,
        onOutput: (OutputStreamName, String) -> Unit,
    ): RenovateRun {
        val started = AtomicReference<Process>()
        try {
            withContext(ioDispatcher) {
                directory.createDirectories()
                val process = ProcessBuilder(command)
                    .directory(directory.toFile())
                    .apply {
                        environment().clear()
                        environment().putAll(env)
                    }
                    .start()
                started.set(process)
                process.outputStream.close()
            }
        } catch (e: CancellationException) {
            // Cancelled once it had started: nobody else can kill it.
            started.get()?.let { withContext(NonCancellable + ioDispatcher) { killer.kill(it) } }
            throw e
        }
        val process = started.get()
        logger.debug("Started {} (pid {})", command.first(), process.pid())
        // Not children of the caller: a blocked read must never hold up cancellation. They keep the job's MDC.
        val pumps = CoroutineScope(ioDispatcher + MDCContext()).launch {
            launch { pump(process.inputStream, OutputStreamName.STDOUT, onOutput) }
            launch { pump(process.errorStream, OutputStreamName.STDERR, onOutput) }
        }
        return LocalRun(process, pumps)
    }

    private inner class LocalRun(private val process: Process, private val pumps: Job) : RenovateRun {
        override suspend fun awaitExit(): Int {
            process.onExit().await()
            awaitOutput()
            return process.exitValue()
        }

        override suspend fun destroy() {
            withContext(ioDispatcher) { killer.kill(process) }
            awaitOutput()
        }

        private suspend fun awaitOutput() {
            if (pumps.isCompleted) return
            withTimeoutOrNull(OUTPUT_DRAIN_TIMEOUT) { pumps.join() }
                ?: logger.warn("Output of pid {} still open after exit; abandoning", process.pid())
        }
    }

    private fun pump(stream: InputStream, name: OutputStreamName, onOutput: (OutputStreamName, String) -> Unit) {
        try {
            stream.bufferedReader().useLines { lines -> lines.forEach { onOutput(name, it) } }
        } catch (@Suppress("SwallowedException") _: IOException) {
            // Stream closed while the process was being killed
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(LocalRenovateLauncher::class.java)
        private val OUTPUT_DRAIN_TIMEOUT = 2.seconds
    }
}
