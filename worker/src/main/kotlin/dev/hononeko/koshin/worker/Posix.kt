package dev.hononeko.koshin.worker

import org.slf4j.LoggerFactory
import java.lang.foreign.FunctionDescriptor
import java.lang.foreign.Linker
import java.lang.foreign.MemorySegment
import java.lang.foreign.ValueLayout.ADDRESS
import java.lang.foreign.ValueLayout.JAVA_INT
import java.lang.foreign.ValueLayout.JAVA_LONG
import java.lang.invoke.MethodHandle

/**
 * The process calls the JVM lacks, through the FFM API (`--enable-native-access=ALL-UNNAMED` keeps it quiet): becoming
 * a child subreaper, and reaping a child the JVM didn't start, which [ProcessHandle] reports alive as a zombie forever.
 */
internal object Posix {
    private val logger = LoggerFactory.getLogger(Posix::class.java)
    private const val PR_SET_CHILD_SUBREAPER = 36
    private const val WNOHANG = 1

    private val prctl: MethodHandle? = downcall(
        "prctl",
        FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_LONG),
        Linker.Option.firstVariadicArg(1),
    )
    private val waitpid: MethodHandle? =
        downcall("waitpid", FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT))

    private fun downcall(name: String, descriptor: FunctionDescriptor, vararg options: Linker.Option): MethodHandle? =
        try {
            val linker = Linker.nativeLinker()
            linker.defaultLookup().find(name).map { linker.downcallHandle(it, descriptor, *options) }.orElse(null)
        } catch (@Suppress("TooGenericExceptionCaught") e: RuntimeException) {
            // e.g. native access denied
            logger.debug("No native {}", name, e)
            null
        }

    /**
     * Makes orphaned descendants (e.g. daemons that `setsid()`) children of this JVM instead of init, so they can be
     * found and reaped (Linux 3.4+). False where unsupported.
     */
    fun becomeChildSubreaper(): Boolean {
        val call = prctl ?: return false
        return try {
            call.invokeExact(PR_SET_CHILD_SUBREAPER, 1L) as Int == 0
        } catch (@Suppress("TooGenericExceptionCaught") e: Throwable) {
            logger.debug("prctl(PR_SET_CHILD_SUBREAPER) failed", e)
            false
        }
    }

    /**
     * Reaps [pid], a child of this JVM, if it has exited. True once it's reaped or isn't our child (anymore); false
     * while it still runs. Never call it for a [Process] this JVM started: the JVM reaps those itself.
     */
    fun reap(pid: Long): Boolean {
        val call = waitpid ?: return true
        return try {
            call.invokeExact(pid.toInt(), MemorySegment.NULL, WNOHANG) as Int != 0
        } catch (@Suppress("TooGenericExceptionCaught") e: Throwable) {
            logger.debug("waitpid({}) failed", pid, e)
            true
        }
    }
}
