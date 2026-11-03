package dev.hononeko.koshin.adapters.valkey

import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import io.mockk.coEvery
import io.mockk.mockk
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

@OptIn(ExperimentalLettuceCoroutinesApi::class)
class ValkeyLeaderLeaseUnitTest {
    @Test
    fun `script results map to holding the lease, and bad arguments are rejected`() = runTest {
        val commands = mockk<RedisCoroutinesCommands<String, String>>()
        coEvery { commands.scriptLoad(any<String>()) } returns "sha"
        coEvery { commands.evalsha<Long>("sha", ScriptOutputType.INTEGER, any<Array<String>>(), "a", "1000") } returns
            1L andThen 0L
        coEvery { commands.evalsha<Long>("sha", ScriptOutputType.INTEGER, any<Array<String>>(), "a") } returns 1L
        val mocked = ValkeyLeaderLeaseAdapter(commands)

        assertTrue(mocked.acquireOrRenew("a", 1.seconds))
        assertFalse(mocked.acquireOrRenew("a", 1.seconds))
        mocked.release("a")
        assertThrows<IllegalArgumentException> { mocked.acquireOrRenew(" ", 1.seconds) }
        assertThrows<IllegalArgumentException> { mocked.acquireOrRenew("a", Duration.ZERO) }
    }
}
