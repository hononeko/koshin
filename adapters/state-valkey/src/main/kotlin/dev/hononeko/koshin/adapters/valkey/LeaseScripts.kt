package dev.hononeko.koshin.adapters.valkey

/**
 * Lua snippets for leases in a ZSET (`member → expiry`), timed by Valkey's own clock (`TIME`) so clock skew between
 * pods can't expire another pod's lease.
 */
internal object LeaseScripts {
    /** Sets `now` to Valkey's time in milliseconds. */
    val NOW =
        """
        local t = redis.call('TIME')
        local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
        """.trimIndent()

    /**
     * Leases `ARGV[1]` in `KEYS[1]` for `ttl` ms. Keeps the ZSET's own TTL at least as long as its longest lease, so an
     * abandoned key goes away.
     */
    val LEASE =
        """
        redis.call('ZADD', KEYS[1], now + ttl, ARGV[1])
        if redis.call('PTTL', KEYS[1]) < ttl then redis.call('PEXPIRE', KEYS[1], ttl) end
        return 1
        """.trimIndent()
}
