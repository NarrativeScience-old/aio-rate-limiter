"""Contains the main RateLimiter interface"""

from hashlib import sha1

import aioredis


# Adapted from http://redis.io/commands/incr#pattern-rate-limiter-2
INCREMENT_SCRIPT = b"""
    local current
    current = tonumber(redis.call("incrby", KEYS[1], ARGV[2]))
    if current == tonumber(ARGV[2]) then
        redis.call("expire", KEYS[1], ARGV[1])
    end
    return current
"""
INCREMENT_SCRIPT_HASH = sha1(INCREMENT_SCRIPT).hexdigest()


class TooManyRequests(Exception):
    """Occurs when the maximum number of requests is reached for a given resource"""

    pass


class RateLimiter:
    """Abstraction of a rate limit algorithm implemented on top of Redis >= 2.6.0"""

    def __init__(
        self,
        redis_pool: aioredis.ConnectionsPool,
        resource: str,
        client: str = "global",
        max_requests: int = 1,
        time_window: int = 1,
    ) -> None:
        """Class initialization method

        Args:
            redis_pool: aioredis connection pool
            resource: resource identifier string (e.g. user_pictures)
            client: client identifier string (e.g. 192.168.0.10)
            max_requests: maximum number of requests allowed within the time window
            time_window: time window in seconds to wait before resetting counters

        """
        self._redis = redis_pool
        self._rate_limit_key = f"rate_limit:{resource}:{client}"
        self._max_requests = max_requests
        self._time_window = time_window

    async def __aenter__(self) -> int:
        """Increment the counter when we enter the context

        Returns:
            current usage

        """
        return await self._increment_usage()

    async def __aexit__(self, *exc) -> None:
        """No-op when we leave the context"""
        pass

    async def _increment_usage(self) -> int:
        """Increment the resource usage by client

        Calls a LUA script to increments the resource usage. If the resource limit
        overflows the maximum number of requests, raise an exception.

        Returns:
            current usage

        Raises:
            TooManyRequests: if usage exceeds max requests

        """
        increment_by = 1
        keys = [self._rate_limit_key]
        args = [
            self._time_window,
            increment_by,
        ]
        try:
            current_usage = await self._redis.evalsha(INCREMENT_SCRIPT_HASH, keys, args)
        except aioredis.errors.RedisError:
            # Script is not yet stored
            current_usage = await self._redis.eval(INCREMENT_SCRIPT, keys, args)

        current_usage = int(current_usage)
        if current_usage > self._max_requests:
            raise TooManyRequests()

        return current_usage
