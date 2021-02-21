# aio-rate-limiter

[![](https://img.shields.io/pypi/v/aio-rate-limit.svg)](https://pypi.org/pypi/aio-rate-limit/) [![License](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)

Rate limit a function using Redis as a backend. This is a smaller library modeled after [python-redis-rate-limit](https://github.com/EvoluxBR/python-redis-rate-limit) but it uses [aioredis](https://github.com/aio-libs/aioredis). Supports Python 3.6+.

## Installation

```bash
pip install aio-rate-limiter
```

## Example

```python
import logging

from aio_rate_limiter import RateLimiter, TooManyRequests
import aioredis

async def example():
    pool = await aioredis.create_redis_pool("redis://localhost:6379")
    try:
        async with RateLimiter(
            redis_pool,
            # Rate limit requests to a resource
            "name-of-external-system",
            # Allow up to 100 requests in 60 seconds
            max_requests=100,
            time_window=60
        ):
            async do_work()
    except TooManyRequests:
        logging.warning("Try again later")
```

## Development

```bash
# Install poetry
pip install poetry

# Install all package dependencies
poetry install

# Launch a shell with dependencies available
poetry shell

# Run tests (requires Redis server running at localhost:6379)
pytest

# When you're ready to publish...
# Bump version
poetry version <version>
# Set your pypi token
export POETRY_PYPI_TOKEN_PYPI='...'
# Build and publish
poetry build
poetry publish
```
