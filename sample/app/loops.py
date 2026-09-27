"""Background loop helper for the publisher and sweeper."""
import asyncio
import logging
from contextlib import suppress

logger = logging.getLogger(__name__)


async def run_periodically(name: str, interval: float, fn, stop: asyncio.Event) -> None:
    """Call `fn()` every `interval` seconds until `stop` is set. A failing tick is logged, not fatal."""
    while not stop.is_set():
        try:
            await fn()
        except Exception:
            logger.exception("%s tick failed", name)
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=interval)
