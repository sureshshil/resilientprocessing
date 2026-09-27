"""Outbox publisher: sends every job whose sendMe flag is set and due."""
import logging

from app.models import utcnow
from app.repository import JobRepository

logger = logging.getLogger(__name__)


async def publish_due(repo: JobRepository, sender) -> int:
    sent = 0
    for job in await repo.due_for_send(utcnow()):
        try:
            await sender.send(job["_id"])
        except Exception as exc:
            logger.warning("Publisher could not send %s (%s); will retry", job["_id"], type(exc).__name__)
            break  # the queue is probably down; try again next tick
        # A crash here means a second send later; the worker treats duplicates safely.
        await repo.mark_sent(job["_id"], job["sendAfter"], utcnow())
        sent += 1
    return sent
