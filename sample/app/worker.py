"""Worker app: consumes the queue one message at a time and settles each message
only after the job's state has been saved."""
import asyncio
import logging
from contextlib import asynccontextmanager, suppress

from azure.servicebus import ServiceBusReceiveMode
from azure.servicebus.aio import AutoLockRenewer
from azure.servicebus.exceptions import ServiceBusError
from fastapi import FastAPI, HTTPException
from pymongo import AsyncMongoClient

from app.config import Settings, load_settings
from app.processing import Settlement, process_message
from app.queue import servicebus_client
from app.repository import JobRepository

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

MAX_LOCK_RENEWAL_SECONDS = 3600
SHUTDOWN_GRACE_SECONDS = 30


async def consume(receiver, repo: JobRepository, settings: Settings, stop: asyncio.Event) -> None:
    async with AutoLockRenewer() as renewer:
        while not stop.is_set():
            try:
                messages = await receiver.receive_messages(max_message_count=1, max_wait_time=5)
                for message in messages:
                    renewer.register(receiver, message, max_lock_renewal_duration=MAX_LOCK_RENEWAL_SECONDS)
                    settlement = await process_message(str(message), repo, settings)
                    await _settle(receiver, message, settlement)
            except ServiceBusError as exc:
                logger.error("Service Bus error (%s); retrying in 5s", type(exc).__name__)
                await asyncio.sleep(5)


async def _settle(receiver, message, settlement: Settlement) -> None:
    try:
        if settlement.action == "complete":
            await receiver.complete_message(message)
        elif settlement.action == "dead_letter":
            await receiver.dead_letter_message(
                message, reason=settlement.reason, error_description="See worker logs.")
        else:
            await receiver.abandon_message(message)
    except ServiceBusError as exc:
        # e.g. the lock was lost: the message will be redelivered, which is safe.
        logger.warning("Could not %s message (%s)", settlement.action, type(exc).__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or load_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        mongo = AsyncMongoClient(settings.mongo_url, tz_aware=True, serverSelectionTimeoutMS=5000)
        repo = JobRepository(mongo[settings.mongo_db], settings)
        stop = asyncio.Event()
        async with servicebus_client(settings) as sb, sb.get_queue_receiver(
            queue_name=settings.servicebus_queue,
            receive_mode=ServiceBusReceiveMode.PEEK_LOCK,
            prefetch_count=0,
        ) as receiver:
            task = asyncio.create_task(consume(receiver, repo, settings, stop))
            app.state.consumer = task
            logger.info("Worker %s consuming %s", settings.worker_id, settings.servicebus_queue)
            try:
                yield
            finally:
                stop.set()
                try:
                    await asyncio.wait_for(asyncio.shield(task), timeout=SHUTDOWN_GRACE_SECONDS)
                except asyncio.TimeoutError:
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
        await mongo.close()

    app = FastAPI(title="FPA pipeline sample worker", lifespan=lifespan)

    @app.get("/ready")
    async def ready():
        if app.state.consumer.done():
            raise HTTPException(status_code=503, detail="Queue consumer stopped")
        return {"status": "ready", "workerId": settings.worker_id}

    return app


app = create_app()
