"""Educational FastAPI Service Bus worker; see README before connecting a queue."""
import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager, suppress

from azure.identity.aio import DefaultAzureCredential
from azure.servicebus import ServiceBusReceiveMode
from azure.servicebus.aio import AutoLockRenewer, ServiceBusClient
from fastapi import FastAPI, HTTPException

from pipeline import PIPELINE_IMPLEMENTED, run_pipeline

logger = logging.getLogger(__name__)


async def consume_messages(receiver, stop_event):
    async with AutoLockRenewer() as renewer:
        while not stop_event.is_set():
            messages = await receiver.receive_messages(
                max_message_count=1, max_wait_time=5
            )
            for message in messages:
                renewer.register(receiver, message, max_lock_renewal_duration=900)
                try:
                    payload = json.loads(str(message))
                    if not isinstance(payload, dict):
                        raise ValueError("Expected an object")
                    interaction_id = payload.get("interactionId")
                    if not isinstance(interaction_id, str) or not interaction_id.strip():
                        raise ValueError("Expected a nonempty interactionId")
                except (ValueError, TypeError):
                    await receiver.dead_letter_message(
                        message, reason="INVALID_MESSAGE",
                        error_description="Expected a JSON object with interactionId.",
                    )
                    continue

                try:
                    await run_pipeline(interaction_id)
                except Exception as exc:
                    # Avoid logging provider bodies or sensitive exception text.
                    logger.error("Pipeline failed: %s", type(exc).__name__)
                    # Teaching example only: this is immediate redelivery.
                    # Implement durable delayed retry before production use.
                    await receiver.abandon_message(message)
                else:
                    await receiver.complete_message(message)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Fail before receiving messages until the real pipeline is connected.
    if not PIPELINE_IMPLEMENTED:
        raise RuntimeError("Connect your real pipeline before running this worker")
    stop_event = asyncio.Event()
    async with DefaultAzureCredential() as credential:
        async with ServiceBusClient(
            fully_qualified_namespace=os.environ["SERVICEBUS_NAMESPACE"],
            credential=credential,
        ) as client:
            async with client.get_queue_receiver(
                queue_name=os.environ["SERVICEBUS_QUEUE"],
                receive_mode=ServiceBusReceiveMode.PEEK_LOCK,
                prefetch_count=0,
            ) as receiver:
                task = asyncio.create_task(consume_messages(receiver, stop_event))
                app.state.consumer_task = task
                try:
                    yield
                finally:
                    stop_event.set()
                    try:
                        await asyncio.wait_for(asyncio.shield(task), timeout=30)
                    except asyncio.TimeoutError:
                        task.cancel()
                        with suppress(asyncio.CancelledError):
                            await task


app = FastAPI(lifespan=lifespan)


@app.get("/ready")
async def ready():
    if app.state.consumer_task.done():
        raise HTTPException(status_code=503, detail="Queue listener stopped")
    return {"status": "ready"}
