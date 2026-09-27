"""Service Bus access: the local emulator (connection string) or Azure (DefaultAzureCredential)."""
import json
from contextlib import asynccontextmanager

from azure.identity.aio import DefaultAzureCredential
from azure.servicebus import ServiceBusMessage
from azure.servicebus.aio import ServiceBusClient

from app.config import Settings


@asynccontextmanager
async def servicebus_client(settings: Settings):
    if settings.servicebus_connection_string:
        async with ServiceBusClient.from_connection_string(settings.servicebus_connection_string) as client:
            yield client
    else:
        async with DefaultAzureCredential() as credential:
            async with ServiceBusClient(settings.servicebus_namespace, credential) as client:
                yield client


class QueueSender:
    def __init__(self, sender):
        self._sender = sender

    async def send(self, job_id: str) -> None:
        message = ServiceBusMessage(json.dumps({"jobId": job_id}), content_type="application/json")
        await self._sender.send_messages(message)
