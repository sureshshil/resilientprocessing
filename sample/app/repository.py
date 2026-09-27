"""All MongoDB access. Every write for a running job is fenced by the lease token."""
import asyncio
import logging
import random
import uuid
from datetime import datetime, timedelta

from pymongo import ASCENDING, DESCENDING, IndexModel, ReturnDocument
from pymongo.errors import AutoReconnect, DuplicateKeyError, OperationFailure

from app.config import Settings
from app.errors import LeaseLost, RepositoryUnavailable
from app.models import Status

logger = logging.getLogger(__name__)

THROTTLED = 16500  # Cosmos DB for MongoDB: "request rate is large"


def _retryable(exc: Exception) -> bool:
    # AutoReconnect covers NetworkTimeout and ServerSelectionTimeoutError.
    if isinstance(exc, AutoReconnect):
        return True
    return isinstance(exc, OperationFailure) and exc.code == THROTTLED


class JobRepository:
    def __init__(self, db, settings: Settings, rng: random.Random | None = None):
        self.jobs = db["jobs"]
        self.settings = settings
        self.rng = rng or random.Random()

    async def _call(self, operation):
        """Run `operation` (a zero-argument coroutine function), retrying throttling and
        network errors. Fenced updates are safe to repeat; a repeated `$inc` may over-count
        `tries`, which is acceptable for this sample."""
        tries = self.settings.mongo_tries
        for attempt in range(tries):
            try:
                if self.rng.random() < self.settings.fault_cosmos_429_rate:
                    raise OperationFailure("Injected: request rate is large", code=THROTTLED)
                return await operation()
            except Exception as exc:
                if not _retryable(exc):
                    raise
                if attempt == tries - 1:
                    raise RepositoryUnavailable(type(exc).__name__) from exc
                logger.warning("Mongo call failed (%s); retrying", type(exc).__name__)
                await asyncio.sleep(self.settings.mongo_backoff_seconds * 2**attempt)

    async def ensure_indexes(self) -> None:
        # Cosmos DB for MongoDB (RU) indexes only _id by default.
        await self._call(lambda: self.jobs.create_indexes([
            IndexModel([("sendMe", ASCENDING), ("sendAfter", ASCENDING)]),
            IndexModel([("status", ASCENDING), ("owner.expiresAt", ASCENDING)]),
            IndexModel([("status", ASCENDING), ("updatedAt", ASCENDING)]),
            IndexModel([("createdAt", DESCENDING)]),
        ]))

    async def insert_job(self, doc: dict) -> bool:
        """False when a job with the same id already exists."""
        try:
            await self._call(lambda: self.jobs.insert_one(doc))
        except DuplicateKeyError:
            return False
        return True

    async def get(self, job_id: str) -> dict | None:
        return await self._call(lambda: self.jobs.find_one({"_id": job_id}))

    async def recent(self, limit: int = 20) -> list[dict]:
        return await self._call(
            lambda: self.jobs.find().sort("createdAt", DESCENDING).limit(limit).to_list()
        )

    # --- outbox ---------------------------------------------------------------------------

    async def due_for_send(self, now: datetime, limit: int = 50) -> list[dict]:
        return await self._call(lambda: self.jobs.find(
            {"sendMe": True, "sendAfter": {"$lte": now}}, {"sendAfter": 1}
        ).limit(limit).to_list())

    async def mark_sent(self, job_id: str, send_after: datetime, now: datetime) -> bool:
        """Clear the flag only if nobody asked for a newer send in the meantime."""
        result = await self._call(lambda: self.jobs.update_one(
            {"_id": job_id, "sendMe": True, "sendAfter": send_after},
            {"$set": {"sendMe": False, "updatedAt": now}},
        ))
        return result.modified_count == 1

    # --- ownership ------------------------------------------------------------------------

    async def claim(self, job_id: str, worker_id: str, now: datetime) -> dict | None:
        """Take ownership if nobody holds a valid lease. Returns the claimed document."""
        owner = {
            "token": uuid.uuid4().hex,
            "workerId": worker_id,
            "expiresAt": now + timedelta(seconds=self.settings.lease_seconds),
        }
        return await self._call(lambda: self.jobs.find_one_and_update(
            {
                "_id": job_id,
                "status": {"$in": [Status.PENDING, Status.RETRYING, Status.RUNNING]},
                "$or": [{"owner": None}, {"owner.expiresAt": {"$lt": now}}],
            },
            {"$set": {"status": Status.RUNNING, "owner": owner, "sendMe": False, "updatedAt": now}},
            return_document=ReturnDocument.AFTER,
        ))

    async def _fenced(self, job_id: str, token: str, update: dict, now: datetime) -> None:
        update.setdefault("$set", {})["updatedAt"] = now
        result = await self._call(
            lambda: self.jobs.update_one({"_id": job_id, "owner.token": token}, update)
        )
        if result.matched_count == 0:
            raise LeaseLost(job_id)

    async def heartbeat(self, job_id: str, token: str, now: datetime) -> bool:
        expires = now + timedelta(seconds=self.settings.lease_seconds)
        try:
            await self._fenced(job_id, token, {"$set": {"owner.expiresAt": expires}}, now)
        except LeaseLost:
            return False
        return True

    # --- stage checkpoints ----------------------------------------------------------------

    async def start_stage(self, job_id: str, token: str, stage: str, now: datetime) -> int:
        """Mark the stage RUNNING and return its total try count, including this one."""
        doc = await self._call(lambda: self.jobs.find_one_and_update(
            {"_id": job_id, "owner.token": token},
            {
                "$set": {f"stages.{stage}.status": Status.RUNNING, "updatedAt": now},
                "$inc": {f"stages.{stage}.tries": 1},
            },
            projection={f"stages.{stage}.tries": 1},
            return_document=ReturnDocument.AFTER,
        ))
        if doc is None:
            raise LeaseLost(job_id)
        return doc["stages"][stage]["tries"]

    async def complete_stage(self, job_id, token, stage, output, now) -> None:
        await self._fenced(job_id, token, {"$set": {
            f"stages.{stage}.status": Status.COMPLETED,
            f"stages.{stage}.output": output,
            f"stages.{stage}.finishedAt": now,
        }}, now)

    # --- final outcomes -------------------------------------------------------------------

    async def finish_completed(self, job_id, token, now) -> None:
        await self._fenced(job_id, token, {"$set": {
            "status": Status.COMPLETED, "owner": None, "error": None,
        }}, now)

    async def finish_failed(self, job_id, token, stage, error, now) -> None:
        await self._fenced(job_id, token, {"$set": {
            "status": Status.FAILED, f"stages.{stage}.status": Status.FAILED,
            "owner": None, "error": error,
        }}, now)

    async def schedule_retry(self, job_id, token, stage, error, send_after, now) -> None:
        """One atomic write: RETRYING + due time + outbox flag + release ownership."""
        await self._fenced(job_id, token, {
            "$set": {
                "status": Status.RETRYING, f"stages.{stage}.status": Status.PENDING,
                "owner": None, "error": error, "sendMe": True, "sendAfter": send_after,
            },
            "$inc": {"attempts": 1},
        }, now)

    # --- recovery -------------------------------------------------------------------------

    async def sweep(self, now: datetime, running_before: datetime, unsent_before: datetime) -> int:
        """Flag stuck jobs for the publisher. Never sends and never changes status."""
        flag = {"$set": {"sendMe": True, "sendAfter": now, "updatedAt": now}}
        dead_owner = await self._call(lambda: self.jobs.update_many({
            "status": Status.RUNNING, "sendMe": False,
            "owner.expiresAt": {"$lt": running_before}, "updatedAt": {"$lt": running_before},
        }, flag))
        lost_message = await self._call(lambda: self.jobs.update_many({
            "status": {"$in": [Status.PENDING, Status.RETRYING]}, "sendMe": False,
            "updatedAt": {"$lt": unsent_before},
        }, flag))
        return dead_owner.modified_count + lost_message.modified_count
