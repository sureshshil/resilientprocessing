"""API app: submit and read jobs, serve the UI, and run the publisher and sweeper loops."""
import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path

from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import FileResponse, JSONResponse
from pymongo import AsyncMongoClient

from app.config import Settings, load_settings
from app.errors import RepositoryUnavailable
from app.loops import run_periodically
from app.models import STAGES, SubmitRequest, new_job_document, utcnow
from app.publisher import publish_due
from app.queue import QueueSender, servicebus_client
from app.repository import JobRepository
from app.sweeper import sweep_stuck

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"


def job_view(doc: dict) -> dict:
    return {
        "id": doc["_id"],
        "key": doc["key"],
        "status": doc["status"],
        "stages": {
            name: {k: doc["stages"][name][k] for k in ("status", "tries", "output", "finishedAt")}
            for name in STAGES
        },
        "attempts": doc["attempts"],
        "error": doc["error"],
        "sendMe": doc["sendMe"],
        "sendAfter": doc["sendAfter"],
        "owner": doc["owner"]["workerId"] if doc["owner"] else None,
        "faults": doc.get("faults", {}),
        "createdAt": doc["createdAt"],
        "updatedAt": doc["updatedAt"],
    }


def create_app(settings: Settings | None = None, repo: JobRepository | None = None,
               sender=None) -> FastAPI:
    """Pass `repo` and `sender` (tests) to skip real connections and background loops."""
    settings = settings or load_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if repo is not None:
            yield
            return
        mongo = AsyncMongoClient(settings.mongo_url, tz_aware=True, serverSelectionTimeoutMS=5000)
        app.state.repo = JobRepository(mongo[settings.mongo_db], settings)
        await app.state.repo.ensure_indexes()
        stop = asyncio.Event()
        async with servicebus_client(settings) as sb, sb.get_queue_sender(settings.servicebus_queue) as sb_sender:
            app.state.sender = QueueSender(sb_sender)
            loops = [
                asyncio.create_task(run_periodically(
                    "publisher", settings.publish_interval_seconds,
                    lambda: publish_due(app.state.repo, app.state.sender), stop)),
                asyncio.create_task(run_periodically(
                    "sweeper", settings.sweep_interval_seconds,
                    lambda: sweep_stuck(app.state.repo, settings), stop)),
            ]
            try:
                yield
            finally:
                stop.set()
                await asyncio.gather(*loops, return_exceptions=True)
        await mongo.close()

    app = FastAPI(title="FPA pipeline sample API", lifespan=lifespan)
    if repo is not None:
        app.state.repo, app.state.sender = repo, sender

    @app.exception_handler(RepositoryUnavailable)
    async def database_unavailable(request, exc):
        return JSONResponse({"detail": "Database unavailable, try again"}, status_code=503)

    @app.post("/jobs", status_code=201)
    async def submit(req: SubmitRequest, response: Response):
        if req.recordingType == "consent":
            response.status_code = 200
            return {"status": "SKIPPED", "id": None}

        now = utcnow()
        # sendAfter gives our own send a head start so the publisher doesn't race it.
        doc = new_job_document(req, now, now + timedelta(seconds=settings.publish_grace_seconds))
        if not await app.state.repo.insert_job(doc):
            response.status_code = 200
            return job_view(await app.state.repo.get(req.job_id))

        try:
            await app.state.sender.send(doc["_id"])
            await app.state.repo.mark_sent(doc["_id"], doc["sendAfter"], utcnow())
        except Exception as exc:
            logger.warning("Immediate send failed for %s (%s); the publisher will send it",
                           doc["_id"], type(exc).__name__)
        return job_view(await app.state.repo.get(doc["_id"]))

    @app.get("/jobs")
    async def list_jobs():
        return [job_view(doc) for doc in await app.state.repo.recent()]

    @app.get("/jobs/{job_id}")
    async def get_job(job_id: str):
        doc = await app.state.repo.get(job_id)
        if doc is None:
            raise HTTPException(status_code=404, detail="Job not found")
        return job_view(doc)

    @app.get("/", include_in_schema=False)
    async def index():
        return FileResponse(STATIC_DIR / "index.html")

    return app


app = create_app()
