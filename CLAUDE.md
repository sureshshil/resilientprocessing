# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

The FPA insurance recording/analysis pipeline design (migration from FastAPI `BackgroundTasks` to Azure Service Bus + a separate AKS worker, with Cosmos DB for MongoDB as the source of truth), plus two code examples:

- **Root (`main.py`, `pipeline.py`)**: a minimal teaching consumer with no tests.
- **[sample/](sample/)**: the runnable medium-level sample with tests. **Most work happens here.**

## sample/ (the resilient pipeline sample)

Run all commands from `sample/`:

```powershell
python -m venv .venv; .\.venv\Scripts\python -m pip install -r requirements.txt
docker compose up -d mongo                                  # tests need only Mongo
.\.venv\Scripts\python -m pytest                            # all tests
.\.venv\Scripts\python -m pytest tests/test_runner.py -k resume   # a single test
docker compose up -d                                        # + Service Bus emulator (needs ACCEPT_EULA=Y in .env)
.\.venv\Scripts\uvicorn app.api:app --port 8000             # API + UI + publisher + sweeper
.\.venv\Scripts\uvicorn app.worker:app --port 8001          # worker
```

Architecture (spec: [docs/superpowers/specs/2026-09-27-resilient-pipeline-sample-design.md](docs/superpowers/specs/2026-09-27-resilient-pipeline-sample-design.md)):
- One Mongo document per audio recording, `_id = sessionId:recordingId`. It holds the stage checkpoints, the lease (`owner.token`/`expiresAt`), the outbox flag (`sendMe`/`sendAfter`) and `attempts`.
- **All Mongo access goes through `app/repository.py`.**
  - `_call` retries Cosmos 16500 throttling and network errors, then raises `RepositoryUnavailable`.
  - Every write for a running job is fenced on `owner.token` and raises `LeaseLost` on a mismatch. Keep new writes fenced and single-document.
- **Only the publisher and the API's immediate send put messages on the queue.** The sweeper and `schedule_retry` just set `sendMe=true`.
- `runner.py` is Service Bus-agnostic and returns an `Outcome`. `processing.py` turns that outcome into Mongo writes plus a `Settlement` (complete/dead_letter/abandon). `worker.py` only applies the settlement.
- Tests use a real Mongo (a random db per test), `FakeQueue` from `tests/conftest.py`, and `tests/helpers.py` (`make_request`, `insert_job`). They pass `stage_fn` or job `faults` to drive failures.
- Real FPA stage code plugs into `app/stages.py::run_stage`. The error classification lives in `app/errors.py`.

## Root teaching example

### Commands

Python 3.11+. Windows/PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
$env:SERVICEBUS_NAMESPACE = 'your-namespace.servicebus.windows.net'
$env:SERVICEBUS_QUEUE = 'fpa-pipeline'
uvicorn main:app --host 0.0.0.0 --port 8000 --workers 1
```

- `.env.example` documents settings, but nothing loads `.env` automatically â€” set env vars yourself.
- Auth is `DefaultAzureCredential`; a credential with queue receive rights must be available.
- The app **refuses to start** while `pipeline.PIPELINE_IMPLEMENTED` is `False` (intentional guard so a placeholder never completes or churns real messages). Don't flip it without connecting a real pipeline.
- `requirements.txt` is intentionally unpinned sample requirements.

### Code structure

- [main.py](main.py): FastAPI `lifespan` opens a `PEEK_LOCK` queue receiver (`prefetch_count=0`) and starts `consume_messages` as a background task, one message at a time, with `AutoLockRenewer` (900s). Invalid payloads (not a JSON object with a nonempty string `interactionId`) are dead-lettered as `INVALID_MESSAGE`; pipeline exceptions are logged by exception **type only** (no bodies/PII) and the message is abandoned; success completes it. Shutdown sets a stop event and waits 30s before cancelling. `/ready` returns 503 if the consumer task has stopped (it does not restart it).
- [pipeline.py](pipeline.py): the integration point. `run_pipeline(interaction_id)` must return only after completion is durably recorded.

Values like 900s renewal, 30s shutdown budget, and immediate abandon are illustrative, not production defaults.

## Design docs and precedence

- [docs/FPA_Insurance_Pipeline_Implementation_Guide.md](docs/FPA_Insurance_Pipeline_Implementation_Guide.md) (~6.9k lines): sections 1â€“28 are the consolidated spec (reliability invariants Â§3, stage checkpoint/resume Â§4, Cosmos schema Â§5, state transitions Â§6, queue contract Â§8, worker settlement Â§9, ownership/fencing Â§10, retries Â§12â€“15, DLQ Â§16, outbox Â§17, verification Â§27). **Appendix B (line ~1494 onward) is a preserved raw conversation** with earlier alternatives that the main guide supersedes â€” don't implement from it.
- [docs/REVISION_NOTES.md](docs/REVISION_NOTES.md) **takes precedence** over conflicting guidance in the guide. Key points: permanent business failures persist terminal `FAILED` then complete the message (DLQ is for malformed commands/technical incidents); delayed retry is an atomic `RETRYING` + due time + outbox event + ownership release, then complete â€” abandon is not a backoff scheduler; each logical transition is one conditional atomic document update; the worker is a separate FastAPI app, one process per pod.
- The guide's evidence boundary: no real FPA repo, Azure account, or deployment was inspected. Field names, APIs, and config values are proposed contracts, and Cosmos guidance assumes RU-based Cosmos DB for MongoDB.

## Core invariants to preserve when writing pipeline code

- Cosmos is the truth; a queue message is only a command. Persist results and final state **before** completing the message. Delivery is at-least-once.
- Five sequential stages in fixed order: `transcription`, `summary`, `profiling`, `compliance`, `behaviour`. The runner checks each stage's versioned checkpoint and skips valid completed ones; never wrap the whole pipeline in a retry decorator â€” retry at the stage level.
- Only the current owner (claim/attempt token, fencing) may commit a stage result.
- Persist external job IDs (e.g. async transcription) and resume by querying them rather than resubmitting paid work.
- `sample/` implements ownership/fencing, checkpoints, the outbox, recovery scans and failure classification. Not implemented anywhere yet:
  - versioned checkpoints (input hash, prompt version)
  - artifact persistence (Blob)
  - lock-loss cancellation of an in-flight stage
  - DLQ reconciliation tooling
  - persisted external job IDs
