# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

A design guide plus a small **teaching example**, not a working FPA application. It pairs the FPA insurance recording/analysis pipeline design (migration from FastAPI `BackgroundTasks` to Azure Service Bus + a separate AKS worker, with Cosmos DB for MongoDB as the source of truth) with a minimal FastAPI Service Bus consumer. There is no test suite, linter config, or build step.

## Commands

Python 3.11+. Windows/PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
$env:SERVICEBUS_NAMESPACE = 'your-namespace.servicebus.windows.net'
$env:SERVICEBUS_QUEUE = 'fpa-pipeline'
uvicorn main:app --host 0.0.0.0 --port 8000 --workers 1
```

- `.env.example` documents settings, but nothing loads `.env` automatically — set env vars yourself.
- Auth is `DefaultAzureCredential`; a credential with queue receive rights must be available.
- The app **refuses to start** while `pipeline.PIPELINE_IMPLEMENTED` is `False` (intentional guard so a placeholder never completes or churns real messages). Don't flip it without connecting a real pipeline.
- `requirements.txt` is intentionally unpinned sample requirements.

## Code structure

- [main.py](main.py): FastAPI `lifespan` opens a `PEEK_LOCK` queue receiver (`prefetch_count=0`) and starts `consume_messages` as a background task, one message at a time, with `AutoLockRenewer` (900s). Invalid payloads (not a JSON object with a nonempty string `interactionId`) are dead-lettered as `INVALID_MESSAGE`; pipeline exceptions are logged by exception **type only** (no bodies/PII) and the message is abandoned; success completes it. Shutdown sets a stop event and waits 30s before cancelling. `/ready` returns 503 if the consumer task has stopped (it does not restart it).
- [pipeline.py](pipeline.py): the integration point. `run_pipeline(interaction_id)` must return only after completion is durably recorded.

Values like 900s renewal, 30s shutdown budget, and immediate abandon are illustrative, not production defaults.

## Design docs and precedence

- [docs/FPA_Insurance_Pipeline_Implementation_Guide.md](docs/FPA_Insurance_Pipeline_Implementation_Guide.md) (~6.9k lines): sections 1–28 are the consolidated spec (reliability invariants §3, stage checkpoint/resume §4, Cosmos schema §5, state transitions §6, queue contract §8, worker settlement §9, ownership/fencing §10, retries §12–15, DLQ §16, outbox §17, verification §27). **Appendix B (line ~1494 onward) is a preserved raw conversation** with earlier alternatives that the main guide supersedes — don't implement from it.
- [docs/REVISION_NOTES.md](docs/REVISION_NOTES.md) **takes precedence** over conflicting guidance in the guide. Key points: permanent business failures persist terminal `FAILED` then complete the message (DLQ is for malformed commands/technical incidents); delayed retry is an atomic `RETRYING` + due time + outbox event + ownership release, then complete — abandon is not a backoff scheduler; each logical transition is one conditional atomic document update; the worker is a separate FastAPI app, one process per pod.
- The guide's evidence boundary: no real FPA repo, Azure account, or deployment was inspected. Field names, APIs, and config values are proposed contracts, and Cosmos guidance assumes RU-based Cosmos DB for MongoDB.

## Core invariants to preserve when writing pipeline code

- Cosmos is the truth; a queue message is only a command. Persist results and final state **before** completing the message. Delivery is at-least-once.
- Five sequential stages in fixed order: `transcription`, `summary`, `profiling`, `compliance`, `behaviour`. The runner checks each stage's versioned checkpoint and skips valid completed ones; never wrap the whole pipeline in a retry decorator — retry at the stage level.
- Only the current owner (claim/attempt token, fencing) may commit a stage result.
- Persist external job IDs (e.g. async transcription) and resume by querying them rather than resubmitting paid work.
- Not implemented in the sample (and required for production): Cosmos ownership/fencing, checkpoints, artifact persistence, lock-loss cancellation, outbox publisher, DLQ reconciliation, recovery scans, failure classification.
