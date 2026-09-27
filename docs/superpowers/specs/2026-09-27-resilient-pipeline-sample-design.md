# Resilient pipeline sample — design

**Date:** 2026-09-27
**Status:** Draft for review
**Location of code:** `sample/` in this repo (existing `main.py` / `pipeline.py` stay untouched)

## 1. Goal

A medium-sized, runnable sample that shows how the FPA post-recording pipeline
can be made resilient. It is a learning and reference project, not production code.

It must demonstrate:

1. Checkpointed stages: completed stages are skipped on re-run.
2. The queue message is completed only after results are saved.
3. Ownership lease + fencing (the "name tag"), so a stale worker cannot overwrite a newer one.
4. Outbox (the `sendMe` flag), so a saved job always reaches the queue.
5. Delayed retry stored in Mongo, not done by the queue.
6. Transient vs permanent errors, with a bounded retry budget.
7. A recovery sweeper for stuck jobs.
8. A tiny UI to submit jobs, watch stages, and inject failures.

Everything runs locally with Docker. Configuration is via environment variables so
the same code can later point at Azure Cosmos DB for MongoDB and Azure Service Bus.

## 2. Out of scope

Auth, real LLM / Speech calls, Blob storage for stage outputs, AKS/KEDA, metrics
dashboards, multiple API instances.

## 3. Architecture

```
 Browser (tiny UI)
     │  POST /jobs            GET /jobs/{id}  (polls every 2s)
     ▼
┌──────────────────────────┐  one write: job + sendMe=true   ┌──────────┐
│ API app                  │ ──────────────────────────────► │ MongoDB  │  source of truth
│  - tries to send at once │                                  └──────────┘
│  - publisher loop (2s)   │ ◄── finds sendMe=true ───────────┘   ▲   ▲
│  - sweeper loop (60s)    │ ── flips sendMe=true on stuck jobs ───┘   │
└──────────────────────────┘                                          │
        │ send {"jobId"}                                               │
        ▼                                                              │
 Service Bus queue "fpa-pipeline"                                      │
        │                                                              │
        ▼                                                              │
┌──────────────────────────┐  claim, heartbeat, fenced stage writes   │
│ Worker app (1..n)        │ ─────────────────────────────────────────┘
│  5 stages, in order      │
└──────────────────────────┘
```

Two processes:

- **API app** (`uvicorn app.api:app --port 8000`): HTTP API, UI, publisher loop, sweeper loop.
- **Worker app** (`uvicorn app.worker:app --port 8001`): queue consumer plus `/ready`.
  More workers can be started on other ports to demonstrate the lease.

Only the publisher and the API's immediate send put messages on the queue.
The sweeper never sends; it only sets `sendMe = true`.

## 4. Job identity and document

One document per **audio** recording in collection `jobs`.

- `_id` = `"{sessionId}:{recordingId}"` (recordingId is unique only within a session;
  sessionId is assumed globally unique).
- `recordingType = "consent"` is accepted by the API but creates no job.

```json
{
  "_id": "S-3321:rec-8812",
  "key": {
    "opportunityId": "OPP-001", "agentId": "AG-17", "clientId": "CL-450",
    "eventId": "EV-9001", "sessionId": "S-3321", "recordingId": "rec-8812",
    "recordingType": "audio"
  },
  "status": "PENDING | RUNNING | RETRYING | COMPLETED | FAILED",
  "stages": {
    "transcription": { "status": "PENDING | RUNNING | COMPLETED | FAILED",
                       "tries": 0, "output": null, "finishedAt": null },
    "summary":     { "...": "same shape" },
    "profiling":   { "...": "same shape" },
    "compliance":  { "...": "same shape" },
    "behaviour":   { "...": "same shape" }
  },
  "owner": { "token": "uuid4", "workerId": "worker-8001", "expiresAt": "datetime" },
  "sendMe": true,
  "sendAfter": "datetime",
  "attempts": 0,
  "error": null,
  "faults": { "summary": { "type": "llm_429", "times": 2 } },
  "createdAt": "datetime",
  "updatedAt": "datetime"
}
```

- `owner` is `null` when nobody holds the job.
- `attempts` counts delayed-retry handoffs (whole-job retries), not in-stage tries.
- `stages.*.tries` counts every try of that stage across all attempts; fault injection uses it.
- `error` is a short safe string (exception class + our message), never raw provider bodies.
- `faults` is optional and only used by the demo (see section 9).

Indexes (created at API startup; also required on Cosmos RU, which indexes only `_id` by default):
`{sendMe: 1, sendAfter: 1}`, `{status: 1, "owner.expiresAt": 1}`, `{status: 1, updatedAt: 1}`, `{createdAt: -1}`.

All state changes are single-document updates, so they are atomic in both MongoDB and Cosmos.

## 5. API

| Method | Path | Behaviour |
|---|---|---|
| `POST` | `/jobs` | Body: the 7 key fields plus optional `faults`. Consent → `200 {"status": "SKIPPED"}`. Audio → insert job with `status=PENDING`, `sendMe=true`, `sendAfter=now+5s`, returns `201`. Duplicate `_id` → return the existing job with `200`. |
| `GET` | `/jobs/{id}` | Returns id, key, status, per-stage status/tries/finishedAt, attempts, error, sendAfter. `404` if missing. |
| `GET` | `/jobs` | 20 most recent jobs (for the UI). |
| `GET` | `/` | The UI page. |

**Immediate send (fast path):** after a successful insert, the API sends
`{"jobId": id}` to the queue, then sets `sendMe=false` (only if `sendAfter` is unchanged).
If the send fails, the API still returns `201`; the publisher will send it.
The 5-second `sendAfter` stops the publisher from racing the API's own send.

## 6. Publisher (in API app, every 2s)

1. Find jobs with `sendMe = true` and `sendAfter <= now` (limit 50).
2. For each: send `{"jobId": id}`, then set `sendMe=false` with a filter that also matches the
   `sendAfter` value it read (so a newer retry request is not erased).
3. Errors are logged and the loop continues.

A duplicate send (send succeeded, flag update failed) is harmless: the worker handles duplicates.

## 7. Worker

Receives with peek-lock, `prefetch_count=0`, one message at a time, and `AutoLockRenewer`.

Per message:

1. Parse `{"jobId": str}`. Invalid → dead-letter (`INVALID_MESSAGE`).
2. Load the job. Missing → dead-letter (`JOB_NOT_FOUND`).
   `COMPLETED` / `FAILED` → complete the message (duplicate).
   `RETRYING` with `sendAfter > now` → complete the message (early duplicate; the publisher will send again when due).
3. **Claim** with one conditional update:
   filter `status in [PENDING, RETRYING, RUNNING]` and (`owner = null` or `owner.expiresAt < now`);
   set `status=RUNNING`, `owner={new token, workerId, expiresAt=now+LEASE}`, `sendMe=false`.
   Not matched → someone else owns it → complete the message and stop.
4. Start the **heartbeat** task: every `HEARTBEAT` seconds, extend `owner.expiresAt`
   (filtered by `owner.token`). If it matches nothing, mark the lease as lost.
5. Run the stages (section 8).
6. Stop the heartbeat, then settle the message according to the outcome:

| Outcome | Mongo | Message |
|---|---|---|
| All stages done | `status=COMPLETED`, `owner=null` (fenced) | complete |
| Permanent error | `status=FAILED`, stage `FAILED`, `error`, `owner=null` (fenced) | complete |
| Transient error, attempts left | `status=RETRYING`, `attempts+=1`, `sendMe=true`, `sendAfter=now+backoff`, `owner=null`, `error` (fenced, one write) | complete |
| Transient error, budget used up | `status=FAILED`, `error="gave up after N attempts: ..."` (fenced) | complete |
| Lease lost (fenced write matched nothing) | nothing | complete, ignoring lock-lost errors |
| Mongo unavailable (repository still failing after its own retries) | nothing possible | wait 10s, then abandon |

Abandon after a Mongo outage leads to redelivery. If the queue's max delivery count (10) is hit,
the message goes to the DLQ, and the sweeper rescues the job once Mongo is back.
Also, if a redelivered message arrives while the crashed run's lease is still valid, the claim is
refused and that message is completed. This is intentional: a valid lease may belong to a live
worker, and the sweeper re-queues the job once the lease has been expired for 2 minutes.

## 8. Runner (stage execution)

Stage order: `transcription, summary, profiling, compliance, behaviour`.

For each stage:

1. If `COMPLETED` → skip.
2. If the lease is known to be lost → stop with `LeaseLost`.
3. Fenced write: stage `status=RUNNING`, `tries += 1`.
4. Call the stage function. **In-stage retry** on transient errors: up to `STAGE_TRIES` (3) tries with
   exponential backoff and jitter (1s, 2s, ...). Each try does step 3 first.
5. On success, fenced write: stage `status=COMPLETED`, `output`, `finishedAt`.
6. Any fenced write that matches nothing → `LeaseLost`.

The runner does not know about Service Bus. It takes a job id plus a lease token and returns
an outcome (`completed`, `failed`, `retry`, `lease_lost`), which the worker turns into settlement.

**Error classification** (`errors.py`):

- **Transient:** `openai.RateLimitError`, `APITimeoutError`, `APIConnectionError`, `InternalServerError`;
  pymongo `AutoReconnect`, `NetworkTimeout`, `ServerSelectionTimeoutError`, `OperationFailure` code 16500;
  our `TransientError`; any unknown exception (bounded by the attempt budget).
- **Permanent:** `openai.BadRequestError` (e.g. content filter), our `PermanentError`.

**Repository retry:** every Mongo call retries `OperationFailure` 16500 and network errors
up to 3 times with short backoff before raising `RepositoryUnavailable`.

## 9. Fault injection

**Stage faults** are set per job at submit time (`faults` field, chosen in the UI):
`{"<stage>": {"type": "<fault>", "times": N}}`. The fault fires while `stages.<stage>.tries <= N`,
so it survives restarts and is deterministic. `times = 0` means "always".

| Fault | Raises | Expected result |
|---|---|---|
| `llm_429` | `openai.RateLimitError` | in-stage retry, then success |
| `llm_timeout` | `openai.APITimeoutError` | in-stage retry, then success |
| `llm_500` | `openai.InternalServerError` | with enough `times`: `RETRYING` → later attempt → success, or `FAILED` after the budget |
| `llm_content_blocked` | `openai.BadRequestError` | `FAILED` immediately |
| `bad_audio` | `PermanentError` | `FAILED` immediately |
| `slow` | sleeps 60s | time to Ctrl+C the worker or start a second one |

**Cosmos faults** for manual play are worker env vars:
`FAULT_COSMOS_429_RATE` (0.0–1.0 chance of raising `OperationFailure` 16500 per repository call).
A real outage is simulated with `docker compose stop mongo`.

The fake stages sleep `STAGE_SECONDS` (default 3) and return a short dummy string.

## 10. Sweeper (in API app, every 60s)

Sets `sendMe=true`, `sendAfter=now` on:

- `status = RUNNING` and `owner.expiresAt < now - 2 min` (worker died and the message is gone).
- `status in [PENDING, RETRYING]`, `sendMe = false`, `updatedAt < now - 10 min` (message lost, e.g. DLQ).

It never sends messages itself and never changes `status`.

## 11. Timings (env-configurable, demo-friendly defaults)

| Setting | Default |
|---|---|
| `LEASE_SECONDS` | 60 |
| `HEARTBEAT_SECONDS` | 20 |
| `STAGE_TRIES` | 3 |
| `MAX_ATTEMPTS` | 5 |
| `RETRY_BASE_SECONDS` / cap | 30 / 300 (doubling) |
| `PUBLISH_INTERVAL_SECONDS` | 2 |
| `SWEEP_INTERVAL_SECONDS` | 60 |
| `STAGE_SECONDS` | 3 |

Production values would be longer; these keep the demo watchable.

## 12. Local infrastructure

`sample/docker-compose.yml`:

- `mongo` (MongoDB 7) on port 27017.
- Azure Service Bus emulator plus the SQL container it depends on, with
  `servicebus-config.json` declaring queue `fpa-pipeline` (lock duration 60s, max delivery count 10).

Connection:

- `MONGO_URL` (default `mongodb://localhost:27017`), `MONGO_DB` (default `fpa_sample`).
- `SERVICEBUS_CONNECTION_STRING` (the emulator's development connection string) and `SERVICEBUS_QUEUE`.
  If no connection string is given, `SERVICEBUS_NAMESPACE` plus `DefaultAzureCredential` is used, for Azure later.

## 13. UI

A single static `index.html` (no build step):

- A form with the 7 key fields (prefilled with random ids) plus a fault picker (stage, fault, times).
- A list of recent jobs. Each row shows 5 stage boxes (grey pending, blue running, green done,
  red failed), the job status, attempts, next retry time, and the error.
- Polls `GET /jobs` every 2s.

## 14. Code layout

```
sample/
  docker-compose.yml
  servicebus-config.json
  .env.example
  requirements.txt
  README.md                 # setup, run, and manual failure scenarios
  app/
    config.py               # settings from env
    models.py               # key model, statuses, stage names, new-job document
    errors.py               # TransientError, PermanentError, LeaseLost, RepositoryUnavailable, classify()
    repository.py           # all Mongo calls + 16500/network retry + fault hook
    queue.py                # send / receive wrapper (emulator or Azure)
    faults.py               # stage fault injection
    stages.py               # 5 fake stages
    runner.py               # stage loop, checkpoints, in-stage retry, outcome
    publisher.py            # outbox loop
    sweeper.py              # stuck-job loop
    api.py                  # API app, lifespan starts publisher + sweeper
    worker.py               # worker app, lifespan starts consumer; heartbeat; settlement
    static/index.html
  tests/
    conftest.py             # per-test Mongo database, FakeQueue
    test_api.py
    test_runner.py
    test_worker.py
    test_publisher.py
    test_sweeper.py
```

## 15. Testing

Automated tests use pytest against the Docker MongoDB (a fresh database per test) and an
in-memory `FakeQueue`, so they don't need the Service Bus emulator. Run them with
`pytest` from `sample/`, or a single test with `pytest tests/test_runner.py -k resume`.

Cases:

1. Happy path: all 5 stages are `COMPLETED`, the message is completed, `owner` is cleared.
2. Resume: 2 stages already done → only 3 run.
3. Stale worker: a second claim changes the token, and the first worker's stage write is rejected (`LeaseLost`).
4. Claim refused while another worker's lease is valid; allowed after it expires.
5. `llm_429` twice → in-stage retry → success, `tries = 3`.
6. `llm_500` beyond the stage tries → `RETRYING`, `sendMe = true`, `sendAfter` in the future, `attempts = 1`.
7. `llm_content_blocked` → `FAILED` without retry.
8. Budget used up → `FAILED` with "gave up".
9. `RETRYING` message arriving before `sendAfter` → completed, nothing runs.
10. API: consent → `SKIPPED` with no document; duplicate submit → same job; immediate send puts one message on the queue.
11. API send failure → job stays `sendMe = true`; the publisher sends it once due.
12. Publisher does not clear `sendMe` if `sendAfter` changed after it read the job.
13. Sweeper flags an expired `RUNNING` job and an old unsent `PENDING` job, and ignores healthy ones.
14. Repository retries 16500 then succeeds; persistent failure raises `RepositoryUnavailable`.

Manual scenarios in the README:

- Ctrl+C the worker during a `slow` stage.
- Two workers.
- `docker compose stop mongo` for 60s.
- Each UI fault.
- Stopping the worker for 10+ minutes to see the sweeper.
