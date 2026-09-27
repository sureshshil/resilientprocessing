# FPA resilient pipeline sample

A runnable, medium-sized version of the FPA post-recording pipeline. It shows how to make
the five analysis stages (transcription, summary, profiling, compliance, behaviour)
survive crashes, duplicates, LLM errors and database outages. The stages are fakes. The
resilience machinery is real.

Design: [../docs/superpowers/specs/2026-09-27-resilient-pipeline-sample-design.md](../docs/superpowers/specs/2026-09-27-resilient-pipeline-sample-design.md)

## What it demonstrates

| Idea | Where |
|---|---|
| Mongo is the source of truth; the queue message is only `{"jobId": ...}` | `app/processing.py` |
| Checkpoints: completed stages are skipped on re-run | `app/runner.py` |
| Message completed only after the result is saved | `app/processing.py`, `app/worker.py` |
| Lease + fencing ("name tag"): only the current owner can write | `app/repository.py` (`claim`, `_fenced`, `heartbeat`) |
| Outbox: `sendMe` flag saved with the job, API sends at once, publisher is the backup | `app/api.py`, `app/publisher.py` |
| In-stage retry (3 tries), then delayed retry stored in Mongo (`RETRYING` + `sendAfter`) | `app/runner.py`, `app/processing.py` |
| Transient vs permanent errors, with a budget of 5 attempts | `app/errors.py` |
| Cosmos throttling (16500) and network errors retried in one place | `app/repository.py` (`_call`) |
| Sweeper re-queues stuck jobs | `app/sweeper.py` |

Job id is `sessionId:recordingId`. Consent recordings are accepted but skipped.

## Setup

Needs Python 3.11+ and Docker Desktop.

```powershell
cd sample
python -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements.txt
Copy-Item .env.example .env
```

The Service Bus emulator needs SQL Server. Read both licence terms, then set
`ACCEPT_EULA=Y` in `.env`:

- Service Bus emulator: https://github.com/Azure/azure-service-bus-emulator-installer/blob/main/EMULATOR_EULA.txt
- SQL Server Linux: https://go.microsoft.com/fwlink/?LinkId=746388

```powershell
docker compose up -d          # mongo + mssql + servicebus (the emulator takes ~30s to be ready)
```

## Run

Two terminals, both in `sample/`:

```powershell
.\.venv\Scripts\uvicorn app.api:app --port 8000        # API + UI + publisher + sweeper
.\.venv\Scripts\uvicorn app.worker:app --port 8001     # worker
```

Open http://localhost:8000, submit a recording and watch the stages turn green.

## Tests

Tests only need MongoDB (`docker compose up -d mongo`); the queue is faked.

```powershell
.\.venv\Scripts\python -m pytest                                   # all
.\.venv\Scripts\python -m pytest tests/test_processing.py -k retry # one area
```

## Things to try

| Scenario | How | What you should see |
|---|---|---|
| Rate limit | Fault `summary`, `llm_429`, times 2 | Summary shows `(3)` tries and the job completes in the same attempt |
| Delayed retry | `llm_500`, times 4 | `RETRYING`, "Next send" ~30s later, then it resumes at that stage and completes |
| Give up | `llm_500`, times 0 | Retries after 30s, 60s, 120s, 240s, then `FAILED: gave up after 5 attempts` |
| Permanent error | `llm_content_blocked` or `bad_audio` | `FAILED` immediately, no retry |
| Worker crash | Fault `profiling`, `slow`, times 1; press Ctrl+C in the worker during profiling; start it again | The job stays `RUNNING`; after the 60s lock and 60s lease expire it resumes at profiling, and earlier stages are not rerun |
| Two workers | Start another worker on `--port 8002` and submit several jobs | Jobs are shared out; the owner column shows which worker has each |
| Mongo outage | `docker compose stop mongo` for ~60s, then `docker compose start mongo` | The worker abandons and retries messages; jobs continue once Mongo is back |
| Cosmos throttling | Start the worker with `$env:FAULT_COSMOS_429_RATE="0.3"` | Warnings for 16500 retries in the log; jobs still complete |
| Queue down at submit | `docker compose stop servicebus`, submit, then start it again | Submit still returns 201 with `sendMe` set; the publisher sends it once the queue is back |
| Sweeper | Stop the worker, submit, wait 10+ minutes (or lower `STUCK_UNSENT_SECONDS`), start the worker | The sweeper re-flags the job and it completes |

## Plugging in real FPA code

Replace `run_stage` in `app/stages.py`. Return the stage output, or raise. `openai` rate limit,
timeout, connection and 5xx errors are retried. `openai.BadRequestError` and
`app.errors.PermanentError` fail the job. Any other exception is treated as transient.

## Moving to Azure

- **Cosmos DB for MongoDB:** set `MONGO_URL` to the account's connection string.
- **Service Bus:**
  - clear `SERVICEBUS_CONNECTION_STRING`
  - set `SERVICEBUS_NAMESPACE=<name>.servicebus.windows.net`
  - give your identity the *Azure Service Bus Data Sender/Receiver* roles

  `DefaultAzureCredential` is then used.
