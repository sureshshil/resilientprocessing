# FPA FastAPI worker example

This repository pairs the detailed FPA implementation guide with the educational
FastAPI startup/queue-consumer example discussed alongside it. It is not a complete
FPA application or production-ready worker.

## Files

- `main.py`: FastAPI lifespan starts a Service Bus listener; one job at a time.
- `pipeline.py`: deliberately unimplemented integration point for the real pipeline.
- `docs/FPA_Insurance_Pipeline_Implementation_Guide.md`: full original guide and source conversation appendix.
- `docs/REVISION_NOTES.md`: later design clarifications that take precedence over conflicting original examples.

The worker refuses to start until you implement the pipeline and explicitly set
`PIPELINE_IMPLEMENTED = True`. This prevents an empty placeholder from acknowledging
real jobs or repeatedly abandoning them.

## Local setup

Use Python 3.11 or later. From this directory:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
$env:SERVICEBUS_NAMESPACE = 'your-namespace.servicebus.windows.net'
$env:SERVICEBUS_QUEUE = 'fpa-pipeline'
uvicorn main:app --host 0.0.0.0 --port 8000 --workers 1
```

Configure an identity with queue receive permissions. `DefaultAzureCredential`
must have a suitable credential available; AKS workload identity requires Azure
and Kubernetes configuration outside this example. `.env.example` documents the
settings; this code does not load `.env` files automatically.

Dependencies are intentionally unpinned sample requirements. Resolve and lock a
tested set of versions before deployment.

## How it works

FastAPI starts -> listener receives a command -> real pipeline processes the
interaction and persists completion -> listener completes the message.

Message shape: `{"interactionId": "INT-123"}`. The production envelope needs
tenant/routing identity, generation, dispatch identity, and schema validation as
described in the guide.

## Important limits

The sample uses immediate abandon on pipeline exceptions to show the basic flow.
Production must classify failures and implement the durable outbox retry handoff.
It does not implement Cosmos ownership, fencing, checkpoints, artifact persistence,
lock-loss cancellation, an outbox publisher, DLQ reconciliation, or recovery scans.
The example renewal and shutdown budgets (900 and 30 seconds) are not production defaults.

Readiness detects a stopped listener but does not restart it. Production needs
consumer supervision or process failure plus appropriate Kubernetes probes.
Do not treat a temporary downstream outage as a reason for a fleet-wide restart loop.

Validation performed for this publication is documented separately from deployment:
no Azure queue was consumed, and no live pipeline was run.

## Architecture

UI -> FastAPI API -> Cosmos workflow/outbox -> publisher -> Service Bus
-> separate FastAPI worker -> sequential stages -> Cosmos checkpoints -> UI polling.

The guide contains the original conversation appendix. Review that material before
choosing public repository visibility.
