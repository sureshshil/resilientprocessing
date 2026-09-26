# Design clarifications after the original guide

The full guide is preserved as originally produced. These subsequent reviewed
decisions take precedence over conflicting examples; this is not a claim that
the original guide has already been comprehensively rewritten.

1. Expected permanent business/input failure: persist terminal FAILED, then complete
   the command. DLQ is for malformed/unsupported commands and technical incidents.
   Do not complete if the terminal write is unresolved. A Cosmos outage alone is
   not evidence that a referenced workflow does not exist. Redelivered business-failed
   runs complete without rerunning analysis.
2. Primary delayed retry: atomically persist RETRYING, due time, durable recovery
   budget, a new outbox event, and ownership release. Complete the current message
   only after this handoff. Publisher sends when due. Abandon is immediate recovery,
   not an exponential scheduler. Fresh messages do not reset workflow retry budgets.
3. Outbox discovery requires an exact query/index/pagination/polling design validated
   against the real shard key. Measure RU, fan-out, backlog and oldest pending age.
4. Long external jobs yield through durable continuations. Persist provider job ID
   and resume by querying it. Ordinary status checks are not failed attempts. Address
   the crash between provider acceptance and ID persistence with provider idempotency
   or client-reference lookup when supported; otherwise duplicate submission remains possible.
5. Each logical transition must be one conditional atomic document update. Completing
   summary and separately claiming profiling is valid; no requirement to combine them.
   Keep unique claim/attempt tokens and revision; derive five-stage progress on reads.
6. Before production, explicitly decide identity/authentication per service, least
   privilege, tenant authorization, secrets, network/TLS, PII-safe logging, replay
   audit, artifact/metadata retention and deletion, backups, residency and provider
   data handling. Organization-specific policy remains unconfirmed.
7. Use a design gate for correctness-critical contracts and a production-readiness
   gate for measured settings such as concurrency, pooling, quotas and KEDA bounds.
8. The worker may be a separate FastAPI application. Its lifespan starts a supervised
   queue consumer, with HTTP used for health/readiness. Start with one process per pod;
   scale pods with explicit concurrency. Preserve all original failure and recovery tests.

No additional per-stage queues, Redis, parallel stages or orchestration service is
required for these changes. The sample is a teaching example, not their implementation.
