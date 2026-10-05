# FPA worker concurrency and capacity planning

Discussion summary — 2026-10-03.

## Decision and evidence boundary

Start with **two active regions, two worker replicas per region**, and **2 CPU cores / 2 GiB memory per pod**. Prefer async workers, with one worker owning an entire interaction and executing its stages sequentially.

The 100-simultaneous-job scenario is a **performance/stress test**, not the expected normal workload. The discussion ended with the expectation that normal concurrency is below about 25 interactions. This document interprets that figure as **fleet-wide concurrency**, not 25 per pod; confirm it against observed traffic.

This configuration is a reasonable starting hypothesis, **not verified production capacity**. No actual application clients, Azure resource quota allocations, Speech timings, or pod memory measurements were inspected. The simulator tests establish model behavior, not real Azure performance.

## Initial configuration

| Setting | Per pod | Four-pod fleet |
|---|---:|---:|
| CPU allocation | 2 cores | 8 cores |
| Memory limit | 2 GiB | 8 GiB |
| Python processes | 1 | 4 |
| Maximum active interactions | 25 | 100 |
| Concurrent Speech operations | 4 | 16 |
| Concurrent LLM calls | 8 | 32 |
| Service Bus prefetch | 0 initially | — |
| HPA minimum | 2 pods per region | 4 pods |
| HPA maximum, if budget permits | 4 pods per region | 8 pods |

The operation limits are shared by all job workers in a pod. They are not additional interactions. Earlier simulator examples used 10 LLM slots per pod; **8 is the later proposed initial worker setting**. Change the simulator input to 8 when evaluating this configuration.

At roughly 25 simultaneous interactions across the fleet, the average is six or seven per pod, although distribution is not guaranteed to be perfectly even. Sixteen Speech slots still cause waiting if all 25 jobs begin transcription together. Staggered arrivals and interactions at different stages reduce that contention.

The 25-interaction cap is a target to validate. A lower admission cap may be appropriate if retained payloads, bandwidth or provider constraints require it.

## One worker owns one whole job

```text
Worker A owns interaction A:
  transcription -> summary -> profiling -> compliance -> behaviour

Worker B owns interaction B:
  transcription -> summary -> profiling -> compliance -> behaviour
```

Parallelism is **between interactions**. The five stages within each interaction remain sequential. A stage is an awaited function call; it need not be a separate independently scheduled task.

Two implementation patterns can provide this abstraction:

- **Fixed async worker pool:** 25 long-lived coroutine workers, each handling one complete interaction before requesting another.
- **Bounded per-message tasks:** a single receiver reserves an interaction slot before reception and creates one tracked task for the received message.

The fixed async pool expresses the user's preferred abstraction clearly. Either pattern must bound reception and use SDK-supported receiver access. Do not assume a shared Service Bus receiver supports 25 concurrent receive calls; receiver coordination is an implementation detail requiring verification.

## Async workers versus literal threads

| Perspective | Async worker | Literal thread |
|---|---|---|
| Job ownership | Coroutine owns the whole job | Thread executes the whole job |
| Waiting | Nonblocking I/O suspends the coroutine | Blocking I/O pauses the thread |
| Scheduling | Event loop cooperatively advances ready tasks | Operating system schedules threads |
| Clients | Async client methods | Synchronous client methods |
| Concurrency overhead | Usually lower for many waiting jobs | Thread resources remain allocated while waiting |
| Cancellation | Cooperative cancellation at suspension points | Running threads cannot generally be safely stopped forcibly |
| CPU-heavy Python | One event loop does not supply multicore parallelism | Standard CPython's GIL limits pure-Python thread parallelism |

**Recommendation:** async for the overall worker, because most work waits for Blob Storage, Speech, Cosmos and Azure OpenAI. Isolate genuinely blocking dependencies in a bounded thread pool when necessary. Substantial CPU-heavy audio work may need a separate process-based execution mechanism.

If the real pipeline is predominantly synchronous, a bounded thread-pool implementation remains a valid benchmark alternative. Switching syntax alone does not speed up external services.

## How the event loop works

```text
Job A runs -> awaits network I/O -> suspends
Job B runs -> awaits network I/O -> suspends
Job C runs -> awaits network I/O -> suspends
An operation becomes ready -> its job can resume
```

A single event-loop thread executes one coroutine's Python code at a time, while many external requests can remain in flight. Operating-system I/O facilities let the networking implementation detect readiness/completion without one Python polling thread per request.

`await` is a possible suspension point, not a guaranteed task switch. A ready operation may return immediately. Declaring a function `async def` does not make synchronous calls nonblocking. Blocking network calls, `time.sleep`, or lengthy computation inside the event loop prevent other tasks from advancing.

Suspension preserves local variables and the job's execution state. It does not discard ownership or start the next stage early. Cancellation of local work also does not guarantee that an external provider stops a submitted operation.

## Admission control and provider limits

The configured admission limit represents capacity established through testing; it does not automatically measure free CPU or memory.

```text
Reserve an interaction slot
  -> receive a Service Bus message
  -> process the complete job
  -> durably record outcome and settle message
  -> release the slot
```

When capacity is full, stop receiving additional messages. Leave the backlog in Service Bus rather than creating unlimited waiting tasks or collecting locked messages locally.

`asyncio.BoundedSemaphore` is suitable for slot accounting and detects over-release. `TaskGroup` can manage task lifetimes but does not impose a concurrency limit; unhandled task failure cancels sibling tasks. Expected message failures must be handled within their owning task, while cancellation and fatal consumer failures require explicit policies.

A bounded `asyncio.Queue` plus fixed workers is another option, but queue size limits waiting items, not active jobs. Twenty-five workers plus a queue of 25 can hold 50 messages. Likewise, `ThreadPoolExecutor(max_workers=25)` bounds executing threads, not submitted work.

Use separate Speech and LLM concurrency gates inside job processing. Provider request/token rate limits are separate from concurrency and must cover all pods sharing the same deployment/resource.

## Client checks still required

| Service | Blocking interface | Async interface |
|---|---|---|
| Azure Blob Storage | `azure.storage.blob.BlobClient` | `azure.storage.blob.aio.BlobClient` |
| Azure OpenAI Python SDK | `openai.AzureOpenAI` | `openai.AsyncAzureOpenAI` |
| Cosmos DB for NoSQL | `azure.cosmos.CosmosClient` | `azure.cosmos.aio.CosmosClient` |

The repository design assumes **Cosmos DB for MongoDB**. That requires a compatible MongoDB driver rather than the NoSQL `CosmosClient`; do not silently change the database API. Actual imports and Speech invocation were not provided, so their blocking behavior remains unconfirmed.

Streaming an audio upload does not identify the Speech API, and does not necessarily make the upload nonblocking.

## Memory and streaming

Async reduces scheduling overhead but does not remove per-job memory. Awaiting coroutines retain their variables, including transcripts, prompt bodies and buffers.

```text
Pod memory = process and clients
           + retained state for admitted interactions
           + audio-stream buffers
           + request/response buffers and temporary copies
```

Twenty-five jobs retaining 40 MiB each would use 1,000 MiB before other allocations. This is an illustration, not a measurement.

Acquire a Speech slot **before opening the audio download**. Keep combined download/upload buffers bounded and propagate backpressure: if uploading slows, downloading must slow too. Avoid building a complete audio body from chunks or keeping unnecessary stage payload copies.

Tentatively budget 1–4 MiB of application buffering per active stream, while measuring SDK internal buffering separately. Aim for at least 25% unused memory under representative peak load: roughly 1.5 GiB used in a 2-GiB pod. This is a headroom target, not a guarantee against OOM.

## HTTP, Istio and HPA

The worker is a FastAPI application with a background Service Bus consumer. Its HTTP `/ready` endpoint reports consumer liveness; it does not admit jobs or expose free interaction capacity.

```text
HTTP: health probe -> /ready
Jobs: Service Bus -> background consumer -> pipeline
```

Istio HTTP routing does not assign these Service Bus jobs to worker pods. A proxy may carry outbound connections, but application reception controls job admission. Awaiting a provider does not make an occupied job slot free.

For HTTP workloads, least-request routing counts outstanding requests; an HTTP request awaiting an LLM remains outstanding. Background work after an HTTP response is no longer represented by that request count.

HPA should primarily follow job demand/backlog, not CPU alone: I/O-heavy workers can have low CPU and full interaction slots. Ideally observe queued plus active work, oldest queue age and occupied slots. If both regions use a shared queue, partition or coordinate the scaling signal to avoid both controllers sizing for the full global backlog independently.

HPA needs startup time and available cluster resources. It cannot increase provider quotas. During scale-down, stop new admissions and drain active work according to the shutdown budget. Include mesh sidecar memory in pod resource planning if one is present.

## Throughput versus advisor latency

**100 admitted interactions is not 100 requests advancing without resource waits.** With the initial configuration, Speech capacity is only 16 concurrent operations.

An illustrative synchronized burst of 100 one-chunk interactions needs roughly seven Speech waves. If Speech-slot occupancy is one minute and summary plus profiling takes 30 seconds, the first profiles may appear in about 1.5–2 minutes, while the last profiles may take around eight minutes. Two-minute Speech occupancy gives approximately 15 minutes for the last profiles; four-minute occupancy gives approximately 29 minutes. These are rough examples, not measured predictions.

Four minutes of audio is not necessarily four minutes of processing. Measure the elapsed time covering download/upload and transcription. Multiple chunks increase cumulative Speech-slot demand.

The UI may show a profile after `profiling`, or wait until all five stages finish. That product behavior must be confirmed because it changes advisor-visible latency.

The temporary discussion goal of **single-advisor latency for all 100 simultaneous advisors** is much stronger. It would require approximately 100 Speech and LLM slots fleet-wide at synchronized stages, plus matching bandwidth, CPU/memory and provider capacity. Six pods (three per region) at 17 slots each provide 102 configured interaction slots, but do not prove equivalent latency.

The final direction is to keep the initial four-pod configuration and use 100 simultaneous jobs as a stress test. Define separate latency expectations for normal traffic and stress traffic; increased stress-test latency is acceptable unless the product requirement says otherwise.

## Published Azure quotas checked during the discussion

These values were checked against Microsoft documentation on 2026-10-03. They are **published references, not the account's verified allocations**, and can change.

### Speech

- Standard S0 real-time speech-to-text: 100 concurrent requests per resource by default; adjustable. Real-time speech translation shares the applicable allowance.
- Free F0 real-time: one concurrent request.
- Fast and batch transcription: shared published rate limit of 600 requests/minute per Speech resource. Do not apply the real-time concurrency figure to Fast Transcription.

A request-rate allowance does not guarantee 100 simultaneous operations have single-job latency. Determine the exact transcription endpoint before selecting a quota model.

Source: [Speech quotas and limits](https://learn.microsoft.com/en-us/azure/ai-services/speech-service/speech-services-quotas-and-limits).

### Azure OpenAI

There is no universal default. Quota varies by model, deployment type, region and subscription tier; the actual deployment allocation may be smaller than available subscription quota.

The checked Tier 1 reference included GPT-4.1 Global Standard at 1,000 RPM / 1,000,000 TPM; GPT-4.1 Data Zone Standard at 300 RPM / 300,000 TPM; and GPT-4.1-mini Global Standard at 5,000 RPM / 5,000,000 TPM. These are examples, not the application's selected model or verified limits.

Short-window enforcement can throttle synchronized bursts even below minute totals. Rate-limit token estimates can include configured output allowances and differ from billed tokens. For illustration, 100 jobs making summary and profiling calls at 5,000 estimated tokens/call require 200 requests and 1,000,000 tokens if those calls fall in one minute, before other stages and retries.

Sources: [Azure OpenAI quota reference](https://learn.microsoft.com/en-us/azure/foundry/openai/quotas-limits), [quota allocation and enforcement](https://learn.microsoft.com/en-us/azure/foundry/openai/how-to/quota).

## Reliability requirements remain unchanged

- Cosmos is authoritative; messages are commands and delivery is at-least-once.
- Claim ownership and fence writes; validate versioned checkpoints before rerunning stages.
- Persist results and terminal state before completing messages.
- For delayed retry, atomically record retry state, due time, outbox event and ownership release before completing the current message. Abandon is not a backoff scheduler.
- Persist external job IDs and resume submitted work where the real provider API permits it.
- Coordinate lock renewal, lock-loss cancellation, shutdown and settlement for each interaction.

Concurrency does not supply these guarantees automatically. The actual sample consumer has not been converted into this proposed concurrent worker as part of this discussion.

## Validation plan

1. Measure one-job profile latency, stage durations, audio transfer volume and retained memory.
2. Verify actual clients are async/nonblocking, identify the Speech API, and inspect assigned Azure OpenAI TPM/RPM.
3. Test 10, 15 and 25 active interactions per pod with fixed Speech/LLM gates; measure completions, p95 profile latency, peak memory, bandwidth and throttling.
4. Test expected fleet-wide normal traffic, including a 25-job synchronized burst.
5. Stress-test 100 simultaneous jobs; require bounded resources, no lost work, correct settlement and documented latency degradation.
6. Exercise shutdown, lock/ownership loss and regional failure. Four pods provide 100 slots normally; loss of one region initially leaves only 50. Recovery to 100 requires the surviving region to reach four pods and have provider capacity.
7. Tune operation limits or increase pods only where measurements identify a local bottleneck. Do not increase concurrency to compensate for exhausted provider quota.

## Simulator artifacts

- [Simulator](../capacity-simulator/index.html): local browser interface with resource controls, timeline, HPA and failover scenarios.
- [Model assumptions](../capacity-simulator/README.md): scope and limitations.
- [Sample JSON](../capacity-simulator/sample-scenario.json): four pods at 25 interaction slots, 100-job burst. Its LLM setting is 10/pod; use 8/pod for the initial configuration above.

Fifteen deterministic engine tests passed during implementation; browser checks covered scenario switching, timeline controls and JSON export/import. The model approximates timing, resource contention and quota enforcement. It does not execute the actual Python worker or call Azure and cannot certify capacity or latency.

## Review addendum — 2026-10-05

This section records a later review of the plan above. The text above is unchanged. Where they conflict, this section reflects the newer decisions. Every number here is an **estimate from assumed timings**; replace it with measured values before treating it as capacity.

### Confirmed inputs

- **Speech:** Fast Transcription. It is synchronous: one HTTP request carries the audio and returns the transcript. There is no external job ID to persist or poll. A Speech slot stays busy for the whole upload plus transcription. A failed call is resubmitted and paid for again, so keep transcription retries bounded and write the transcript checkpoint as soon as the call returns.
- **LLM:** GPT-5.4, **Global Standard** deployment.
- **Audio upload:** streamed, so per-job audio memory is bounded. Verify that the multipart request body is not built in memory: pod memory should stay flat while one large file uploads. A retry must open a fresh Blob download because a stream can be read only once.

### How to size a pod

Kitchen analogy: Speech slots are **stoves**, LLM slots are **prep counters**, and the admission cap is **how many order tickets a pod takes off the shared rail** (Service Bus) at once.

- Take only as many tickets as keep the stoves and counters busy, plus a small margin. Extra tickets add no throughput. They sit in the pod while holding a Service Bus lock and a Cosmos lease, block other pods (including new HPA pods) from taking that work, and are all redelivered if the pod crashes.
- **The admission cap follows the slots.** Change the slot counts and the cap changes with them.
- The **Azure OpenAI quota** is shared by every pod and sets the fleet ceiling. More pods or more slots cannot exceed it; they only produce 429 responses.

The working rule (Little's law):

```text
pod rate (interactions/min) = min(speech_slots / T_speech,
                                  llm_slots / (sum of the four LLM stage times))
admission cap               ≈ pod rate × total time per interaction (+ small margin)
fleet rate                  ≤ OpenAI TPM / tokens per interaction
```

### Assumptions behind the numbers

| Input | Assumed value |
|---|---:|
| Fast Transcription (upload + transcription) | 30 s |
| LLM call duration | 20 s |
| LLM calls per interaction | 4 (summary, profiling, compliance, behaviour) |
| Tokens per LLM call (prompt + reasoning + response) | 5,000 |
| Active pods sharing one OpenAI deployment | 4 |

### Revised worker settings (supersede the Speech and LLM rows in "Initial configuration")

| Setting | Per pod | Four-pod fleet |
|---|---:|---:|
| Admission cap | 25 (unchanged) | 100 |
| Speech slots | **7** (was 4) | 28 |
| LLM slots | **17** (was 8) | 68 |
| Pods running normally | 2 per region | 4 |
| HPA maximum | 4 per region, **for regional failover only** | — |

With the default Tier 1 quota (1,000,000 TPM), the fleet ceiling is about **50 interactions/minute**. Expected results: a normal 25-interaction burst finishes in about 2–2.5 minutes; the 100-job stress test finishes in about 4 minutes.

Four pods already use the whole quota. Running more than four pods with these per-pod settings adds 429s, not throughput. If one region fails, the surviving region scales to four pods and carries the full load.

### Quota request (targets plus 50% buffer)

Targets: normal traffic of up to 25 simultaneous interactions at single-job speed; the 100-job stress test completing in about 4 minutes.

| Service | Needed | +50% buffer | Published default | Action |
|---|---:|---:|---:|---|
| Azure OpenAI GPT-5.4 Global Standard, tokens/min | 1,000,000 | **1,500,000** | 1,000,000 (Tier 1) | **Request 1.5M TPM** |
| Azure OpenAI GPT-5.4 Global Standard, requests/min | 200 | 300 | 10,000 (Tier 1) | None |
| Speech Fast Transcription, requests/min | 200 | 300 | 600 (S0) | None; S0 tier required |

- Normal traffic alone needs about 375k TPM (about 560k with buffer), which the default already covers. The stress test sets the 1.5M figure.
- The buffer is headroom for retries, longer responses and synchronized bursts. Azure enforces quota over windows shorter than a minute, so a synchronized start can receive 429s below the per-minute total. Honour `Retry-After` on 429 instead of failing the stage.
- If each region has its **own** OpenAI resource, request the full 1.5M in **each** region, because the surviving region must carry everything during a failover.
- Everything scales with tokens per call. At 8,000 tokens per call the request becomes about 2.4M TPM. GPT-5.4 reasoning tokens count, and the rate limiter counts `max_completion_tokens` when admitting a request, so keep that setting close to real need.
- **Stretch option:** the 100-job stress test at single-job speed (about 2 minutes for all) needs about 2.25M TPM with buffer (request about 2.5M or reach Tier 3), plus 25 Speech and 25 LLM slots per pod. Pursue it only if the product requires it.
- Not yet sized: Cosmos DB RU throughput (checkpoint writes, lease heartbeats, outbox and sweeper scans). It needs measured write counts.

Quota source: [Azure OpenAI quotas and limits](https://learn.microsoft.com/en-us/azure/foundry/openai/quotas-limits), page dated 2026-08-20. The account's actual tier and allocation were not inspected; the quota-tiers API reports the real tier.

### Async rule: nothing in the job path may block

Async remains the recommendation. The sample worker is already async (`AsyncMongoClient`, `azure.servicebus.aio`, `asyncio.sleep` in repository retries).

The risk is a single blocking call. One event loop serves all 25 jobs in a pod, plus the lease heartbeat (every 20 s, 60 s lease) and the Service Bus lock renewer. A blocking call stalls all of them. If it lasts longer than the lease, heartbeats are missed, another pod claims the jobs, and this pod hits `LeaseLost` on every active job. Fencing keeps the data correct, but the work and its paid Speech and OpenAI calls are repeated.

| Call | Must use | Status |
|---|---|---|
| Cosmos (Mongo API) | `AsyncMongoClient` | Done in sample |
| Service Bus | `azure.servicebus.aio` | Done in sample |
| Blob download | `azure.storage.blob.aio`, chunks read with `async for` | Check streaming code |
| Fast Transcription upload | Async HTTP client (for example `httpx.AsyncClient`) with an async-readable body | **Check first**: a sync client such as `requests`, or a multipart helper that needs a sync file object, blocks the loop |
| GPT-5.4 | `openai.AsyncAzureOpenAI` | Check |
| Sleeps, file reads, sync SDKs | Never on the event loop | Check |

- Wrap any unavoidable sync call in `await asyncio.to_thread(...)`. Put CPU-heavy work such as audio conversion in a separate process.
- During load tests set `PYTHONASYNCIODEBUG=1`. Python then logs any callback that holds the loop for more than 100 ms. Fix every such warning in the job path.

### Additions to the validation plan

- Record per-stage start and finish times (the checkpoint writes are a natural place) and use the mean stage times in the sizing rule above.
- Record **local wait time**: the time between admitting a message and starting its first stage. If it is not near zero, the admission cap is too high.
- Measure tokens per call on real jobs before submitting the quota request.
- Raise Speech slots in steps (4 → 7 → 10…) while watching pod memory (keep it under about 1.5 GiB), upload duration and 429 rates; stop when any of them degrades.
