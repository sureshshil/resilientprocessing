# FPA Pipeline – Service Bus Request

2026-09-28

Please create one Azure Service Bus namespace and one queue per environment for jp-fpa-svc. The API will send a small job message per recording, and a new worker deployment (jp-fpa-worker) will receive it.

## Request

One namespace and one queue per environment, all in Japan East. Keep prod in its own namespace so a dev or perf service can never read prod jobs.

| Environment | Namespace (suggested; use your naming standard) | Queue | Tier |
| --- | --- | --- | --- |
| dev | sb-jp-fpa-dev-jpe | fpa-pipeline | Standard |
| perf | sb-jp-fpa-perf-jpe | fpa-pipeline | Standard |
| prod | sb-jp-fpa-prod-jpe | fpa-pipeline | Standard |

Standard tier covers everything the app uses. If our network policy requires a private endpoint, the namespace must be Premium instead, because private endpoints are Premium-only.

No topics, subscriptions or sessions are needed.

## Queue settings

Same settings in all three environments. Items marked "fixed at creation" cannot be changed later without recreating the queue.

| Property | Value | Why |
| --- | --- | --- |
| Lock duration | 1 minute | The worker renews the lock automatically while it works |
| Max delivery count | 10 | After 10 failed deliveries the message goes to the dead-letter queue |
| Default message time to live | 14 days | Unprocessed jobs survive a long worker outage |
| Dead-lettering on message expiration | Enabled | Expired messages stay visible instead of disappearing |
| Max queue size | 1 GB (default) | Messages are under 1 KB each |
| Requires session | No (fixed at creation) | Jobs are independent |
| Duplicate detection | No (fixed at creation) | The app handles duplicates itself |
| Partitioning | No (fixed at creation) | Not needed at this volume |
| Auto-forwarding | None | |
| Auto-delete on idle | Never | |

Message body: `{"jobId": "<sessionId>:<recordingId>"}`, content type `application/json`. It carries no customer data.

## Access

The app authenticates with Entra ID workload identity (as jp-fpa-svc already does), not with connection strings. Assign roles at the **queue** scope, not the namespace.

| Identity | Used by | Role on queue fpa-pipeline |
| --- | --- | --- |
| jp-fpa-svc managed identity (existing) | API deployment | Azure Service Bus Data Sender |
| jp-fpa-worker managed identity (new, or reuse the API one) | Worker deployment | Azure Service Bus Data Receiver |
| FPA developers group | dev only, to inspect the dead-letter queue | Azure Service Bus Data Owner |

- If the worker gets its own identity, it also needs a federated credential for the new Kubernetes service account `jp-fpa-worker`, and the same Cosmos DB access as jp-fpa-svc.
- If the worker reuses the API identity, give that identity both Sender and Receiver.
- Please disable local (SAS key) authentication on the namespaces if that is our standard.

## Network

Both deployments connect outbound from AKS to the namespace. Nothing connects inbound to the pods.

- Allow egress to `<namespace>.servicebus.windows.net` on **TCP 5671** (AMQP over TLS) and **443**.
- If Istio restricts outbound traffic (`REGISTRY_ONLY`), add a ServiceEntry for that host and those ports.
- If the namespace uses a private endpoint (Premium), the AKS subnet needs DNS resolution to it.

## Monitoring

Please send the namespace diagnostic logs to our Log Analytics workspace and add these alerts (prod and perf; dev optional). Thresholds are starting points we will tune after launch.

| Metric (queue fpa-pipeline) | Alert when | Meaning |
| --- | --- | --- |
| Dead-lettered messages | > 0 for 5 min | A job message could not be processed; needs a look |
| Active messages | > 50 for 15 min | Workers are not keeping up or are down |
| Server errors | > 0 for 5 min | Service Bus side problem |
| Throttled requests | > 0 for 15 min | Namespace limits reached |

## What we need back

- [ ] Namespace hostname per environment (e.g. `sb-jp-fpa-dev-jpe.servicebus.windows.net`)
- [ ] Queue name, if different from `fpa-pipeline`
- [ ] Client ID of the worker's managed identity, and its Kubernetes service account name
- [ ] Confirmation that role assignments and the network rules are in place
- [ ] Tier chosen (Standard or Premium) and whether a private endpoint is used
