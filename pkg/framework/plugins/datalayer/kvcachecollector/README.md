# kv-cache-collector

Polls each model's pool-aggregated Prometheus metrics endpoint and writes a per-model `KVCacheMetrics` attribute carrying KV-cache utilization and queue depth.


## Plugin configuration

```yaml
plugins:
  - type: kv-cache-collector
    parameters:
      interval: "30s"                                # poll interval, default 30s
      timeout:  "5s"                                 # per-scrape HTTP timeout, default 5s
      utilizationMetric: "vllm:kv_cache_usage_perc"  # default; override for older vLLM
      queueDepthMetric:  "vllm:num_requests_waiting" # default
      maxConcurrent: 8                               # concurrent scrapes per poll, default 8

datalayer:
  collectors:
    - pluginRef: kv-cache-collector
```

## Per-model input

The collector reads each model's `metrics-endpoint` attribute, attached by
`modelconfigcollector` when a model entry in `models.json` has a non-empty
`metricsURL`:

```json
{ "models": [
    { "name": "llama-3.1-8b",
      "metricsURL": "http://llama-pool.svc:9090/federate?match[]=..." } ] }
```

The URL is contractually a *pool-aggregated* endpoint (Prometheus `/federate`, a thin aggregator, or — for dev — a single representative pod). The collector does no pod discovery and no client-side aggregation.

## What it writes (per scraped model)

| Where | Contents |
|--|--|
| Datastore attribute `kv-cache-metrics` | `KVCacheMetrics{Utilization, QueueDepth, LastObservedAt}` |
| Prometheus gauge `ipp_kv_cache_utilization_ratio{model}` | Mirror of the last observed utilization |
| Prometheus histogram `ipp_kv_cache_scrape_duration_seconds{model}` | Per-scrape latency (successes and failures) |
| Prometheus counter `ipp_kv_cache_scrape_failures_total{model, reason}` | Failure counts by reason (`dial`, `timeout`, `http_status`, `parse`, `request_build`) |

On failure the prior attribute and gauge are left in place; freshness is gated
on `LastObservedAt` by downstream consumers.

## Manual smoke test

[hack/kvcache-manual-test/run-test.sh](../../../../../hack/kvcache-manual-test/run-test.sh)
spins up a fake `/metrics`, builds the IPP, runs four end-to-end assertions, and
tears down.
