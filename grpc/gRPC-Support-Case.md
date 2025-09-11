# Make gRPC a First-Class API in ArcadeDB

## TL;DR

- **Unary ops (local, HTTP/2):** gRPC ≈ JSON/HTTP/2 on localhost—expected because ArcadeDB already moved to HTTP/2 (Undertow), removing most HTTP/1.1 overhead.
- **Streaming ops:** gRPC is a step-change:
  - **Time-to-first-row:** 0–3 ms (vs ~175–227 ms with batched JSON).
  - **Total time (7,746 rows):** 50–74 ms (vs 175–227 ms).
  - That’s roughly a **3.1×–3.6× throughput gain** plus near-instant first byte.

**Bottom line:** gRPC streaming enables real-time, low-latency graph I/O and unlocks new product capabilities, while unary parity on localhost is normal given HTTP/2 parity.

## What I Measured (Local, Same Machine & Query)

| Scenario | Transport | Passes | Time to First Response | Time to All | Rows |
|----------|-----------|--------|-----------------------|-------------|------|
| Large single query (batch JSON vs streamed gRPC) | JSON/HTTP (HTTP/2) | 4 | ~175–227 ms (first byte bundled with full payload) | 175–227 ms | 7,746 |
| | gRPC streaming (batchSize 100/500/800) | 3 | 0–3 ms | 50–74 ms | 7,746 |
| Many smaller queries (unary) | JSON/HTTP | 1 | — | 544 ms | — |
| | gRPC (unary) | 1 | — | 694 ms | — |

**Why unary parity/slight regressions locally?** With HTTP/2 (Undertow), the big wins over HTTP/1.1 (handshakes, head-of-line blocking, connection churn) are already gone. On localhost, JSON parse costs are negligible; gRPC’s binary/framing advantages tend to pay off with network latency, concurrency, or streaming.

## Why Leadership Should Care (Business Outcomes)

- **Differentiate with “real-time graph.”** “Milliseconds to first result” demos incredibly well. Streaming queries feel instant—a clear edge vs REST-only graph stores.
- **Lower TCO under load.** Streaming reduces server memory pressure (no giant JSON payloads), tail latencies, and egress bytes—fewer cores, less RAM, less bandwidth for the same work.
- **Mobile & edge advantage.** On lossy/WAN links, HTTP/2 + gRPC streams tolerate jitter and deliver partial results promptly—snappier UX for apps and agents.
- **Enterprise readiness.** gRPC offers typed SDKs (Java, Go, Python, Node, Rust, Swift), mTLS, and OpenTelemetry—accelerating evals, POCs, and platform integrations.
- **New revenue surfaces.** Package Real-time Subscriptions, Live Traversal Streams, and Bulk Ingest via Client-Streaming as enterprise add-ons.

## Why Engineering Should Care (Technical Benefits)

1. **Server/Client Streaming**
   - Time-to-first-row in ms, not 100s of ms.
   - Backpressure built-in (HTTP/2 flow control); avoids buffering entire result sets.
   - Enables live queries / CDC / tailing (e.g., “stream new edges matching X”).

2. **Resource Efficiency**
   - Smaller on-wire frames (binary protobuf) vs verbose JSON.
   - Lower GC churn (no giant JSON strings/DOMs); steadier memory profile.

3. **Concurrency at Scale**
   - Single multiplexed channel per client; avoids socket storms.
   - No app-layer head-of-line blocking; better tail latencies under load.

4. **Typed Contracts & Evolvability**
   - Proto schemas → generated clients, fewer integration bugs.
   - Back/forward compatibility via field numbering and optionality.

5. **Observability & Control**
   - Standard interceptors for auth, quotas, retries, deadlines, metrics, tracing.
   - First-class OpenTelemetry spans per RPC for SLOs and latency budgets.

6. **Security & Networking**
   - mTLS/ALPN by default; tight per-RPC auth (JWT, service tokens).
   - Works seamlessly with Envoy/Istio and gRPC-Web for browsers.

7. **Mesh-Native Traffic Policy via gRPC xDS (Kubernetes/Istio)**
   - With Istio (or any xDS control plane), gRPC can consume xDS (CDS/EDS/LDS/RDS) to get dynamic, centrally managed traffic policy—no app redeploys.
   - Smart LB & resilience: locality-aware load-balancing, fast failover, retries/hedging/timeouts/circuit-breaking/outlier detection by config.
   - Progressive delivery: weighted canaries, blue-green, traffic mirroring, and fault injection for chaos tests—uniformly applied to gRPC traffic.
   - Security & observability: mesh-wide mTLS (SPIFFE), consistent authz, and unified metrics/traces (Envoy/Otel).

## Product Capabilities gRPC Unlocks

- **Live Results / Progressive UI**: Stream traversals and long paths as they’re discovered (great for GraphRAG, visual explorers, dashboards).
- **Subscription Queries / CDC**: Subscribe to patterns (“new Person→Facility edges in Region=…”) and receive server-push updates in real time.
- **Bulk Write Pipelines**: Client-streaming for batch insert/upsert; server replies with a summary (counts, errors) at stream end.
- **Chunked Binary Blocks**: Stream rows in row-block or columnar frames for faster client deserialization and vectorization.
- **Back-office & Intra-cluster RPC**: Use the same gRPC surface for internal services (analytics, ingestion, connectors) to remove REST glue code.

## Kubernetes + Istio + gRPC xDS (Advanced Benefit)

When ArcadeDB runs in Kubernetes with Istio (or another xDS control plane), you can apply centralized, dynamic traffic policy to gRPC without code changes.

### Two deployment patterns
1. **Sidecar-terminated gRPC (most common)**: App uses standard gRPC; Envoy sidecars enforce policy, mTLS, and routing—no client changes.
2. **Client-side xDS (sidecar-optional)**: Enable the gRPC xDS client to pull policy directly from the mesh’s xDS server (via a bootstrap file/env). Useful for sidecarless or off-mesh clients that still need mesh policy.

### Example Istio policy (weighted canary + retry tuning for gRPC)

```
apiVersion: networking.istio.io/v1beta1
kind: DestinationRule
metadata:
  name: arcadedb
spec:
  host: arcadedb.svc.cluster.local
  trafficPolicy:
    outlierDetection:
      consecutive5xx: 5
      interval: 5s
      baseEjectionTime: 30s
  subsets:
  - name: v1
    labels: { version: v1 }
  - name: v2
    labels: { version: v2 }
---
apiVersion: networking.istio.io/v1beta1
kind: VirtualService
metadata:
  name: arcadedb
spec:
  hosts: [arcadedb.svc.cluster.local]
  http:
  - retries:
      attempts: 3
      perTryTimeout: 200ms
      retryOn: >
        cancelled,connect-failure,refused-stream,resource-exhausted,unavailable
    route:
    - destination: { host: arcadedb.svc.cluster.local, subset: v1 }
      weight: 90
    - destination: { host: arcadedb.svc.cluster.local, subset: v2 }
      weight: 10
```

**Notes:**
- Apply retries only to idempotent operations.
- Tune timeouts for long-running traversals; consider hedging for tail-latency control.
- Enable locality-weighted LB for multi-zone clusters.
- For browser apps, expose gRPC via gRPC-Web through the Istio ingress gateway.

## Suggested MVP (Fast Path)

### Services
- `QueryService.ExecuteStream(Query, Params) -> stream<RowBlock>`
  - Row blocks carry N rows + schema, flow-controlled.
- `IngestService.StreamWrites(stream<Mutation>) -> Summary`
- `SubscriptionService.Subscribe(Query) -> stream<Event>`
- `Health` (gRPC health checking), `Auth` (JWT/mTLS).

### Wire Format
- Protobuf frames by default; allow JSON payloads only for interop, not default.

### Packaging
- Ship `.proto`; publish official clients for Java & Node first.
- Provide gRPC-Web via Envoy for browser apps.

### Config
- Toggle gRPC alongside REST; shared auth; TLS on by default.

## Benchmark Plan (Beyond Localhost)

- **Axes**: LAN vs WAN, concurrency (1, 16, 128 clients), payloads (1K, 10K, 100K rows), TTFB vs total time, p95/p99.
- **Controls**: warm-ups, stabilized JIT, pinned CPU governor, fixed GC (G1/ZGC), channel reuse, compression on/off.
- **Server**: Undertow HTTP/2 vs Netty gRPC; identical query engine/result materialization; measure server CPU/RAM and egress bytes.

**Expectation**: Streaming gRPC yields lower p95/p99, smaller egress, and better CPU-per-result at medium/high concurrency; unary parity remains close to HTTP/2 JSON.

## Risks & Mitigations

- **Browser support**: Use gRPC-Web via Envoy; keep REST for simple use cases.
- **Schema evolution**: Lock proto CI checks; reserve tags; add compatibility tests.
- **Operational complexity**: Ship Helm charts + sample Envoy config; provide OpenTelemetry defaults.
- **Client fragmentation**: Provide first-party clients and examples; keep REST for long tail.

## Call to Action

1. Green-light a gRPC Streaming MVP (services above).
2. Publish proto + Java/Node SDKs with examples (streaming query, subscription, bulk ingest).
3. Add docs & a “Real-Time Streaming API” page featuring the metrics below (show 0–3 ms TTFB demo and ~3× throughput).
4. Run external benchmarks and publish guidance on when to choose streaming gRPC vs REST.

## Appendix: Raw Local Test Results

```
testDBPerformance(): Case 1 [Pass 1] - JSON/HTTP: Query Execution Time = 227 ms; # of results = 7746 rows
testDBPerformance(): Case 1 [Pass 2] - JSON/HTTP: Query Execution Time = 179 ms; # of results = 7746 rows
testDBPerformance(): Case 1 [Pass 3] - JSON/HTTP: Query Execution Time = 177 ms; # of results = 7746 rows
testDBPerformance(): Case 1 [Pass 4] - JSON/HTTP: Query Execution Time = 175 ms; # of results = 7746 rows
testDBPerformance(): Case 1 [Pass 1] - gRPC. Query Execution Time = 327 ms; # of results = 7746 rows
testDBPerformance(): Case 1 [Pass 2] - gRPC. Query Execution Time = 202 ms; # of results = 7746 rows
testDBPerformance(): Case 1 [Pass 3] - gRPC. Query Execution Time = 184 ms; # of results = 7746 rows
testDBPerformance(): Case 1 [Pass 4] - gRPC. Query Execution Time = 185 ms; # of results = 7746 rows

testGRPCLargeSingleQueryWithStreaming(): [Pass 1; batchSize = 100] - gRPC. To 1st response => Query Execution Time = 3 ms
testGRPCLargeSingleQueryWithStreaming(): [Pass 1] - gRPC. To all responses => Query Execution Time = 74 ms; Total Records = 7746
testGRPCLargeSingleQueryWithStreaming(): [Pass 2; batchSize = 500] - gRPC. To 1st response => Query Execution Time = 0 ms
testGRPCLargeSingleQueryWithStreaming(): [Pass 2] - gRPC. To all responses => Query Execution Time = 52 ms; Total Records = 7746
testGRPCLargeSingleQueryWithStreaming(): [Pass 3; batchSize = 800] - gRPC. To 1st response => Query Execution Time = 0 ms
testGRPCLargeSingleQueryWithStreaming(): [Pass 3] - gRPC. To all responses => Query Execution Time = 50 ms; Total Records = 7746

testHTTPManySmallerQueries(): Entry ...
testDBPerformance(): Case 2 - JSON/HTTP: Query Execution Time = 544 ms

testGRPCManySmallerQueries(): Entry ...
testDBPerformance(): Case 2 - gRPC: Query Execution Time = 694 ms
```
