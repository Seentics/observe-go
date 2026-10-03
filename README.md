# observe-go

Logs, traces and metrics for Go, shipped straight to [Seentics Observe](https://observe.seentics.com) — one ingest key, no collector to run.

Logs are plain structured JSON. Traces and metrics run on the real `go.opentelemetry.io/otel` SDK underneath (id generation, context propagation, OTLP encoding are all things worth not reimplementing) — this package removes the exporter/provider/resource boilerplate, and adds the HTTP middleware and `log/slog` handler most services need.

## Install

```bash
go get github.com/seentics/observe-go@latest
```

```go
import observe "github.com/seentics/observe-go"
```

## Quick start

Grab an ingest key from a project's **Setup** page, set `SEENTICS_INGEST_KEY` and `SEENTICS_SERVICE_NAME`, then:

```go
func main() {
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		observe.Shutdown(ctx) // sends what is still buffered: logs, spans, metrics
	}()

	// Already using log/slog? Every record is sent too, with its trace.
	slog.SetDefault(slog.New(observe.SlogHandler(slog.NewJSONHandler(os.Stdout, nil))))

	mux := http.NewServeMux()
	mux.HandleFunc("GET /orders/{id}", getOrder)
	http.ListenAndServe(":8080", observe.Middleware(mux)) // a span and a duration per request
}
```

Or configure in code instead of the environment:

```go
err := observe.Configure(observe.Options{
	IngestKey:   key,
	Service:     "checkout-service",
	Environment: "production",
})
```

### Logs

```go
observe.Log.Info("order placed", observe.Fields{"orderId": 81423, "total": 42.5})

// Linked to the request's trace (trace_id, span_id):
observe.Log.WithContext(r.Context()).Warn("payment provider slow", observe.Fields{"latencyMs": 2400})

// Fields on every line from this logger:
orders := observe.Log.With(observe.Fields{"component": "orders"})

// A caught error — its Error() becomes the line, plus error.message and error.type:
observe.Log.Error(err, observe.Fields{"orderId": order.ID})
```

`Fatal`/`FatalMsg` log at the "fatal" severity and never call `os.Exit`.

Values under secret-looking names — `password`, `token`, `authorization`, `cookie`, `api_key` and similar — are replaced with `[Redacted]` at any depth before a line leaves the process. `Options.Redact` changes the list.

### HTTP

`observe.Middleware(handler)` starts a server span per request, continuing the caller's trace when a `traceparent` arrives, and records `http.server.request.duration` (ms, by `http.route`, `http.request.method`, `http.status_class`). Routes come from `http.ServeMux` patterns, so `/orders/81423` is recorded as `/orders/{id}`.

```go
observe.Middleware(mux, observe.MiddlewareOptions{
	Skip:        func(r *http.Request) bool { return r.URL.Path == "/health" },
	MeasureOnly: func(r *http.Request) bool { return strings.HasPrefix(r.URL.Path, "/collect") },
})
```

### Traces

```go
ctx, span := observe.Tracer().Start(ctx, "process-order")
defer span.End()
```

To continue a trace in a service you call, inject the context into the outgoing request:

```go
otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
```

### Metrics

```go
ordersProcessed, _ := observe.Meter().Int64Counter("orders.processed")
ordersProcessed.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "success")))
```

The Go runtime is reported on its own: `process.runtime.go.mem.heap_alloc`, `process.runtime.go.goroutines`, `process.runtime.go.gc.count`, `process.runtime.go.gc.pause_total`.

### Process and host metrics

Also on by default, read once per export interval, standard library only:

| Metric | Unit | Attributes |
| --- | --- | --- |
| `process.cpu.utilization` | `1` | share of all cores, 0 to 1 |
| `process.memory.usage` | `By` | resident memory (Go runtime bytes where `/proc` is missing) |
| `process.uptime` | `s` | |
| `system.cpu.utilization` | `1` | `state`: user, system, idle, nice, interrupt, softirq, steal, wait |
| `system.cpu.load_average.1m` | `{thread}` | |
| `system.memory.usage` | `By` | `state`: used, free, cached |
| `system.memory.utilization` | `1` | `state`: used, free |
| `system.filesystem.usage` / `.utilization` | `By` / `1` | `mountpoint`, `device`, `state`: used, free (real disks only, at most 8) |
| `system.network.io` | `By` | `device`, `direction`: receive, transmit (not `lo` or container bridges) |
| `system.disk.io` | `By` | `device`, `direction`: read, write (whole disks only) |

The `system.*` metrics are read from `/proc` and `statfs`, so they are reported on Linux only; other platforms send the `process.*` ones. The resource also gets `host.name`, `host.arch`, `os.type`, `process.pid` and `service.instance.id` (values from `OTEL_RESOURCE_ATTRIBUTES` win).

Turn it off with `Options.HostMetrics` or `SEENTICS_HOST_METRICS=false` (`OBSERVE_HOST_METRICS` works too). Do not run it on a host where the OpenTelemetry Collector's `hostmetrics` receiver is also enabled: the series would be sent twice.

## Delivery

Logging calls never block on the network and never panic. Lines are queued and sent in batches:

- **Endpoint down or rate-limited** (5xx, 429, network error): the batch stays queued and is retried with exponential backoff up to 60s, honouring `Retry-After`. Reported through `OnError` once per outage, not once per retry.
- **Rejected for good** (a revoked key, a 4xx): the batch is dropped and reported — retrying would fail the same way forever.
- **Queue full** (`MaxQueueSize`): the oldest lines are dropped and the count is reported, with the cause.

Spans and metrics use the OTLP exporters' own retry.

## Environment

| Variable | Same as |
| --- | --- |
| `SEENTICS_INGEST_KEY` | `Options.IngestKey` |
| `SEENTICS_INGEST_ENDPOINT` | `Options.Endpoint` (defaults to the hosted endpoint) |
| `SEENTICS_SERVICE_NAME` (falls back to `OTEL_SERVICE_NAME`) | `Options.Service` |
| `SEENTICS_SERVICE_VERSION` | `Options.ServiceVersion` |
| `SEENTICS_ENVIRONMENT` | `Options.Environment` |
| `SEENTICS_CONSOLE` | `Options.Console` |
| `SEENTICS_TRACES_SAMPLE_RATE` | `Options.TracesSampleRate` |
| `SEENTICS_HOST_METRICS` (or `OBSERVE_HOST_METRICS`) | `Options.HostMetrics` |

With the key in the environment, traces and metrics start at import, so `Configure` is optional.

## Options

| Field | Default | |
| --- | --- | --- |
| `IngestKey` | `SEENTICS_INGEST_KEY` | without one nothing is sent — see below |
| `Endpoint` | hosted ingest endpoint | one override covers logs, traces and metrics |
| `Service` | `SEENTICS_SERVICE_NAME` / `OTEL_SERVICE_NAME` | log `service`, resource `service.name` |
| `ServiceVersion` | `SEENTICS_SERVICE_VERSION` | resource `service.version` |
| `Environment` | `SEENTICS_ENVIRONMENT` | `deployment.environment` |
| `DefaultFields` | `nil` | merged into every log line |
| `Redact` | secret-looking names | an empty, non-nil slice turns redaction off |
| `MaxBatchSize` | `500` | log lines per request (at most 5000) |
| `MaxQueueSize` | `10000` | log lines held while sending is behind |
| `FlushInterval` | `5s` | how often queued lines are sent |
| `Console` | on without a key | also print every line to stderr as JSON |
| `TracesSampleRate` | `1` | share of new traces kept; continued traces keep the caller's decision |
| `MetricExportInterval` | `60s` | |
| `RuntimeMetrics` | `true` | Go runtime memory, goroutines, GC |
| `HostMetrics` | `true` | process and host CPU, memory, disk, network; host figures on Linux only |
| `OnError` | writes to stderr | the only place SDK failures surface |

## A missing key never breaks your app

Without an ingest key, log lines print to stderr and nothing is sent; `Tracer()`/`Meter()` fall back to OpenTelemetry's no-op implementation. The SDK says so once, not per line.
