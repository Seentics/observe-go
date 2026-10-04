// Package observe sends logs, traces and metrics from a Go service straight
// to Seentics Observe — one ingest key, no collector to run.
//
// Logs are plain structured JSON, no OpenTelemetry dependency. Traces and
// metrics run on the real go.opentelemetry.io/otel SDK underneath; this
// package only removes the exporter/provider/resource boilerplate.
//
//	observe.Configure(observe.Options{Service: "checkout-service"})
//	defer observe.Shutdown(context.Background())
//
//	observe.Log.Info("order placed", observe.Fields{"orderId": 81423})
//	observe.Log.WithContext(ctx).Warn("payment slow", nil) // linked to the request's trace
//
//	http.ListenAndServe(":8080", observe.Middleware(mux)) // a span per request
//	slog.SetDefault(slog.New(observe.SlogHandler(slog.NewJSONHandler(os.Stdout, nil))))
//
//	ctx, span := observe.Tracer().Start(ctx, "process-order")
//	defer span.End()
//
//	counter, _ := observe.Meter().Int64Counter("orders.processed")
//	counter.Add(ctx, 1)
package observe

import (
	"os"
	"strconv"
	"time"
)

// Options configures logs, traces and metrics together. Every field has an
// environment-variable fallback, so most services need none of them.
type Options struct {
	// IngestKey is the key from a project's Setup page. Falls back to
	// SEENTICS_INGEST_KEY. Without one nothing is sent anywhere: log lines
	// print to stderr instead, and Tracer()/Meter() are no-ops.
	IngestKey string

	// Endpoint defaults to the hosted ingest endpoint. Override for a
	// self-hosted deployment, or set SEENTICS_INGEST_ENDPOINT.
	Endpoint string

	// Service is attached to every log line as "service", and as the
	// trace/metric resource's service.name. Falls back to
	// SEENTICS_SERVICE_NAME, then OTEL_SERVICE_NAME.
	Service string

	// ServiceVersion is attached as service.version — a release tag or git
	// sha. Falls back to SEENTICS_SERVICE_VERSION.
	ServiceVersion string

	// Environment is attached as "deployment.environment". Falls back to
	// SEENTICS_ENVIRONMENT.
	Environment string

	// DefaultFields are merged into every log line.
	DefaultFields Fields

	// Redact lists field names whose values are replaced with "[Redacted]",
	// at any depth, compared case-insensitively. Nil means the defaults
	// (password, token, secret, authorization, cookie, api keys); an empty,
	// non-nil slice turns redaction off.
	Redact []string

	// MaxBatchSize is how many log lines are sent per request. Default 2000.
	MaxBatchSize int

	// MaxQueueSize is the most log lines held while the endpoint is
	// unreachable or a burst is being sent. Past it the oldest are dropped
	// and reported through OnError. Default 10000.
	MaxQueueSize int

	// FlushInterval is how often buffered lines are sent. Default 5s.
	FlushInterval time.Duration

	// Console also prints every line to stderr as JSON. Default: on without
	// an ingest key, off with one. Env: SEENTICS_CONSOLE.
	Console *bool

	// TracesSampleRate is the share of new traces kept, 0–1. Default 1. A
	// trace continued from an upstream service keeps the upstream's decision.
	// Env: SEENTICS_TRACES_SAMPLE_RATE.
	TracesSampleRate *float64

	// MetricExportInterval is how often metrics export. Default 60s.
	MetricExportInterval time.Duration

	// RuntimeMetrics reports the Go runtime's memory, goroutines and GC.
	// Default true.
	RuntimeMetrics *bool

	// HostMetrics reports process CPU and memory, and the server's CPU, load,
	// memory and root disk (Linux only), plus host.* and os.type resource
	// attributes. Default true. Env: SEENTICS_HOST_METRICS (also
	// OBSERVE_HOST_METRICS). Turn it off if the OpenTelemetry Collector's
	// hostmetrics receiver already covers the host — both would send it.
	HostMetrics *bool

	// OnError is called when something goes wrong inside the SDK — a failed
	// send, dropped lines. Nothing here panics or blocks, so this is the only
	// way to notice. Defaults to writing to stderr.
	OnError func(error)
}

// Configure wires logs, traces and metrics together. Optional: without a
// call, everything reads the SEENTICS_* environment variables on first use.
// Call it when the key comes from somewhere other than the environment, or to
// name the service in code. Safe to call more than once; the last call wins
// for logs, and the first starts traces and metrics.
func Configure(opts Options) error {
	cfg := resolve(opts)
	Log.core.configure(cfg)
	if cfg.ingestKey == "" {
		return nil
	}
	return startTelemetry(cfg)
}

// resolved is Options with every environment fallback and default applied.
type resolved struct {
	ingestKey            string
	endpoint             string
	service              string
	serviceVersion       string
	environment          string
	defaultFields        Fields
	redact               []string
	maxBatchSize         int
	maxQueueSize         int
	flushInterval        time.Duration
	console              bool
	sampleRate           float64
	metricExportInterval time.Duration
	runtimeMetrics       bool
	hostMetrics          bool
	onError              func(error)
}

func resolve(o Options) resolved {
	r := resolved{
		ingestKey:            firstNonEmpty(o.IngestKey, os.Getenv("SEENTICS_INGEST_KEY")),
		endpoint:             firstNonEmpty(o.Endpoint, os.Getenv("SEENTICS_INGEST_ENDPOINT"), defaultEndpoint),
		service:              firstNonEmpty(o.Service, os.Getenv("SEENTICS_SERVICE_NAME"), os.Getenv("OTEL_SERVICE_NAME")),
		serviceVersion:       firstNonEmpty(o.ServiceVersion, os.Getenv("SEENTICS_SERVICE_VERSION")),
		environment:          firstNonEmpty(o.Environment, os.Getenv("SEENTICS_ENVIRONMENT")),
		defaultFields:        o.DefaultFields,
		redact:               o.Redact,
		maxBatchSize:         positive(o.MaxBatchSize, 2000),
		maxQueueSize:         positive(o.MaxQueueSize, 10_000),
		flushInterval:        o.FlushInterval,
		metricExportInterval: o.MetricExportInterval,
		runtimeMetrics:       o.RuntimeMetrics == nil || *o.RuntimeMetrics,
		onError:              o.OnError,
	}
	if r.maxBatchSize > maxRequestBatch {
		r.maxBatchSize = maxRequestBatch
	}
	if r.redact == nil {
		r.redact = defaultRedact
	}
	if r.flushInterval <= 0 {
		r.flushInterval = 5 * time.Second
	}
	if r.metricExportInterval <= 0 {
		r.metricExportInterval = 60 * time.Second
	}
	switch {
	case o.Console != nil:
		r.console = *o.Console
	case os.Getenv("SEENTICS_CONSOLE") != "":
		r.console, _ = strconv.ParseBool(os.Getenv("SEENTICS_CONSOLE"))
	default:
		r.console = r.ingestKey == ""
	}
	r.hostMetrics = o.HostMetrics == nil || *o.HostMetrics
	if o.HostMetrics == nil {
		if v, err := strconv.ParseBool(firstNonEmpty(os.Getenv("SEENTICS_HOST_METRICS"), os.Getenv("OBSERVE_HOST_METRICS"))); err == nil {
			r.hostMetrics = v
		}
	}
	r.sampleRate = 1
	if o.TracesSampleRate != nil {
		r.sampleRate = *o.TracesSampleRate
	} else if v, err := strconv.ParseFloat(os.Getenv("SEENTICS_TRACES_SAMPLE_RATE"), 64); err == nil {
		r.sampleRate = v
	}
	if r.onError == nil {
		r.onError = stderrError
	}
	return r
}

func positive(v, fallback int) int {
	if v > 0 {
		return v
	}
	return fallback
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// Traces and metrics start at import when the key is already in the
// environment: a tracer/meter provider has to exist before the first span or
// instrument is created, and Go's init runs before main.
func init() {
	if os.Getenv("SEENTICS_INGEST_KEY") != "" {
		_ = Configure(Options{})
	}
}
