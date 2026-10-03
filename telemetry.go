package observe

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// Traces and metrics ride on the real OpenTelemetry Go SDK — the wire
// format, span-context propagation, and id generation are all things worth
// not reimplementing. This file removes the exporter/provider/resource
// boilerplate every service would otherwise write by hand.
//
// Tracer() and Meter() are *functions*, not vars captured once —
// otel.Tracer(name) resolves against whichever provider is registered at
// call time, so a span started after Configure is always a real one.

const instrumentationName = "seentics-observe-go"

var (
	telemetryMu      sync.Mutex
	tracerProvider   *sdktrace.TracerProvider
	meterProvider    *sdkmetric.MeterProvider
	telemetryStarted bool
)

// The propagator is set at import, key or not: a service without a key still
// passes an incoming traceparent on to the services it calls.
func init() {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
}

func startTelemetry(cfg resolved) error {
	telemetryMu.Lock()
	defer telemetryMu.Unlock()
	if telemetryStarted {
		return nil
	}

	ctx := context.Background()

	attrs := []attribute.KeyValue{
		semconv.ServiceNameKey.String(serviceOrDefault(cfg.service)),
		semconv.TelemetrySDKLanguageGo,
	}
	if cfg.serviceVersion != "" {
		attrs = append(attrs, semconv.ServiceVersionKey.String(cfg.serviceVersion))
	}
	if cfg.environment != "" {
		attrs = append(attrs, attribute.String("deployment.environment", cfg.environment))
	}
	res, err := resource.New(ctx, buildResourceOptions(cfg, attrs)...)
	if err != nil {
		return fmt.Errorf("build resource: %w", err)
	}

	// Export failures reach OnError instead of the otel default (a log line on
	// stderr per failed batch).
	onError := cfg.onError
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) { onError(fmt.Errorf("telemetry: %w", err)) }))

	headers := map[string]string{"Authorization": "Bearer " + cfg.ingestKey}

	// The OTLP exporters retry on their own (exponential backoff, honouring
	// Retry-After) for up to a minute per batch.
	traceExporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpointURL(cfg.endpoint+"/v1/traces"),
		otlptracehttp.WithHeaders(headers),
	)
	if err != nil {
		return fmt.Errorf("build trace exporter: %w", err)
	}
	rate := cfg.sampleRate
	if rate < 0 {
		rate = 0
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(traceExporter),
		// A trace begun upstream keeps the upstream's decision, so a sampled
		// request is traced end to end or not at all.
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(rate))),
	)

	metricExporter, err := otlpmetrichttp.New(ctx,
		otlpmetrichttp.WithEndpointURL(cfg.endpoint+"/v1/metrics"),
		otlpmetrichttp.WithHeaders(headers),
	)
	if err != nil {
		_ = tp.Shutdown(ctx)
		return fmt.Errorf("build metric exporter: %w", err)
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter, sdkmetric.WithInterval(cfg.metricExportInterval))),
	)

	tracerProvider, meterProvider = tp, mp
	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	telemetryStarted = true

	if cfg.runtimeMetrics {
		// With host metrics on, process.memory.usage is reported there as RSS.
		if err := registerRuntimeMetrics(mp.Meter(instrumentationName), !cfg.hostMetrics); err != nil {
			onError(fmt.Errorf("runtime metrics: %w", err))
		}
	}
	if cfg.hostMetrics {
		if err := registerHostMetrics(mp.Meter(instrumentationName)); err != nil {
			onError(fmt.Errorf("host metrics: %w", err))
		}
	}
	return nil
}

// buildResourceOptions adds host.arch, os.type and service.instance.id when
// host metrics are on. OTEL_RESOURCE_ATTRIBUTES is read after them, so a value
// the user already set wins; host.name and process.pid come from the detectors.
func buildResourceOptions(cfg resolved, attrs []attribute.KeyValue) []resource.Option {
	opts := []resource.Option{resource.WithHost(), resource.WithProcessPID()}
	if cfg.hostMetrics {
		host, _ := os.Hostname()
		opts = append(opts, resource.WithAttributes(
			attribute.String("host.arch", runtime.GOARCH),
			attribute.String("os.type", runtime.GOOS),
			semconv.ServiceInstanceID(fmt.Sprintf("%s-%d", host, os.Getpid())),
		), resource.WithFromEnv())
	}
	return append(opts, resource.WithAttributes(attrs...))
}

// registerRuntimeMetrics reports memory, goroutines and GC, read once per
// export — ReadMemStats stops the world for microseconds, so never per call.
// withProcessMemory adds process.memory.usage as bytes obtained from the OS;
// registerHostMetrics reports it as resident memory instead.
func registerRuntimeMetrics(m metric.Meter, withProcessMemory bool) error {
	heap, err1 := m.Int64ObservableGauge("process.runtime.go.mem.heap_alloc", metric.WithUnit("By"),
		metric.WithDescription("Bytes of allocated heap objects"))
	goroutines, err3 := m.Int64ObservableGauge("process.runtime.go.goroutines",
		metric.WithDescription("Goroutines that currently exist"))
	gcCount, err4 := m.Int64ObservableCounter("process.runtime.go.gc.count",
		metric.WithDescription("Completed garbage collection cycles"))
	gcPause, err5 := m.Float64ObservableCounter("process.runtime.go.gc.pause_total", metric.WithUnit("s"),
		metric.WithDescription("Total time the world was stopped for garbage collection"))
	if err := errors.Join(err1, err3, err4, err5); err != nil {
		return err
	}
	instruments := []metric.Observable{heap, goroutines, gcCount, gcPause}
	var sys metric.Int64ObservableGauge
	if withProcessMemory {
		var err error
		sys, err = m.Int64ObservableGauge("process.memory.usage", metric.WithUnit("By"),
			metric.WithDescription("Bytes of memory obtained from the OS"))
		if err != nil {
			return err
		}
		instruments = append(instruments, sys)
	}
	_, err := m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		o.ObserveInt64(heap, int64(ms.HeapAlloc))
		if withProcessMemory {
			o.ObserveInt64(sys, int64(ms.Sys))
		}
		o.ObserveInt64(goroutines, int64(runtime.NumGoroutine()))
		o.ObserveInt64(gcCount, int64(ms.NumGC))
		o.ObserveFloat64(gcPause, float64(ms.PauseTotalNs)/1e9)
		return nil
	}, instruments...)
	return err
}

// Tracer returns the package's tracer, resolved against whatever provider
// is currently registered — safe to call before or after Configure().
//
//	ctx, span := observe.Tracer().Start(ctx, "process-order")
//	defer span.End()
func Tracer() trace.Tracer {
	return otel.Tracer(instrumentationName)
}

// Meter returns the package's meter, same live-binding behaviour as
// Tracer().
//
//	counter, _ := observe.Meter().Int64Counter("orders.processed")
//	counter.Add(ctx, 1)
func Meter() metric.Meter {
	return otel.Meter(instrumentationName)
}

// Shutdown sends everything still buffered — log lines, spans, metrics — and
// stops. Call it before the process exits, with a deadline:
//
//	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
//	defer cancel()
//	observe.Shutdown(ctx)
func Shutdown(ctx context.Context) error {
	telemetryMu.Lock()
	tp, mp := tracerProvider, meterProvider
	telemetryStarted = false
	tracerProvider, meterProvider = nil, nil
	telemetryMu.Unlock()

	var errs []error
	if tp != nil {
		if err := tp.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("traces: %w", err))
		}
	}
	if mp != nil {
		if err := mp.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("metrics: %w", err))
		}
	}
	// Last, so lines logged while spans were ending are sent too.
	if err := Log.core.close(ctx); err != nil {
		errs = append(errs, fmt.Errorf("logs: %w", err))
	}
	return errors.Join(errs...)
}

func serviceOrDefault(service string) string {
	if service == "" {
		return "unknown-service"
	}
	return service
}
