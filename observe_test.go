package observe

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// ingest is a fake ingest endpoint: it answers with the next status in
// statuses (200 once they run out) and keeps every line it accepted.
type ingest struct {
	mu       sync.Mutex
	statuses []int
	lines    []map[string]any
	auth     string
	calls    atomic.Int32
}

func (in *ingest) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	in.calls.Add(1)
	in.mu.Lock()
	defer in.mu.Unlock()
	in.auth = r.Header.Get("Authorization")
	status := http.StatusOK
	if len(in.statuses) > 0 {
		status, in.statuses = in.statuses[0], in.statuses[1:]
	}
	if status != http.StatusOK {
		w.WriteHeader(status)
		return
	}
	var batch []map[string]any
	_ = json.NewDecoder(r.Body).Decode(&batch)
	in.lines = append(in.lines, batch...)
	w.WriteHeader(http.StatusOK)
}

func (in *ingest) accepted() []map[string]any {
	in.mu.Lock()
	defer in.mu.Unlock()
	return append([]map[string]any(nil), in.lines...)
}

type errs struct {
	mu   sync.Mutex
	list []error
}

func (e *errs) add(err error) { e.mu.Lock(); e.list = append(e.list, err); e.mu.Unlock() }
func (e *errs) text() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := errors.Join(e.list...); err != nil {
		return err.Error()
	}
	return ""
}

// testLogger builds a logger with its own queue against a fake endpoint.
func testLogger(t *testing.T, in *ingest, o Options) (*Logger, *errs) {
	t.Helper()
	srv := httptest.NewServer(in)
	t.Cleanup(srv.Close)
	reported := &errs{}
	o.IngestKey = "test-key"
	o.Endpoint = srv.URL
	o.Service = "checkout"
	o.FlushInterval = time.Hour
	o.OnError = reported.add
	l := &Logger{core: newCore()}
	l.core.configure(resolve(o))
	t.Cleanup(func() { _ = l.core.close(context.Background()) })
	return l, reported
}

func TestLinesAreSentWithServiceRedactionAndTrace(t *testing.T) {
	in := &ingest{}
	l, _ := testLogger(t, in, Options{})

	tp := sdktrace.NewTracerProvider()
	ctx, span := tp.Tracer("t").Start(context.Background(), "op")
	defer span.End()

	fields := Fields{"orderId": 7, "password": "hunter2", "req": map[string]any{"authorization": "Bearer x"}}
	l.With(Fields{"region": "eu"}).WithContext(ctx).Info("order placed", fields)
	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}

	lines := in.accepted()
	if len(lines) != 1 {
		t.Fatalf("got %d lines, want 1", len(lines))
	}
	got := lines[0]
	checks := map[string]any{
		"message": "order placed", "severity": "info", "service": "checkout", "region": "eu",
		"password": "[Redacted]", "trace_id": span.SpanContext().TraceID().String(),
	}
	for k, want := range checks {
		if got[k] != want {
			t.Errorf("%s = %v, want %v", k, got[k], want)
		}
	}
	if got["req"].(map[string]any)["authorization"] != "[Redacted]" {
		t.Errorf("nested authorization not redacted: %v", got["req"])
	}
	if fields["password"] != "hunter2" {
		t.Error("the caller's map was modified")
	}
	if in.auth != "Bearer test-key" {
		t.Errorf("auth header %q", in.auth)
	}
}

func TestPassingFailureKeepsLinesForTheRetry(t *testing.T) {
	in := &ingest{statuses: []int{http.StatusServiceUnavailable}}
	l, reported := testLogger(t, in, Options{})

	l.Warn("first", nil)
	if err := l.Flush(); err == nil {
		t.Fatal("want the 503 reported")
	}
	if n := len(in.accepted()); n != 0 {
		t.Fatalf("accepted %d lines on a 503", n)
	}
	// A timer tick during the backoff waits it out instead of hammering.
	_ = l.core.flush(false)
	if in.calls.Load() != 1 {
		t.Fatalf("sent during backoff: %d calls", in.calls.Load())
	}
	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}
	if n := len(in.accepted()); n != 1 {
		t.Fatalf("got %d lines after the retry, want 1", n)
	}
	if !strings.Contains(reported.text(), "retrying") {
		t.Errorf("reported %q", reported.text())
	}
}

func TestRejectedBatchIsDroppedAndReported(t *testing.T) {
	in := &ingest{statuses: []int{http.StatusUnauthorized}}
	l, reported := testLogger(t, in, Options{})

	l.ErrorMsg("never accepted", nil)
	_ = l.Flush()
	l.Info("after", nil)
	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}
	lines := in.accepted()
	if len(lines) != 1 || lines[0]["message"] != "after" {
		t.Fatalf("got %v", lines)
	}
	if !strings.Contains(reported.text(), "dropped 1 log lines") {
		t.Errorf("reported %q", reported.text())
	}
}

func TestQueueBoundDropsOldest(t *testing.T) {
	in := &ingest{}
	l, reported := testLogger(t, in, Options{MaxQueueSize: 3, MaxBatchSize: 100})

	for _, m := range []string{"a", "b", "c", "d", "e"} {
		l.Info(m, nil)
	}
	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, line := range in.accepted() {
		got = append(got, line["message"].(string))
	}
	if strings.Join(got, "") != "cde" {
		t.Fatalf("kept %v, want the newest three", got)
	}
	deadline := time.Now().Add(time.Second)
	for !strings.Contains(reported.text(), "dropped 2 oldest") && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !strings.Contains(reported.text(), "dropped 2 oldest") {
		t.Errorf("reported %q", reported.text())
	}
}

func TestNoKeyPrintsWithoutErrors(t *testing.T) {
	reported := &errs{}
	var out bytes.Buffer
	c := newCore()
	c.console = &out
	c.configure(resolve(Options{IngestKey: "", OnError: reported.add, FlushInterval: time.Hour}))
	t.Setenv("SEENTICS_INGEST_KEY", "")
	l := &Logger{core: c}

	l.Info("one", nil)
	l.Info("two", nil)
	if strings.Count(out.String(), "\n") != 2 {
		t.Fatalf("console output %q", out.String())
	}
	if len(reported.list) != 0 {
		t.Fatalf("errors without a key: %v", reported.list)
	}
}

func TestMiddlewareNamesSpansByRouteAndContinuesTheTrace(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	var inHandler string
	mux := http.NewServeMux()
	mux.HandleFunc("GET /monitors/{id}", func(w http.ResponseWriter, r *http.Request) {
		inHandler = otelTraceID(r.Context())
		w.WriteHeader(http.StatusTeapot)
	})
	h := Middleware(mux, MiddlewareOptions{Skip: func(r *http.Request) bool { return r.URL.Path == "/health" }})

	req := httptest.NewRequest(http.MethodGet, "/monitors/0f8e-42", nil)
	req.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/health", nil))

	if res.Code != http.StatusTeapot {
		t.Fatalf("status %d", res.Code)
	}
	spans := rec.Ended()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1 (health is skipped)", len(spans))
	}
	s := spans[0]
	if s.Name() != "GET /monitors/{id}" {
		t.Errorf("span name %q", s.Name())
	}
	if s.SpanContext().TraceID().String() != "4bf92f3577b34da6a3ce929d0e0e4736" || inHandler != s.SpanContext().TraceID().String() {
		t.Errorf("trace not continued: span %s, handler %s", s.SpanContext().TraceID(), inHandler)
	}
}

func TestSlogHandlerForwardsAttrsAndGroups(t *testing.T) {
	in := &ingest{}
	l, _ := testLogger(t, in, Options{})
	prev := Log
	Log = l
	t.Cleanup(func() { Log = prev })

	var printed bytes.Buffer
	logger := slog.New(SlogHandler(slog.NewJSONHandler(&printed, nil))).With("svc", "uptime").WithGroup("req")
	logger.Warn("slow", "id", 42, slog.Group("db", "ms", 12), "err", errors.New("boom"))
	logger.Debug("hidden")
	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}

	lines := in.accepted()
	if len(lines) != 1 {
		t.Fatalf("got %d lines, want 1 (debug is below the inner handler's level)", len(lines))
	}
	got := lines[0]
	want := map[string]any{"severity": "warn", "message": "slow", "svc": "uptime", "req.id": float64(42), "req.db.ms": float64(12), "req.err": "boom"}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %v, want %v", k, got[k], v)
		}
	}
	if !strings.Contains(printed.String(), `"msg":"slow"`) {
		t.Errorf("inner handler did not print: %q", printed.String())
	}
}

func otelTraceID(ctx context.Context) string {
	return trace.SpanContextFromContext(ctx).TraceID().String()
}

// A batch in flight is not in the queue: lines written while it is being sent
// must not push it out, and its success must not take unsent lines with it.
// It used to: the overflow dropped the in-flight lines from the front, and the
// success then removed as many again — lines that had never been sent.
func TestOverflowWhileSendingLosesNoUnsentLine(t *testing.T) {
	in := &ingest{}
	release := make(chan struct{})
	var first atomic.Bool
	var arrived atomic.Int32
	gate := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		arrived.Add(1)
		if first.CompareAndSwap(false, true) {
			<-release
		}
		in.ServeHTTP(w, r)
	})
	srv := httptest.NewServer(gate)
	t.Cleanup(srv.Close)
	l := &Logger{core: newCore()}
	l.core.configure(resolve(Options{IngestKey: "k", Endpoint: srv.URL, FlushInterval: time.Hour, MaxQueueSize: 4, MaxBatchSize: 2, OnError: func(error) {}}))
	t.Cleanup(func() { _ = l.core.close(context.Background()) })

	for _, m := range []string{"a", "b", "c", "d"} {
		l.Info(m, nil)
	}
	done := make(chan error, 1)
	go func() { done <- l.Flush() }()
	for arrived.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	l.Info("e", nil) // a and b are in flight; c, d, e, f fit the queue
	l.Info("f", nil)
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, line := range in.accepted() {
		got = append(got, line["message"].(string))
	}
	if strings.Join(got, "") != "abcdef" {
		t.Fatalf("delivered %v, want every line once, in order", got)
	}
}

func TestConcurrentShutdownDoesNotPanic(t *testing.T) {
	in := &ingest{}
	l, _ := testLogger(t, in, Options{})
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = l.core.close(context.Background()) }()
	}
	wg.Wait()
}

// A file served through the middleware still reaches the connection's own
// ReadFrom (sendfile), and is recorded as a 200.
func TestMiddlewareKeepsReadFrom(t *testing.T) {
	var used bool
	w := &readFromRecorder{ResponseRecorder: httptest.NewRecorder(), used: &used}
	h := Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(w, struct{ io.Reader }{strings.NewReader("hello")}) // a plain reader, as a file is
	}))
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/file", nil))
	if !used || w.Body.String() != "hello" || w.Code != http.StatusOK {
		t.Fatalf("ReadFrom used=%v body=%q code=%d", used, w.Body.String(), w.Code)
	}
}

type readFromRecorder struct {
	*httptest.ResponseRecorder
	used *bool
}

func (r *readFromRecorder) ReadFrom(src io.Reader) (int64, error) {
	*r.used = true
	return io.Copy(r.ResponseRecorder, src)
}

// http.server.request.duration is in seconds, as OpenTelemetry specifies and the Node SDK
// records: one metric in two units read 1000x off on every chart of it.
func TestRequestDurationIsInSeconds(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	durationOnce, durationHist = sync.Once{}, nil
	t.Cleanup(func() { otel.SetMeterProvider(previous); durationOnce, durationHist = sync.Once{}, nil })

	mux := http.NewServeMux()
	mux.HandleFunc("GET /slow", func(w http.ResponseWriter, r *http.Request) { time.Sleep(30 * time.Millisecond) })
	Middleware(mux, MiddlewareOptions{MeasureOnly: func(*http.Request) bool { return true }}).
		ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/slow", nil))

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "http.server.request.duration" {
				continue
			}
			if m.Unit != "s" {
				t.Fatalf("unit %q, want s", m.Unit)
			}
			sum := m.Data.(metricdata.Histogram[float64]).DataPoints[0].Sum
			if sum < 0.025 || sum > 1 {
				t.Fatalf("recorded %v for a ~30ms request, want about 0.03 (seconds)", sum)
			}
			return
		}
	}
	t.Fatal("http.server.request.duration not recorded")
}
