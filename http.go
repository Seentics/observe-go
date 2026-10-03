package observe

import (
	"bufio"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// MiddlewareOptions tunes Middleware. The zero value traces and measures
// every request.
type MiddlewareOptions struct {
	// Skip leaves a request untraced and unmeasured — health checks, say.
	Skip func(*http.Request) bool

	// MeasureOnly records the request in the duration histogram but starts no
	// span — high-volume endpoints whose every call would drown the rest.
	MeasureOnly func(*http.Request) bool
}

// Middleware gives every request a server span, continuing the caller's
// trace when a traceparent header arrives, and records it in the
// http.server.request.duration histogram (seconds, by http.route,
// http.request.method and http.status_class). The span is on the request's
// context, so Log.WithContext(r.Context()) links log lines to it.
//
// Routes come from http.ServeMux patterns ("GET /monitors/{id}" is recorded
// as "/monitors/{id}"), which keeps span names and metric series bounded.
// Requests no pattern matched are recorded as "unmatched".
func Middleware(next http.Handler, opts ...MiddlewareOptions) http.Handler {
	var o MiddlewareOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if o.Skip != nil && o.Skip(r) {
			next.ServeHTTP(w, r)
			return
		}
		started := time.Now()
		rec := &statusRecorder{ResponseWriter: w}

		if o.MeasureOnly != nil && o.MeasureOnly(r) {
			inner := r.WithContext(r.Context())
			defer func() { recordDuration(inner, started, rec.code()) }()
			next.ServeHTTP(rec, inner)
			return
		}

		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := Tracer().Start(ctx, r.Method+" "+r.URL.Path,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("http.request.method", r.Method),
				attribute.String("url.path", r.URL.Path),
			),
		)
		inner := r.WithContext(ctx)
		defer func() {
			status := rec.code()
			if p := recover(); p != nil {
				status = http.StatusInternalServerError
				span.SetStatus(codes.Error, "panic")
				span.AddEvent("panic", trace.WithAttributes(attribute.String("panic.value", panicString(p))))
				finish(span, inner, started, status)
				panic(p)
			}
			if status >= 500 {
				span.SetStatus(codes.Error, http.StatusText(status))
			}
			finish(span, inner, started, status)
		}()
		next.ServeHTTP(rec, inner)
	})
}

func finish(span trace.Span, r *http.Request, started time.Time, status int) {
	route := routeOf(r)
	span.SetName(r.Method + " " + route)
	span.SetAttributes(attribute.String("http.route", route), attribute.Int("http.response.status_code", status))
	span.End()
	recordDuration(r, started, status)
}

// routeOf reads the pattern the mux matched. ServeMux sets it on the request
// it is handed, which is this middleware's own copy.
func routeOf(r *http.Request) string {
	p := r.Pattern
	if p == "" {
		return "unmatched"
	}
	if i := strings.IndexByte(p, ' '); i >= 0 {
		p = p[i+1:]
	}
	// A host-qualified pattern ("example.com/x") keeps only the path.
	if i := strings.IndexByte(p, '/'); i > 0 {
		p = p[i:]
	}
	return p
}

var (
	durationOnce sync.Once
	durationHist metric.Float64Histogram
)

func recordDuration(r *http.Request, started time.Time, status int) {
	// Created on first use, not at import: the meter provider is only real
	// once Configure has run.
	durationOnce.Do(func() {
		// Seconds, with OpenTelemetry's HTTP duration buckets: the unit its semantic conventions
		// fix for this metric, and the one the Node SDK's instrumentation records. In
		// milliseconds, as it was, a project with Go and Node services stored one metric in two
		// units and every chart of it read one of them 1000x off.
		durationHist, _ = Meter().Float64Histogram("http.server.request.duration", metric.WithUnit("s"),
			metric.WithExplicitBucketBoundaries(0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10),
			metric.WithDescription("Time to answer each request, by route, method and status class"))
	})
	if durationHist == nil {
		return
	}
	durationHist.Record(r.Context(), time.Since(started).Seconds(), metric.WithAttributes(
		attribute.String("http.route", routeOf(r)),
		attribute.String("http.request.method", r.Method),
		attribute.String("http.status_class", statusClass(status)),
	))
}

func statusClass(status int) string {
	if status < 100 || status > 599 {
		return "5xx"
	}
	return string(rune('0'+status/100)) + "xx"
}

func panicString(p any) string {
	if err, ok := p.(error); ok {
		return err.Error()
	}
	if s, ok := p.(string); ok {
		return s
	}
	return "non-error panic"
}

// statusRecorder remembers the status written. Unwrap lets
// http.ResponseController reach the real writer's Flush, deadlines and
// hijacking; Flush and Hijack are also passed through for code that
// type-asserts.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	if s.status == 0 {
		s.status = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	return s.ResponseWriter.Write(b)
}

func (s *statusRecorder) code() int {
	if s.status == 0 {
		return http.StatusOK
	}
	return s.status
}

func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

func (s *statusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		if s.status == 0 {
			s.status = http.StatusOK
		}
		f.Flush()
	}
}

// ReadFrom keeps the writer's own ReadFrom — sendfile(2) for http.ServeFile and
// io.Copy from a file — which wrapping it would otherwise hide.
func (s *statusRecorder) ReadFrom(r io.Reader) (int64, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	if rf, ok := s.ResponseWriter.(io.ReaderFrom); ok {
		return rf.ReadFrom(r)
	}
	return io.Copy(struct{ io.Writer }{s.ResponseWriter}, r)
}

func (s *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := s.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, errors.New("observe: the response writer does not support hijacking")
}
