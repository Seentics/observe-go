package observe

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"
)

// Severity matches the console's own scale — sending one of these values
// means the line lands in the right bucket with no server-side translation.
type Severity string

const (
	SeverityTrace Severity = "trace"
	SeverityDebug Severity = "debug"
	SeverityInfo  Severity = "info"
	SeverityWarn  Severity = "warn"
	SeverityError Severity = "error"
	SeverityFatal Severity = "fatal"
)

// Fields is arbitrary structured data attached to one line — every key
// becomes a searchable column the moment it's ingested.
type Fields map[string]any

const (
	maxRequestBatch   = 5000 // the ingest endpoint's cap on one request
	httpClientTimeout = 10 * time.Second
	maxBackoff        = 60 * time.Second
)

// Logger writes structured lines. Calls never block on the network and never
// panic: lines are queued and sent in batches in the background, retried with
// backoff when the endpoint is down, and bounded in memory.
//
// Log is the shared logger. With and WithContext return loggers that add
// fields, or the trace of a request, to every line they write.
type Logger struct {
	core   *logCore
	fields Fields
	ctx    context.Context
}

// Log is the shared logger — configured by Configure, or from the
// environment on first use.
var Log = &Logger{core: newCore()}

// With returns a logger that adds fields to every line it writes.
func (l *Logger) With(fields Fields) *Logger {
	merged := Fields{}
	for k, v := range l.fields {
		merged[k] = v
	}
	for k, v := range fields {
		merged[k] = v
	}
	return &Logger{core: l.core, fields: merged, ctx: l.ctx}
}

// WithContext returns a logger whose lines carry the trace and span of ctx
// (trace_id, span_id), so the console links each line to its request.
func (l *Logger) WithContext(ctx context.Context) *Logger {
	return &Logger{core: l.core, fields: l.fields, ctx: ctx}
}

func (l *Logger) Trace(message string, fields Fields) { l.write(SeverityTrace, message, fields) }
func (l *Logger) Debug(message string, fields Fields) { l.write(SeverityDebug, message, fields) }
func (l *Logger) Info(message string, fields Fields)  { l.write(SeverityInfo, message, fields) }
func (l *Logger) Warn(message string, fields Fields)  { l.write(SeverityWarn, message, fields) }

// Error logs a caught error: its message is the line, and it is attached as
// error.message and error.type.
func (l *Logger) Error(err error, fields Fields) {
	l.write(SeverityError, errMessage(err), withError(fields, err))
}

// ErrorMsg logs a plain-string error line.
func (l *Logger) ErrorMsg(message string, fields Fields) { l.write(SeverityError, message, fields) }

// Fatal logs at the "fatal" severity. It never exits the process — call
// os.Exit yourself, after Shutdown, if the condition should end it.
func (l *Logger) Fatal(err error, fields Fields) {
	l.write(SeverityFatal, errMessage(err), withError(fields, err))
}

func (l *Logger) FatalMsg(message string, fields Fields) { l.write(SeverityFatal, message, fields) }

// Flush sends everything queued now, ignoring any retry backoff.
func (l *Logger) Flush() error { return l.core.flush(true) }

func (l *Logger) write(severity Severity, message string, fields Fields) {
	var sc trace.SpanContext
	if l.ctx != nil {
		sc = trace.SpanContextFromContext(l.ctx)
	}
	if len(l.fields) == 0 && !sc.IsValid() {
		l.core.enqueue(severity, message, fields)
		return
	}
	// The caller's map is never modified.
	merged := make(Fields, len(l.fields)+len(fields)+2)
	for k, v := range l.fields {
		merged[k] = v
	}
	for k, v := range fields {
		merged[k] = v
	}
	if sc.IsValid() {
		merged["trace_id"] = sc.TraceID().String()
		merged["span_id"] = sc.SpanID().String()
	}
	l.core.enqueue(severity, message, merged)
}

func errMessage(err error) string {
	if err == nil {
		return "<nil error>"
	}
	return err.Error()
}

func withError(fields Fields, err error) Fields {
	out := Fields{}
	if err != nil {
		out["error.message"] = err.Error()
		out["error.type"] = fmt.Sprintf("%T", err)
	}
	for k, v := range fields {
		out[k] = v
	}
	return out
}

// ── The queue ────────────────────────────────────────────────────────────────

type logCore struct {
	mu         sync.Mutex
	cfg        resolved
	configured bool
	redact     map[string]struct{}
	queue      []map[string]any
	client     *http.Client

	sending      sync.Mutex // one send at a time, so lines leave in order
	backoffUntil time.Time
	failures     int
	dropped      int
	noKeyNoted   bool

	timerOnce sync.Once
	stop      chan struct{}
	console   io.Writer
}

func newCore() *logCore {
	return &logCore{client: &http.Client{Timeout: httpClientTimeout}, stop: make(chan struct{}), console: os.Stderr}
}

func (c *logCore) configure(cfg resolved) {
	c.mu.Lock()
	c.cfg = cfg
	c.redact = nameSet(cfg.redact)
	c.configured = true
	c.mu.Unlock()
	c.startTimer()
}

func (c *logCore) ensureConfigured() {
	c.mu.Lock()
	done := c.configured
	c.mu.Unlock()
	if !done {
		c.configure(resolve(Options{}))
	}
}

func (c *logCore) startTimer() {
	c.timerOnce.Do(func() {
		c.mu.Lock()
		interval := c.cfg.flushInterval
		c.mu.Unlock()
		go func() {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					_ = c.flush(false)
				case <-c.stop:
					return
				}
			}
		}()
	})
}

func (c *logCore) enqueue(severity Severity, message string, fields Fields) {
	c.enqueueFrom(severity, message, fields, true)
}

// enqueueFrom with console false never prints: a line from SlogHandler has
// already been written by the handler it wraps.
func (c *logCore) enqueueFrom(severity Severity, message string, fields Fields, console bool) {
	c.ensureConfigured()

	c.mu.Lock()
	cfg := c.cfg
	record := map[string]any{}
	for k, v := range cfg.defaultFields {
		record[k] = v
	}
	for k, v := range fields {
		record[k] = v
	}
	record = redactFields(record, c.redact)
	record["timestamp"] = time.Now().UTC().Format(time.RFC3339Nano)
	record["severity"] = string(severity)
	record["message"] = message
	if cfg.service != "" {
		record["service"] = cfg.service
	}
	if cfg.environment != "" {
		record["deployment.environment"] = cfg.environment
	}

	if cfg.console && console {
		if line, err := json.Marshal(record); err == nil {
			fmt.Fprintln(c.console, string(line))
		}
	}
	if cfg.ingestKey == "" {
		// Without a key the line is printed (Console) and nothing is sent. Said
		// once, not per line — a service without observability is not an error.
		noted := c.noKeyNoted
		c.noKeyNoted = true
		c.mu.Unlock()
		if !noted && !cfg.console && console {
			cfg.onError(errors.New("no ingest key: log lines are not sent — set SEENTICS_INGEST_KEY or pass one to observe.Configure"))
		}
		return
	}

	c.queue = append(c.queue, record)
	if over := len(c.queue) - cfg.maxQueueSize; over > 0 {
		c.queue = c.queue[over:]
		c.dropped += over
	}
	sendNow := len(c.queue) >= cfg.maxBatchSize && time.Now().After(c.backoffUntil)
	c.mu.Unlock()

	if sendNow {
		go func() { _ = c.flush(false) }()
	}
}

// flush sends the queue in batches, oldest first, one request at a time. A
// batch that fails for a passing reason stays queued and the queue waits out
// a backoff (Retry-After when the server gives one); one the server will never
// accept is dropped and reported. force ignores the backoff.
func (c *logCore) flush(force bool) error {
	c.sending.Lock()
	defer c.sending.Unlock()

	var lastErr error
	for {
		c.mu.Lock()
		cfg := c.cfg
		if len(c.queue) == 0 || (!force && time.Now().Before(c.backoffUntil)) {
			c.reportDroppedLocked()
			c.mu.Unlock()
			return lastErr
		}
		n := len(c.queue)
		if n > cfg.maxBatchSize {
			n = cfg.maxBatchSize
		}
		batch := append([]map[string]any(nil), c.queue[:n]...)
		c.mu.Unlock()

		retryable, retryAfter, err := c.send(cfg.endpoint, cfg.ingestKey, batch)

		c.mu.Lock()
		switch {
		case err == nil:
			c.queue = c.queue[min(n, len(c.queue)):]
			c.failures = 0
			c.backoffUntil = time.Time{}
		case !retryable:
			// Will fail the same way every time — a revoked key, a plan limit.
			c.queue = c.queue[min(n, len(c.queue)):]
			c.mu.Unlock()
			cfg.onError(fmt.Errorf("dropped %d log lines: %w", n, err))
			lastErr = err
			continue
		default:
			c.failures++
			delay := retryAfter
			if delay <= 0 {
				delay = time.Second << min(c.failures-1, 6)
			}
			if delay > maxBackoff {
				delay = maxBackoff
			}
			c.backoffUntil = time.Now().Add(delay)
			first := c.failures == 1
			c.mu.Unlock()
			// Once per outage, not once per retry.
			if first {
				cfg.onError(fmt.Errorf("log flush failed, retrying: %w", err))
			}
			return err
		}
		c.mu.Unlock()
	}
}

// reportDroppedLocked says how many lines the queue bound dropped, and why.
func (c *logCore) reportDroppedLocked() {
	if c.dropped == 0 {
		return
	}
	count, cause := c.dropped, "because lines were written faster than they could be sent — raise MaxQueueSize or MaxBatchSize for bursts this size"
	if c.failures > 0 {
		cause = "while the ingest endpoint was unreachable"
	}
	c.dropped = 0
	onError := c.cfg.onError
	go onError(fmt.Errorf("dropped %d oldest log lines — the queue hit MaxQueueSize (%d) %s", count, c.cfg.maxQueueSize, cause))
}

func (c *logCore) send(endpoint, ingestKey string, records []map[string]any) (retryable bool, retryAfter time.Duration, err error) {
	body, err := json.Marshal(records)
	if err != nil {
		return false, 0, fmt.Errorf("encode log batch: %w", err)
	}
	req, err := http.NewRequest(http.MethodPost, endpoint+"/logs", bytes.NewReader(body))
	if err != nil {
		return false, 0, fmt.Errorf("build ingest request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+ingestKey)

	res, err := c.client.Do(req)
	if err != nil {
		return true, 0, fmt.Errorf("send log batch: %w", err)
	}
	defer res.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 1<<16))

	if res.StatusCode >= 200 && res.StatusCode < 300 {
		return false, 0, nil
	}
	err = fmt.Errorf("ingest responded %s", res.Status)
	if res.StatusCode == http.StatusTooManyRequests || res.StatusCode == http.StatusRequestTimeout || res.StatusCode >= 500 {
		if s, convErr := strconv.Atoi(res.Header.Get("Retry-After")); convErr == nil && s >= 0 {
			retryAfter = time.Duration(s) * time.Second
		}
		return true, retryAfter, err
	}
	return false, 0, err
}

// close stops the timer and sends what is queued, waiting at most until ctx
// is done.
func (c *logCore) close(ctx context.Context) error {
	select {
	case <-c.stop:
	default:
		close(c.stop)
	}
	done := make(chan error, 1)
	go func() { done <- c.flush(true) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func stderrError(err error) {
	fmt.Fprintf(os.Stderr, "[seentics-observe] %v\n", err)
}
