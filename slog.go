package observe

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/trace"
)

// SlogHandler sends every slog record to Seentics Observe as well as to
// inner, so a service already logging through log/slog needs one line:
//
//	slog.SetDefault(slog.New(observe.SlogHandler(slog.NewJSONHandler(os.Stdout, nil))))
//
// inner decides the level and does the printing; nil means send only, at
// Info and above. Attributes become fields, groups become dotted keys
// ("req.id"), and a record logged with a context (slog.InfoContext) carries
// that context's trace_id and span_id.
func SlogHandler(inner slog.Handler) slog.Handler {
	return &slogHandler{inner: inner}
}

type slogHandler struct {
	inner  slog.Handler
	attrs  Fields
	prefix string // open group, "a.b." — applies to attrs added after it
}

func (h *slogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	if h.inner != nil {
		return h.inner.Enabled(ctx, level)
	}
	return level >= slog.LevelInfo
}

func (h *slogHandler) Handle(ctx context.Context, r slog.Record) error {
	fields := make(Fields, len(h.attrs)+r.NumAttrs()+2)
	for k, v := range h.attrs {
		fields[k] = v
	}
	r.Attrs(func(a slog.Attr) bool {
		addAttr(fields, h.prefix, a)
		return true
	})
	if ctx != nil {
		if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
			fields["trace_id"] = sc.TraceID().String()
			fields["span_id"] = sc.SpanID().String()
		}
	}
	Log.core.enqueueFrom(slogSeverity(r.Level), r.Message, fields, false)

	if h.inner != nil {
		return h.inner.Handle(ctx, r)
	}
	return nil
}

func (h *slogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := &slogHandler{prefix: h.prefix, attrs: make(Fields, len(h.attrs)+len(attrs))}
	for k, v := range h.attrs {
		next.attrs[k] = v
	}
	for _, a := range attrs {
		addAttr(next.attrs, h.prefix, a)
	}
	if h.inner != nil {
		next.inner = h.inner.WithAttrs(attrs)
	}
	return next
}

func (h *slogHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	next := &slogHandler{prefix: h.prefix + name + ".", attrs: h.attrs}
	if h.inner != nil {
		next.inner = h.inner.WithGroup(name)
	}
	return next
}

func addAttr(fields Fields, prefix string, a slog.Attr) {
	v := a.Value.Resolve()
	if v.Kind() == slog.KindGroup {
		group := v.Group()
		if len(group) == 0 {
			return
		}
		// An inline group (empty key) adds its attrs at the current level.
		p := prefix
		if a.Key != "" {
			p = prefix + a.Key + "."
		}
		for _, ga := range group {
			addAttr(fields, p, ga)
		}
		return
	}
	if a.Key == "" {
		return
	}
	fields[prefix+a.Key] = slogValue(v)
}

func slogValue(v slog.Value) any {
	switch v.Kind() {
	case slog.KindString:
		return v.String()
	case slog.KindInt64:
		return v.Int64()
	case slog.KindUint64:
		return v.Uint64()
	case slog.KindFloat64:
		return v.Float64()
	case slog.KindBool:
		return v.Bool()
	case slog.KindDuration:
		return v.Duration().String()
	case slog.KindTime:
		return v.Time().UTC().Format(time.RFC3339Nano)
	default:
		if err, ok := v.Any().(error); ok {
			return err.Error()
		}
		return v.Any()
	}
}

func slogSeverity(l slog.Level) Severity {
	switch {
	case l >= slog.LevelError+4:
		return SeverityFatal
	case l >= slog.LevelError:
		return SeverityError
	case l >= slog.LevelWarn:
		return SeverityWarn
	case l >= slog.LevelInfo:
		return SeverityInfo
	case l >= slog.LevelDebug:
		return SeverityDebug
	default:
		return SeverityTrace
	}
}
