package observe

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func benchLogger(b *testing.B) *Logger {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
	}))
	b.Cleanup(srv.Close)
	l := &Logger{core: newCore()}
	l.core.configure(resolve(Options{IngestKey: "k", Endpoint: srv.URL, Service: "bench", FlushInterval: time.Hour, OnError: func(error) {}}))
	b.Cleanup(func() { _ = l.core.close(context.Background()) })
	return l
}

func BenchmarkLogParallel(b *testing.B) {
	l := benchLogger(b)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.Info("order placed", Fields{"orderId": 81423, "total": 12.5, "user": Fields{"id": 7, "token": "x"}})
		}
	})
}

func BenchmarkLogSerial(b *testing.B) {
	l := benchLogger(b)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		l.Info("order placed", Fields{"orderId": 81423, "total": 12.5, "user": Fields{"id": 7, "token": "x"}})
	}
}
