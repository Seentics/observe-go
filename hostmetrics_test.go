package observe

import (
	"context"
	"math"
	"runtime"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestParseCPUStat(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want cpuTimes
		ok   bool
	}{
		{"aggregate line", "cpu  100 5 50 800 20 3 2 1 0 0\ncpu0 50 2 25 400 10 1 1 0 0 0\nintr 1", cpuTimes{100, 5, 50, 800, 20, 3, 2, 1}, true},
		{"old kernel without steal", "cpu 1 2 3 4 5 6 7\n", cpuTimes{}, false},
		{"garbage", "cpu a b c d e f g h\n", cpuTimes{}, false},
		{"empty", "", cpuTimes{}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseCPUStat(tc.in)
			if ok != tc.ok || got != tc.want {
				t.Fatalf("got %+v %v, want %+v %v", got, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestCPUShares(t *testing.T) {
	prev := cpuTimes{user: 100, system: 50, idle: 800, iowait: 10}
	cur := cpuTimes{user: 130, system: 60, idle: 860, iowait: 10, steal: 0, irq: 0, nice: 0}
	got, ok := cpuShares(prev, cur)
	if !ok {
		t.Fatal("expected shares")
	}
	want := map[string]float64{"user": 0.3, "system": 0.1, "idle": 0.6, "nice": 0, "interrupt": 0, "wait": 0, "softirq": 0, "steal": 0}
	sum := 0.0
	for k, w := range want {
		if math.Abs(got[k]-w) > 1e-9 {
			t.Errorf("%s = %v, want %v", k, got[k], w)
		}
		sum += got[k]
	}
	if math.Abs(sum-1) > 1e-9 {
		t.Errorf("states sum to %v", sum)
	}
	if _, ok := cpuShares(cur, cur); ok {
		t.Error("no movement should yield no shares")
	}
	if _, ok := cpuShares(cur, prev); ok {
		t.Error("counters going backwards should yield no shares")
	}
}

func TestParseLoadAvg(t *testing.T) {
	tests := []struct {
		in   string
		want float64
		ok   bool
	}{
		{"0.52 0.40 0.33 1/389 12345\n", 0.52, true},
		{"", 0, false},
		{"x y z", 0, false},
	}
	for _, tc := range tests {
		got, ok := parseLoadAvg(tc.in)
		if got != tc.want || ok != tc.ok {
			t.Errorf("%q: got %v %v", tc.in, got, ok)
		}
	}
}

func TestParseMemInfo(t *testing.T) {
	full := "MemTotal:       16384000 kB\nMemFree:         2048000 kB\nMemAvailable:    8192000 kB\nBuffers: 1 kB\nCached:          4096000 kB\n"
	got, ok := parseMemInfo(full)
	want := memInfo{total: 16384000 * 1024, free: 2048000 * 1024, available: 8192000 * 1024, cached: 4096000 * 1024}
	if !ok || got != want {
		t.Fatalf("got %+v %v", got, ok)
	}
	for name, in := range map[string]string{
		"no MemAvailable": "MemTotal: 100 kB\nMemFree: 10 kB\n",
		"empty":           "",
		"available>total": "MemTotal: 100 kB\nMemAvailable: 200 kB\n",
	} {
		if _, ok := parseMemInfo(in); ok {
			t.Errorf("%s: expected failure", name)
		}
	}
}

func TestParseStatmRSS(t *testing.T) {
	got, ok := parseStatmRSS("5000 1200 300 10 0 800 0\n", 4096)
	if !ok || got != 1200*4096 {
		t.Fatalf("got %v %v", got, ok)
	}
	if _, ok := parseStatmRSS("", 4096); ok {
		t.Error("empty should fail")
	}
}

func TestCPUUtilization(t *testing.T) {
	tests := []struct {
		name      string
		cpu, wall time.Duration
		cores     int
		want      float64
	}{
		{"half of one of two cores", 500 * time.Millisecond, time.Second, 2, 0.25},
		{"idle", 0, time.Second, 4, 0},
		{"clamped", 5 * time.Second, time.Second, 2, 1},
		{"no elapsed time", time.Second, 0, 2, 0},
		{"negative delta", -time.Second, time.Second, 2, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := cpuUtilization(tc.cpu, tc.wall, tc.cores); math.Abs(got-tc.want) > 1e-9 {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestHostMetricsOptOut(t *testing.T) {
	off, on := false, true
	tests := []struct {
		name string
		opts Options
		env  map[string]string
		want bool
	}{
		{"default on", Options{}, nil, true},
		{"option off", Options{HostMetrics: &off}, nil, false},
		{"SEENTICS env off", Options{}, map[string]string{"SEENTICS_HOST_METRICS": "false"}, false},
		{"OBSERVE env off", Options{}, map[string]string{"OBSERVE_HOST_METRICS": "false"}, false},
		{"option beats env", Options{HostMetrics: &on}, map[string]string{"SEENTICS_HOST_METRICS": "false"}, true},
		{"unparsable env keeps default", Options{}, map[string]string{"SEENTICS_HOST_METRICS": "maybe"}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SEENTICS_HOST_METRICS", "")
			t.Setenv("OBSERVE_HOST_METRICS", "")
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			if got := resolve(tc.opts).hostMetrics; got != tc.want {
				t.Fatalf("hostMetrics = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestRegisterHostMetricsReports(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	if err := registerHostMetrics(mp.Meter("test")); err != nil {
		t.Fatal(err)
	}
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			seen[m.Name] = true
		}
	}
	want := []string{"process.cpu.utilization", "process.memory.usage", "process.uptime"}
	if runtime.GOOS == "linux" {
		want = append(want, "system.memory.usage", "system.memory.utilization", "system.cpu.load_average.1m",
			"system.filesystem.usage", "system.filesystem.utilization")
	}
	// From a container these described the container, not the server.
	for _, gone := range []string{"system.network.io", "system.disk.io"} {
		if seen[gone] {
			t.Errorf("%s should not be reported", gone)
		}
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if g, ok := m.Data.(metricdata.Gauge[float64]); ok && m.Name == "system.filesystem.utilization" {
				for _, dp := range g.DataPoints {
					if v, _ := dp.Attributes.Value("mountpoint"); v.AsString() != "/" {
						t.Errorf("filesystem reported for %q, want only /", v.AsString())
					}
				}
			}
		}
	}
	for _, name := range want {
		if !seen[name] {
			t.Errorf("%s not reported (saw %v)", name, seen)
		}
	}
	if runtime.GOOS != "linux" && seen["system.cpu.utilization"] {
		t.Error("host metrics should be skipped off Linux")
	}
}

func TestRuntimeMetricsKeepProcessMemoryWithoutHostMetrics(t *testing.T) {
	for _, withMem := range []bool{true, false} {
		reader := sdkmetric.NewManualReader()
		mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
		if err := registerRuntimeMetrics(mp.Meter("test"), withMem); err != nil {
			t.Fatal(err)
		}
		var rm metricdata.ResourceMetrics
		_ = reader.Collect(context.Background(), &rm)
		found := false
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				found = found || m.Name == "process.memory.usage"
			}
		}
		if found != withMem {
			t.Errorf("withProcessMemory=%v but found=%v", withMem, found)
		}
	}
}
