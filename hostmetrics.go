package observe

import (
	"context"
	"errors"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Process metrics, and the server's CPU, memory and root disk, read once per
// export interval with no user code. Only what reads true from inside a
// container as well as outside one: /proc/stat, /proc/loadavg and
// /proc/meminfo are not namespaced, and the root filesystem's space is the
// server's disk. Network counters are namespaced (a container sees only its
// own traffic) and other disks are only partly visible, so neither is
// reported. The host side parses /proc and calls statfs, so it reports on
// Linux only; elsewhere those instruments are simply never registered. Everything here is standard
// library — no gopsutil — and nothing returns an error from a callback: a
// file that cannot be read just means no data points that interval.
//
// Do not run this alongside the OpenTelemetry Collector's hostmetrics
// receiver on the same host: the series would be sent twice.

var startedAt = time.Now()

// cpuTimes are the cumulative jiffies of the aggregate "cpu" line in /proc/stat.
type cpuTimes struct {
	user, nice, system, idle, iowait, irq, softirq, steal uint64
}

func (c cpuTimes) total() uint64 {
	return c.user + c.nice + c.system + c.idle + c.iowait + c.irq + c.softirq + c.steal
}

// parseCPUStat reads the first "cpu" line of /proc/stat. guest time is already
// counted inside user, so it is left out of the total.
func parseCPUStat(s string) (cpuTimes, bool) {
	for _, line := range strings.Split(s, "\n") {
		f := strings.Fields(line)
		if len(f) < 9 || f[0] != "cpu" {
			continue
		}
		v := make([]uint64, 8)
		for i := range v {
			n, err := strconv.ParseUint(f[i+1], 10, 64)
			if err != nil {
				return cpuTimes{}, false
			}
			v[i] = n
		}
		return cpuTimes{v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7]}, true
	}
	return cpuTimes{}, false
}

// cpuShares is each state's share of total capacity over the interval
// between two readings, so the states sum to ~1. ok is false when the
// counters went backwards or did not move.
func cpuShares(prev, cur cpuTimes) (map[string]float64, bool) {
	pairs := []struct {
		name      string
		prev, cur uint64
	}{
		{"user", prev.user, cur.user}, {"system", prev.system, cur.system},
		{"idle", prev.idle, cur.idle}, {"nice", prev.nice, cur.nice},
		{"interrupt", prev.irq, cur.irq}, {"wait", prev.iowait, cur.iowait},
		{"softirq", prev.softirq, cur.softirq}, {"steal", prev.steal, cur.steal},
	}
	if cur.total() <= prev.total() {
		return nil, false
	}
	total := float64(cur.total() - prev.total())
	out := make(map[string]float64, len(pairs))
	for _, p := range pairs {
		if p.cur < p.prev {
			return nil, false
		}
		out[p.name] = float64(p.cur-p.prev) / total
	}
	return out, true
}

// parseLoadAvg returns the 1-minute load average from /proc/loadavg.
func parseLoadAvg(s string) (float64, bool) {
	f := strings.Fields(s)
	if len(f) == 0 {
		return 0, false
	}
	v, err := strconv.ParseFloat(f[0], 64)
	return v, err == nil
}

type memInfo struct {
	total, free, available, cached uint64 // bytes
}

// parseMemInfo reads /proc/meminfo, whose values are in kB.
func parseMemInfo(s string) (memInfo, bool) {
	var m memInfo
	var haveTotal, haveAvail bool
	for _, line := range strings.Split(s, "\n") {
		key, rest, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		f := strings.Fields(rest)
		if len(f) == 0 {
			continue
		}
		n, err := strconv.ParseUint(f[0], 10, 64)
		if err != nil {
			continue
		}
		n *= 1024
		switch key {
		case "MemTotal":
			m.total, haveTotal = n, true
		case "MemFree":
			m.free = n
		case "MemAvailable":
			m.available, haveAvail = n, true
		case "Cached":
			m.cached = n
		}
	}
	// Kernels before 3.14 have no MemAvailable; skip rather than guess.
	return m, haveTotal && haveAvail && m.total > 0 && m.available <= m.total
}

type fsUsage struct{ used, free, total uint64 }

// parseStatmRSS returns resident bytes from /proc/self/statm (field 2, pages).
func parseStatmRSS(s string, pageSize int) (uint64, bool) {
	f := strings.Fields(s)
	if len(f) < 2 {
		return 0, false
	}
	pages, err := strconv.ParseUint(f[1], 10, 64)
	return pages * uint64(pageSize), err == nil
}

// cpuUtilization is CPU seconds used over the wall time elapsed, as a share
// of all cores, clamped to 0..1.
func cpuUtilization(cpuDelta, wallDelta time.Duration, cores int) float64 {
	if wallDelta <= 0 || cpuDelta < 0 || cores < 1 {
		return 0
	}
	u := float64(cpuDelta) / float64(wallDelta) / float64(cores)
	if u > 1 {
		return 1
	}
	return u
}

func readProc(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return string(b)
}

// hostSampler holds the instruments and the previous readings the
// delta-based gauges need.
type hostSampler struct {
	mu          sync.Mutex
	prevCPU     cpuTimes
	havePrevCPU bool
	prevProcCPU time.Duration
	prevWall    time.Time

	procCPU, uptime, cpuUtil, load, memUtil, fsUtil metric.Float64ObservableGauge
	procMem, memUsage, fsUsage                      metric.Int64ObservableGauge
}

func registerHostMetrics(m metric.Meter) error {
	linux := runtime.GOOS == "linux"
	s := &hostSampler{prevWall: time.Now()}
	gauge := func(name, unit, desc string) (metric.Float64ObservableGauge, error) {
		return m.Float64ObservableGauge(name, metric.WithUnit(unit), metric.WithDescription(desc))
	}
	bytesGauge := func(name, desc string) (metric.Int64ObservableGauge, error) {
		return m.Int64ObservableGauge(name, metric.WithUnit("By"), metric.WithDescription(desc))
	}

	var errs []error
	keep := func(err error) { errs = append(errs, err) }
	var e error
	s.procCPU, e = gauge("process.cpu.utilization", "1", "CPU used by this process, as a share of all cores")
	keep(e)
	s.procMem, e = bytesGauge("process.memory.usage", "Resident memory of this process")
	keep(e)
	s.uptime, e = gauge("process.uptime", "s", "Seconds since the process started")
	keep(e)
	instruments := []metric.Observable{s.procCPU, s.procMem, s.uptime}

	if linux {
		s.cpuUtil, e = gauge("system.cpu.utilization", "1", "Share of total CPU capacity by state")
		keep(e)
		s.load, e = gauge("system.cpu.load_average.1m", "{thread}", "1 minute load average")
		keep(e)
		s.memUsage, e = bytesGauge("system.memory.usage", "Memory by state")
		keep(e)
		s.memUtil, e = gauge("system.memory.utilization", "1", "Share of memory by state")
		keep(e)
		s.fsUsage, e = bytesGauge("system.filesystem.usage", "Root filesystem space by state")
		keep(e)
		s.fsUtil, e = gauge("system.filesystem.utilization", "1", "Share of root filesystem space by state")
		keep(e)
		instruments = append(instruments, s.cpuUtil, s.load, s.memUsage, s.memUtil, s.fsUsage, s.fsUtil)
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}

	s.prevProcCPU, _ = processCPUTime()
	if linux {
		s.prevCPU, s.havePrevCPU = parseCPUStat(readProc("/proc/stat"))
	}
	_, err := m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		s.observeProcess(o)
		if linux {
			s.observeHost(o)
		}
		return nil
	}, instruments...)
	return err
}

func (s *hostSampler) observeProcess(o metric.Observer) {
	now := time.Now()
	if cpuTime, ok := processCPUTime(); ok {
		s.mu.Lock()
		u := cpuUtilization(cpuTime-s.prevProcCPU, now.Sub(s.prevWall), runtime.NumCPU())
		s.prevProcCPU, s.prevWall = cpuTime, now
		s.mu.Unlock()
		o.ObserveFloat64(s.procCPU, u)
	}
	o.ObserveInt64(s.procMem, int64(processMemory()))
	o.ObserveFloat64(s.uptime, now.Sub(startedAt).Seconds())
}

func (s *hostSampler) observeHost(o metric.Observer) {
	if cur, ok := parseCPUStat(readProc("/proc/stat")); ok {
		s.mu.Lock()
		prev, had := s.prevCPU, s.havePrevCPU
		s.prevCPU, s.havePrevCPU = cur, true
		s.mu.Unlock()
		if had {
			if shares, ok := cpuShares(prev, cur); ok {
				for state, v := range shares {
					o.ObserveFloat64(s.cpuUtil, v, metric.WithAttributes(attribute.String("state", state)))
				}
			}
		}
	}
	if v, ok := parseLoadAvg(readProc("/proc/loadavg")); ok {
		o.ObserveFloat64(s.load, v)
	}
	if mi, ok := parseMemInfo(readProc("/proc/meminfo")); ok {
		used := mi.total - mi.available
		st := func(state string) metric.MeasurementOption {
			return metric.WithAttributes(attribute.String("state", state))
		}
		o.ObserveInt64(s.memUsage, int64(used), st("used"))
		o.ObserveInt64(s.memUsage, int64(mi.free), st("free"))
		o.ObserveInt64(s.memUsage, int64(mi.cached), st("cached"))
		o.ObserveFloat64(s.memUtil, float64(used)/float64(mi.total), st("used"))
		o.ObserveFloat64(s.memUtil, float64(mi.total-used)/float64(mi.total), st("free"))
	}
	// The root filesystem only. Inside a container "/" is an overlay whose space is the
	// server's own disk, so this is the server's disk either way; other mounts are only the
	// ones the container was given (bind-mounted files such as /sbin/docker-init among them).
	if u, ok := statFS("/"); ok && u.used+u.free > 0 {
		st := func(state string) metric.MeasurementOption {
			return metric.WithAttributes(attribute.String("mountpoint", "/"), attribute.String("state", state))
		}
		o.ObserveInt64(s.fsUsage, int64(u.used), st("used"))
		o.ObserveInt64(s.fsUsage, int64(u.free), st("free"))
		denom := float64(u.used + u.free)
		o.ObserveFloat64(s.fsUtil, float64(u.used)/denom, st("used"))
		o.ObserveFloat64(s.fsUtil, float64(u.free)/denom, st("free"))
	}
}

// processMemory is resident memory on Linux; elsewhere (or if /proc is
// unreadable) the bytes the Go runtime has obtained from the OS.
func processMemory() uint64 {
	if runtime.GOOS == "linux" {
		if rss, ok := parseStatmRSS(readProc("/proc/self/statm"), os.Getpagesize()); ok {
			return rss
		}
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.Sys
}
