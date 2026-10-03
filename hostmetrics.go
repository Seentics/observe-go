package observe

import (
	"context"
	"errors"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Process and host metrics, read once per export interval so the Metrics
// page has CPU, memory, disk and network with no user code. The host side
// parses /proc and calls statfs, so it reports on Linux only; elsewhere those
// instruments are simply never registered. Everything here is standard
// library — no gopsutil — and nothing returns an error from a callback: a
// file that cannot be read just means no data points that interval.
//
// Do not run this alongside the OpenTelemetry Collector's hostmetrics
// receiver on the same host: the series would be sent twice.

const (
	maxFilesystems = 8  // bounds the mountpoint/device attribute cardinality
	maxNetDevices  = 16 // likewise for network interfaces
	maxDiskDevices = 16 // and block devices
)

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

type mount struct{ device, mountpoint, fstype string }

// Filesystems that are not storage anyone monitors.
var pseudoFS = map[string]bool{
	"proc": true, "sysfs": true, "tmpfs": true, "devtmpfs": true, "devpts": true,
	"overlay": true, "squashfs": true, "autofs": true, "mqueue": true, "hugetlbfs": true,
	"debugfs": true, "tracefs": true, "securityfs": true, "pstore": true, "bpf": true,
	"configfs": true, "fusectl": true, "binfmt_misc": true, "ramfs": true, "nsfs": true,
	"rpc_pipefs": true, "efivarfs": true, "fuse.lxcfs": true, "selinuxfs": true,
}

var skipMountPrefixes = []string{"/proc", "/sys", "/dev", "/run", "/snap", "/var/lib/docker", "/var/lib/containers", "/var/lib/kubelet"}

// Single-file bind mounts a container runtime adds on top of the real disk:
// /etc/hosts and friends, and `docker run --init`'s /sbin/docker-init.
var skipMountFiles = map[string]bool{"/etc/hosts": true, "/etc/hostname": true, "/etc/resolv.conf": true, "/sbin/docker-init": true}

// parseMounts reads /proc/mounts: "/" always, first, whatever its filesystem —
// in a container it is an overlay, and the space it reports is the disk the
// container writes to — then real block-device filesystems, one entry per
// device (bind mounts repeat it; the shortest mountpoint wins), in file order,
// at most maxFilesystems. Skipping "/" as an overlay left a container with no
// root disk, and its only "disk" a file bind mount like /sbin/docker-init.
func parseMounts(s string) []mount {
	out := []mount{{mountpoint: "/"}}
	index := map[string]int{}
	for _, line := range strings.Split(s, "\n") {
		f := strings.Fields(line)
		if len(f) < 3 {
			continue
		}
		m := mount{device: unescapeMount(f[0]), mountpoint: unescapeMount(f[1]), fstype: f[2]}
		if m.mountpoint == "/" {
			if strings.HasPrefix(m.device, "/") {
				out[0] = m
				index[m.device] = 0
			} else {
				out[0].fstype = m.fstype
			}
			continue
		}
		if pseudoFS[m.fstype] || strings.HasPrefix(m.fstype, "cgroup") || !strings.HasPrefix(m.device, "/") ||
			strings.HasPrefix(m.device, "/dev/loop") || skipMountFiles[m.mountpoint] {
			continue
		}
		skip := false
		for _, p := range skipMountPrefixes {
			if m.mountpoint == p || strings.HasPrefix(m.mountpoint, p+"/") {
				skip = true
				break
			}
		}
		if skip {
			continue
		}
		if i, seen := index[m.device]; seen {
			if len(m.mountpoint) < len(out[i].mountpoint) {
				out[i] = m
			}
			continue
		}
		if len(out) >= maxFilesystems {
			continue
		}
		index[m.device] = len(out)
		out = append(out, m)
	}
	return out
}

// unescapeMount decodes the octal escapes /proc/mounts uses for spaces.
func unescapeMount(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
			if n, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(n))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

type fsUsage struct{ used, free, total uint64 }

type ioCounters struct {
	device string
	a, b   uint64 // receive/transmit, or read/write, in bytes
}

// Container and virtual interfaces would multiply series for no insight.
var skipNetPrefixes = []string{"lo", "veth", "docker", "br-", "cni", "flannel", "cali", "virbr"}

// parseNetDev reads /proc/net/dev: receive bytes is the first counter after
// the interface name, transmit bytes the ninth.
func parseNetDev(s string) []ioCounters {
	var out []ioCounters
	for _, line := range strings.Split(s, "\n") {
		name, rest, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		name = strings.TrimSpace(name)
		skip := name == ""
		for _, p := range skipNetPrefixes {
			if strings.HasPrefix(name, p) {
				skip = true
			}
		}
		f := strings.Fields(rest)
		if skip || len(f) < 9 || len(out) >= maxNetDevices {
			continue
		}
		rx, err1 := strconv.ParseUint(f[0], 10, 64)
		tx, err2 := strconv.ParseUint(f[8], 10, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		out = append(out, ioCounters{name, rx, tx})
	}
	return out
}

// Whole disks only: partitions (sda1, nvme0n1p1) would double count, and
// loop, ram and device-mapper devices are not hardware.
var wholeDisk = regexp.MustCompile(`^(sd[a-z]+|vd[a-z]+|xvd[a-z]+|hd[a-z]+|nvme[0-9]+n[0-9]+|mmcblk[0-9]+)$`)

// parseDiskStats reads /proc/diskstats; sector counts are always 512 bytes.
func parseDiskStats(s string) []ioCounters {
	var out []ioCounters
	for _, line := range strings.Split(s, "\n") {
		f := strings.Fields(line)
		if len(f) < 10 || !wholeDisk.MatchString(f[2]) || len(out) >= maxDiskDevices {
			continue
		}
		rd, err1 := strconv.ParseUint(f[5], 10, 64)
		wr, err2 := strconv.ParseUint(f[9], 10, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		out = append(out, ioCounters{f[2], rd * 512, wr * 512})
	}
	return out
}

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
	netIO, diskIO                                   metric.Int64ObservableCounter
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
	bytesCounter := func(name, desc string) (metric.Int64ObservableCounter, error) {
		return m.Int64ObservableCounter(name, metric.WithUnit("By"), metric.WithDescription(desc))
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
		s.fsUsage, e = bytesGauge("system.filesystem.usage", "Filesystem space by state")
		keep(e)
		s.fsUtil, e = gauge("system.filesystem.utilization", "1", "Share of filesystem space by state")
		keep(e)
		s.netIO, e = bytesCounter("system.network.io", "Bytes through network interfaces")
		keep(e)
		s.diskIO, e = bytesCounter("system.disk.io", "Bytes read from and written to disks")
		keep(e)
		instruments = append(instruments, s.cpuUtil, s.load, s.memUsage, s.memUtil, s.fsUsage, s.fsUtil, s.netIO, s.diskIO)
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
	for _, mt := range parseMounts(readProc("/proc/mounts")) {
		u, ok := statFS(mt.mountpoint)
		if !ok || u.total == 0 {
			continue
		}
		attr := func(state string) metric.MeasurementOption {
			// An overlay root has no device; the attribute is left off, as the Node SDK does.
			if mt.device == "" {
				return metric.WithAttributes(attribute.String("mountpoint", mt.mountpoint), attribute.String("state", state))
			}
			return metric.WithAttributes(attribute.String("mountpoint", mt.mountpoint),
				attribute.String("device", mt.device), attribute.String("state", state))
		}
		o.ObserveInt64(s.fsUsage, int64(u.used), attr("used"))
		o.ObserveInt64(s.fsUsage, int64(u.free), attr("free"))
		if denom := float64(u.used + u.free); denom > 0 {
			o.ObserveFloat64(s.fsUtil, float64(u.used)/denom, attr("used"))
			o.ObserveFloat64(s.fsUtil, float64(u.free)/denom, attr("free"))
		}
	}
	observeIO(o, s.netIO, parseNetDev(readProc("/proc/net/dev")), "receive", "transmit")
	observeIO(o, s.diskIO, parseDiskStats(readProc("/proc/diskstats")), "read", "write")
}

func observeIO(o metric.Observer, inst metric.Int64ObservableCounter, devs []ioCounters, dirA, dirB string) {
	for _, d := range devs {
		o.ObserveInt64(inst, int64(d.a), metric.WithAttributes(attribute.String("device", d.device), attribute.String("direction", dirA)))
		o.ObserveInt64(inst, int64(d.b), metric.WithAttributes(attribute.String("device", d.device), attribute.String("direction", dirB)))
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
