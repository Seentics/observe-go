//go:build !linux

package observe

// Filesystem metrics are Linux-only; this is never reached elsewhere.
func statFS(string) (fsUsage, bool) { return fsUsage{}, false }
