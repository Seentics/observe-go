//go:build !unix

package observe

import "time"

// No getrusage here; process.cpu.utilization is skipped.
func processCPUTime() (time.Duration, bool) { return 0, false }
