//go:build linux

package observe

import "syscall"

// statFS reports a mounted filesystem's space. Used is what is allocated;
// free is what an unprivileged process can still use (Bavail), the figure
// `df` shows.
func statFS(path string) (fsUsage, bool) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return fsUsage{}, false
	}
	bs := uint64(st.Bsize)
	return fsUsage{
		total: uint64(st.Blocks) * bs,
		used:  (uint64(st.Blocks) - uint64(st.Bfree)) * bs,
		free:  uint64(st.Bavail) * bs,
	}, true
}
