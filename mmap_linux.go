//go:build linux

package asyncloguploader

import "golang.org/x/sys/unix"

func mmapAlloc(size int) ([]byte, error) {
	return unix.Mmap(-1, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANONYMOUS)
}

func mmapFree(data []byte) error {
	return unix.Munmap(data)
}
