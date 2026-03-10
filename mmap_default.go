//go:build !linux

package asyncloguploader

func mmapAlloc(size int) ([]byte, error) {
	return make([]byte, size), nil
}

func mmapFree(_ []byte) error {
	return nil
}
