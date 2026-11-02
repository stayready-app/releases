package secureudp

import (
	"errors"
	"runtime"
	"syscall"
)

// A single oversized datagram or an ICMP error from a peer must not terminate a
// shared UDP listener. WSA error values differ from Go's portable errno aliases.
func IsDatagramReadError(err error) bool {
	if runtime.GOOS == "windows" {
		return errors.Is(err, syscall.Errno(10040)) || errors.Is(err, syscall.Errno(10054)) || errors.Is(err, syscall.Errno(10061))
	}
	return errors.Is(err, syscall.EMSGSIZE) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET)
}
