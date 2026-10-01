//go:build !windows

package sdk

import (
	"errors"
	"net"
)

// listenPipe fails: named pipes exist only on Windows.
func listenPipe() (net.Listener, string, error) {
	return nil, "", errors.New("named pipes are only available on Windows")
}
