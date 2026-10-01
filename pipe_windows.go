//go:build windows

package sdk

import (
	"fmt"
	"net"

	"github.com/Microsoft/go-winio"
	"golang.org/x/sys/windows"
)

// listenPipe opens a named pipe under a fresh name. Only the user the plugin
// runs as, which is the user of the host, may connect, and clients on other
// machines are refused.
func listenPipe() (net.Listener, string, error) {
	name, err := newPipeName()
	if err != nil {
		return nil, "", err
	}
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return nil, "", fmt.Errorf("read the user of the process: %w", err)
	}
	l, err := winio.ListenPipe(name, &winio.PipeConfig{
		SecurityDescriptor: "D:P(A;;GA;;;" + user.User.Sid.String() + ")",
	})
	if err != nil {
		return nil, "", fmt.Errorf("listen on %s: %w", name, err)
	}
	return l, name, nil
}
