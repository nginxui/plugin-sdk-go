package sdk

import (
	"crypto/rand"
	"encoding/hex"
	goruntime "runtime"
)

// pipePrefix starts the name of every named pipe the SDK opens on Windows.
const pipePrefix = `\\.\pipe\nginx-ui-plugin-`

// newPipeName returns a pipe name nobody can guess, so no other process can
// create it first.
func newPipeName() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return pipePrefix + hex.EncodeToString(b), nil
}

// defaultNetwork is where the transports listen: a named pipe on Windows, a
// Unix socket everywhere else.
func defaultNetwork() string {
	if goruntime.GOOS == "windows" {
		return "pipe"
	}
	return "unix"
}
