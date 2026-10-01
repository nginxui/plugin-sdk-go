//go:build !windows

package sdk

import "testing"

func TestPipeNeedsWindows(t *testing.T) {
	if _, err := startHTTP(shortTempDir(t), "pipe", testHTTPSecret, echoHandler()); err == nil {
		t.Fatal("a named pipe opened outside Windows")
	}
}
