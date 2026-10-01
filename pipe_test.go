package sdk

import (
	"regexp"
	goruntime "runtime"
	"testing"

	"github.com/nginxui/plugin-sdk-go/protocol"
)

func TestNewPipeName(t *testing.T) {
	// The host accepts only a local pipe whose name starts with a letter or
	// digit and uses letters, digits, dots, dashes and underscores.
	valid := regexp.MustCompile(`^\\\\\.\\pipe\\[A-Za-z0-9][A-Za-z0-9._-]*$`)
	seen := map[string]bool{}
	for range 4 {
		name, err := newPipeName()
		if err != nil {
			t.Fatal(err)
		}
		if !valid.MatchString(name) || len(name) != len(pipePrefix)+32 {
			t.Fatalf("pipe name %q", name)
		}
		if seen[name] {
			t.Fatalf("pipe name %q repeats", name)
		}
		seen[name] = true
	}
}

func TestDefaultNetwork(t *testing.T) {
	want := "unix"
	if goruntime.GOOS == "windows" {
		want = "pipe"
	}
	if got := defaultNetwork(); got != want {
		t.Fatalf("defaultNetwork() = %q, want %q", got, want)
	}
}

func TestPipeIsAdvertisedWithItsToken(t *testing.T) {
	var res protocol.InitializeResult
	(&grpcTransport{pipe: `\\.\pipe\nginx-ui-plugin-1`, token: "t"}).advertise(&res)
	if res.RPCPipe != `\\.\pipe\nginx-ui-plugin-1` || res.RPCToken != "t" || res.RPCPort != 0 || res.RPCSocket != "" {
		t.Fatalf("grpc handshake = %+v", res)
	}

	res = protocol.InitializeResult{}
	(&httpTransport{pipe: `\\.\pipe\nginx-ui-plugin-2`}).advertise(&res)
	if res.HTTPPipe != `\\.\pipe\nginx-ui-plugin-2` || res.HTTPPort != 0 {
		t.Fatalf("http handshake = %+v", res)
	}
}
