//go:build windows

package sdk

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/Microsoft/go-winio"
	"github.com/nginxui/plugin-sdk-go/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func dialPipe(ctx context.Context, name string) (net.Conn, error) {
	return winio.DialPipeContext(ctx, name)
}

func TestGRPCOverAPipeRequiresTheToken(t *testing.T) {
	h := newGRPCHarness(t, Plugin{DNS01: grpcHandler{}}, shortTempDir(t))

	if h.init.RPCPipe == "" || len(h.init.RPCToken) < 32 || h.init.RPCPort != 0 || h.init.RPCSocket != "" {
		t.Fatalf("pipe handshake = %+v", h.init)
	}

	pipe := h.init.RPCPipe
	conn, err := grpc.NewClient("passthrough:///plugin",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(rawCodec{})),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return dialPipe(ctx, pipe) }),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = invoke(ctx, conn, "/nginxui.plugin.v1.Plugin/Ping", nil)
	if st, pe := pluginErrorOf(t, err); st.Code() != codes.Unauthenticated || pe.GetCode() != protocol.CodePermissionDenied {
		t.Fatalf("without the token: status %v, detail %v", st, pe)
	}

	authed := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+h.init.RPCToken)
	if _, err = invoke(authed, conn, "/nginxui.plugin.v1.Plugin/Ping", nil); err != nil {
		t.Fatalf("Ping with token: %v", err)
	}

	h.stop(t)
	if c, err := winio.DialPipe(pipe, nil); err == nil {
		_ = c.Close()
		t.Fatal("the pipe still accepts connections after stop")
	}
}

func TestHTTPOverAPipeRequiresTheSecret(t *testing.T) {
	h := newHTTPHarness(t, Plugin{HTTP: echoHandler()}, shortTempDir(t), WithoutGRPC())

	if h.init.HTTPPipe == "" || h.init.HTTPPort != 0 {
		t.Fatalf("pipe handshake = %+v", h.init)
	}

	pipe := h.init.HTTPPipe
	c := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) { return dialPipe(ctx, pipe) },
	}}
	get := func(header http.Header) (int, string) {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://plugin/whoami", nil)
		req.Header = header
		resp, err := c.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body)
	}

	if code, body := get(http.Header{HeaderUserID: {"1"}, HeaderUser: {"bob"}, HeaderPluginSecret: {testHTTPSecret}}); body != "1:bob" {
		t.Fatalf("whoami = %d %q", code, body)
	}
	if code, _ := get(http.Header{}); code != http.StatusUnauthorized {
		t.Fatalf("request without the secret = %d, want 401", code)
	}
}
