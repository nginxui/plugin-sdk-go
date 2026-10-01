package sdk

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nginxui/plugin-sdk-go/jsonrpc"
	"github.com/nginxui/plugin-sdk-go/protocol"
)

// echoHandler answers with the user the host proxied the request for.
func echoHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/whoami", func(w http.ResponseWriter, r *http.Request) {
		user := UserFromRequest(r)
		_, _ = io.WriteString(w, user.ID+":"+user.Name)
	})
	return mux
}

const testHTTPSecret = "test-secret-value"

// hostTransport adds the secret like the host does, unless the request
// already sets the header (to test a wrong one) or asks for none.
type hostTransport struct {
	base http.RoundTripper
	skip bool
}

func (h hostTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if _, set := r.Header[HeaderPluginSecret]; !set && !h.skip {
		r = r.Clone(r.Context())
		r.Header.Set(HeaderPluginSecret, testHTTPSecret)
	}
	return h.base.RoundTrip(r)
}

// unixClient talks to the socket the way the host does.
func unixClient(socket string) *http.Client {
	return &http.Client{Transport: hostTransport{base: unixTransport(socket)}}
}

func unixTransport(socket string) *http.Transport {
	return &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socket)
		},
	}
}

// newHTTPHarness starts a plugin with the secret the host would hand it.
func newHTTPHarness(t *testing.T, p Plugin, dataDir string, opts ...Option) *grpcHarness {
	t.Helper()
	t.Setenv(EnvPluginHTTPSecret, testHTTPSecret)
	return newGRPCHarness(t, p, dataDir, opts...)
}

func httpGet(t *testing.T, c *http.Client, path string, header http.Header) string {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://plugin"+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range header {
		req.Header[k] = v
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s = %d %s", path, resp.StatusCode, body)
	}
	return string(body)
}

func TestHTTPServesOnTheDataDirSocket(t *testing.T) {
	dataDir := shortTempDir(t)
	h := newHTTPHarness(t, Plugin{HTTP: echoHandler()}, dataDir, WithoutGRPC(), withHTTPNetwork("unix"))

	if len(h.init.Capabilities) != 1 || h.init.Capabilities[0] != protocol.CapabilityHTTP {
		t.Fatalf("capabilities = %v, want [http]", h.init.Capabilities)
	}
	if h.init.HTTPPort != 0 {
		t.Fatalf("http_port = %d on a unix socket", h.init.HTTPPort)
	}

	socket := filepath.Join(dataDir, HTTPSocketName)
	fi, err := os.Lstat(socket)
	if err != nil {
		t.Fatalf("socket: %v", err)
	}
	if fi.Mode()&os.ModeSocket == 0 || fi.Mode().Perm() != 0o600 {
		t.Fatalf("socket mode = %v, want a socket with 0600", fi.Mode())
	}

	header := http.Header{HeaderUserID: {"7"}, HeaderUser: {"alice"}}
	if got := httpGet(t, unixClient(socket), "/whoami", header); got != "7:alice" {
		t.Fatalf("whoami = %q", got)
	}

	h.stop(t)
	if _, err = os.Lstat(socket); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket still exists after stop: %v", err)
	}
}

func TestHTTPReplacesAStaleSocket(t *testing.T) {
	dataDir := shortTempDir(t)
	socket := filepath.Join(dataDir, HTTPSocketName)

	// A listener that keeps its file on close is what a crashed process
	// leaves behind.
	stale, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	stale.(*net.UnixListener).SetUnlinkOnClose(false)
	_ = stale.Close()
	if _, err = os.Lstat(socket); err != nil {
		t.Fatalf("stale socket was not left behind: %v", err)
	}

	newHTTPHarness(t, Plugin{HTTP: echoHandler()}, dataDir, WithoutGRPC(), withHTTPNetwork("unix"))
	if got := httpGet(t, unixClient(socket), "/whoami", nil); got != ":" {
		t.Fatalf("whoami = %q", got)
	}
}

func TestHTTPReportsALoopbackPort(t *testing.T) {
	dataDir := shortTempDir(t)
	h := newHTTPHarness(t, Plugin{HTTP: echoHandler()}, dataDir, WithoutGRPC(), withHTTPNetwork("tcp"))

	if h.init.HTTPPort == 0 {
		t.Fatalf("http_port is not set: %+v", h.init)
	}
	if _, err := os.Lstat(filepath.Join(dataDir, HTTPSocketName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a socket exists next to a loopback port: %v", err)
	}

	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(h.init.HTTPPort))
	c := &http.Client{}
	header := http.Header{HeaderUserID: {"1"}, HeaderUser: {"bob"}, HeaderPluginSecret: {testHTTPSecret}}
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+addr+"/whoami", nil)
	req.Header = header
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if string(body) != "1:bob" {
		t.Fatalf("whoami = %q", body)
	}

	// The loopback port needs the secret as well.
	resp, err = c.Get("http://" + addr + "/whoami")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("request without the secret = %d, want 401", resp.StatusCode)
	}

	h.stop(t)
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err == nil {
		_ = conn.Close()
		t.Fatal("the port still accepts connections after stop")
	}
}

func TestHTTPServesNextToGRPC(t *testing.T) {
	dataDir := shortTempDir(t)
	h := newHTTPHarness(t, Plugin{DNS01: grpcHandler{}, HTTP: echoHandler()}, dataDir,
		withGRPCNetwork("unix"), withHTTPNetwork("unix"))

	if h.init.RPCSocket == "" {
		t.Fatalf("rpc_socket missing: %+v", h.init)
	}
	if got := httpGet(t, unixClient(filepath.Join(dataDir, HTTPSocketName)), "/whoami", nil); got != ":" {
		t.Fatalf("whoami = %q", got)
	}
	caps := strings.Join(h.init.Capabilities, ",")
	if caps != "dns01,http" {
		t.Fatalf("capabilities = %q", caps)
	}
}

func TestHTTPListenFailureFailsTheHandshake(t *testing.T) {
	cases := map[string]string{
		"no data dir":    "",
		"path too long":  filepath.Join(shortTempDir(t), strings.Repeat("d", 120)),
		"unwritable dir": "/dev/null/data",
	}
	for name, dataDir := range cases {
		t.Run(name, func(t *testing.T) {
			t.Setenv(EnvPluginDataDir, dataDir)
			t.Setenv(EnvPluginHTTPSecret, testHTTPSecret)

			pluginIn, hostW := io.Pipe()
			hostR, pluginOut := io.Pipe()
			host := jsonrpc.NewConn(hostR, hostW)

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			go func() { _ = host.Serve(ctx) }()
			done := make(chan struct{})
			go func() {
				defer close(done)
				_ = Run(ctx, Plugin{HTTP: echoHandler()}, pluginIn, pluginOut, WithoutGRPC(), withHTTPNetwork("unix"))
			}()
			t.Cleanup(func() {
				cancel()
				host.Close()
				_ = hostW.Close()
				_ = hostR.Close()
				<-done
			})

			var res protocol.InitializeResult
			err := host.Call(ctx, protocol.MethodInitialize, protocol.InitializeParams{}, &res)
			var perr *protocol.Error
			if !errors.As(err, &perr) || perr.Code != protocol.CodeInternalError || !strings.Contains(perr.Message, "http listener") {
				t.Fatalf("initialize error = %v, want an internal http listener error", err)
			}
		})
	}
}

func TestHTTPShutdownLetsARunningRequestFinish(t *testing.T) {
	dataDir := shortTempDir(t)
	started := make(chan struct{})
	release := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/slow", func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		_, _ = io.WriteString(w, "done")
	})

	shutdownCalled := make(chan struct{})
	h := newHTTPHarness(t, Plugin{
		HTTP: mux,
		Shutdown: func(context.Context) error {
			close(shutdownCalled)
			return nil
		},
	}, dataDir, WithoutGRPC(), withHTTPNetwork("unix"))
	socket := filepath.Join(dataDir, HTTPSocketName)
	client := unixClient(socket)

	result := make(chan string, 1)
	go func() {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://plugin/slow", nil)
		resp, err := client.Do(req)
		if err != nil {
			result <- "error: " + err.Error()
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		result <- string(body)
	}()
	<-started

	shutdownDone := make(chan error, 1)
	go func() {
		var out protocol.EmptyResult
		shutdownDone <- h.host.Call(t.Context(), protocol.MethodShutdown, nil, &out)
	}()

	// The plugin hook ran, the reply waits for the request.
	select {
	case <-shutdownCalled:
	case <-time.After(5 * time.Second):
		t.Fatal("Plugin.Shutdown was not called")
	}
	select {
	case err := <-shutdownDone:
		t.Fatalf("shutdown answered before the request finished: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	// New connections are refused while the running request drains.
	if conn, err := net.DialTimeout("unix", socket, time.Second); err == nil {
		_ = conn.Close()
		t.Fatal("the socket accepts new connections during shutdown")
	}

	close(release)
	if got := <-result; got != "done" {
		t.Fatalf("running request = %q", got)
	}
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not answer")
	}
	if _, err := os.Lstat(socket); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket still exists after shutdown: %v", err)
	}
}

func TestHTTPShutdownClosesARequestThatOutlivesTheGrace(t *testing.T) {
	dataDir := shortTempDir(t)
	started := make(chan struct{})
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	mux := http.NewServeMux()
	mux.HandleFunc("/hang", func(_ http.ResponseWriter, r *http.Request) {
		close(started)
		select {
		case <-block:
		case <-r.Context().Done():
		}
	})

	h := newHTTPHarness(t, Plugin{HTTP: mux}, dataDir, WithoutGRPC(), withHTTPNetwork("unix"))
	go func() {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://plugin/hang", nil)
		if resp, err := unixClient(filepath.Join(dataDir, HTTPSocketName)).Do(req); err == nil {
			_ = resp.Body.Close()
		}
	}()
	<-started

	begin := time.Now()
	var out protocol.EmptyResult
	if err := h.host.Call(t.Context(), protocol.MethodShutdown, nil, &out); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if took := time.Since(begin); took > httpShutdownGrace+2*time.Second {
		t.Fatalf("shutdown took %v, want about %v", took, httpShutdownGrace)
	}
}

func TestUserFromRequest(t *testing.T) {
	r, _ := http.NewRequest(http.MethodGet, "http://plugin/", nil)
	if got := UserFromRequest(r); got != (protocol.HTTPUser{}) {
		t.Fatalf("user = %+v, want zero", got)
	}
	r.Header.Set("nginx-ui-user-id", "3")
	r.Header.Set("nginx-ui-user", "carol")
	if got := UserFromRequest(r); got.ID != "3" || got.Name != "carol" {
		t.Fatalf("user = %+v", got)
	}
}

func TestHTTPRejectsRequestsWithoutTheSecret(t *testing.T) {
	dataDir := shortTempDir(t)
	seen := make(chan http.Header, 4)
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		seen <- r.Header.Clone()
		_, _ = io.WriteString(w, "ok")
	})
	newHTTPHarness(t, Plugin{HTTP: mux}, dataDir, WithoutGRPC(), withHTTPNetwork("unix"))
	socket := filepath.Join(dataDir, HTTPSocketName)

	bare := &http.Client{Transport: hostTransport{base: unixTransport(socket), skip: true}}
	status := func(c *http.Client, header http.Header) (int, string) {
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://plugin/x", nil)
		for k, v := range header {
			req.Header[k] = v
		}
		resp, err := c.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body)
	}

	cases := map[string]http.Header{
		"missing":             nil,
		"empty":               {HeaderPluginSecret: {""}},
		"wrong":               {HeaderPluginSecret: {"nope"}},
		"prefix":              {HeaderPluginSecret: {testHTTPSecret[:5]}},
		"longer":              {HeaderPluginSecret: {testHTTPSecret + "x"}},
		"correct+wrong":       {HeaderPluginSecret: {testHTTPSecret, "nope"}},
		"in the wrong header": {"X-Other": {testHTTPSecret}},
	}
	for name, header := range cases {
		code, body := status(bare, header)
		if code != http.StatusUnauthorized {
			t.Fatalf("%s: status %d, want 401", name, code)
		}
		if strings.Contains(body, testHTTPSecret) {
			t.Fatalf("%s: the response leaks the secret", name)
		}
	}
	select {
	case <-seen:
		t.Fatal("a rejected request reached the handler")
	default:
	}

	// The right value gets through, and the handler never sees the header.
	code, body := status(bare, http.Header{HeaderPluginSecret: {testHTTPSecret}})
	if code != http.StatusOK || body != "ok" {
		t.Fatalf("authorized request = %d %q", code, body)
	}
	if got := (<-seen).Values(HeaderPluginSecret); len(got) != 0 {
		t.Fatalf("the handler saw the secret header: %v", got)
	}
}

func TestHTTPRejectsWebSocketUpgradesWithoutTheSecret(t *testing.T) {
	dataDir := shortTempDir(t)
	newHTTPHarness(t, Plugin{HTTP: echoHandler()}, dataDir, WithoutGRPC(), withHTTPNetwork("unix"))
	socket := filepath.Join(dataDir, HTTPSocketName)

	conn, err := net.Dial("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, _ = io.WriteString(conn, "GET /whoami HTTP/1.1\r\nHost: plugin\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n"+
		"Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n")
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil || !strings.Contains(line, "401") {
		t.Fatalf("upgrade answer = %q, %v, want 401", line, err)
	}
}

func TestHTTPNeedsTheSecretToStart(t *testing.T) {
	t.Setenv(EnvPluginDataDir, shortTempDir(t))
	t.Setenv(EnvPluginHTTPSecret, "")

	pluginIn, hostW := io.Pipe()
	hostR, pluginOut := io.Pipe()
	host := jsonrpc.NewConn(hostR, hostW)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	go func() { _ = host.Serve(ctx) }()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = Run(ctx, Plugin{HTTP: echoHandler()}, pluginIn, pluginOut, WithoutGRPC(), withHTTPNetwork("unix"))
	}()
	t.Cleanup(func() {
		cancel()
		host.Close()
		_ = hostW.Close()
		_ = hostR.Close()
		<-done
	})

	var res protocol.InitializeResult
	err := host.Call(ctx, protocol.MethodInitialize, protocol.InitializeParams{}, &res)
	var perr *protocol.Error
	if !errors.As(err, &perr) || !strings.Contains(perr.Message, EnvPluginHTTPSecret) {
		t.Fatalf("initialize error = %v, want one that names %s", err, EnvPluginHTTPSecret)
	}
}

func TestRunRemovesTheSecretFromTheEnvironment(t *testing.T) {
	t.Setenv(EnvPluginHTTPSecret, testHTTPSecret)
	// A plugin that serves no HTTP still must not leak the secret to children.
	newGRPCHarness(t, Plugin{DNS01: grpcHandler{}}, shortTempDir(t), WithoutGRPC())

	if _, set := os.LookupEnv(EnvPluginHTTPSecret); set {
		t.Fatal("the secret is still in the environment")
	}
}
