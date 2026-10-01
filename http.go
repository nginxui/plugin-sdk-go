package sdk

// This file serves the http capability of a plugin whose manifest declares
// http.listen "unix": the SDK opens the listener the host proxies to, serves
// Plugin.HTTP on it and stops it on plugin.shutdown. The listener is up before
// the plugin.initialize reply is sent, so the first proxied request finds it.

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/nginxui/plugin-sdk-go/protocol"
)

// HTTPSocketName is the file name of the http capability socket inside the
// data directory. The host looks for it there and takes no other path.
const HTTPSocketName = "http.sock"

// Headers the host sets on every proxied request to identify the nginx-ui
// user behind it. The host removes any value the client sent for them.
const (
	HeaderUser   = "Nginx-UI-User"
	HeaderUserID = "Nginx-UI-User-ID"
)

// EnvPluginHTTPSecret is the environment variable that carries the secret the
// host generated for this process. The SDK reads it once at start and removes
// it from the environment, so child processes do not inherit it.
const EnvPluginHTTPSecret = "NGINX_UI_PLUGIN_HTTP_SECRET"

// HeaderPluginSecret is the request header that carries that secret. The SDK
// answers 401 to a request without the matching value and removes the header
// before it hands the request to Plugin.HTTP.
const HeaderPluginSecret = "Nginx-UI-Plugin-Secret"

// httpReadHeaderTimeout bounds how long a client may take to send the request
// headers.
const httpReadHeaderTimeout = 30 * time.Second

// httpShutdownGrace bounds how long plugin.shutdown waits for the requests
// still running before it closes their connections. The host waits only a few
// seconds for the reply, so this stays below that.
const httpShutdownGrace = 3 * time.Second

// UserFromRequest returns the nginx-ui user the host proxied a request for. It
// is the zero value for a request that did not come through the host.
func UserFromRequest(r *http.Request) protocol.HTTPUser {
	return protocol.HTTPUser{
		ID:   r.Header.Get(HeaderUserID),
		Name: r.Header.Get(HeaderUser),
	}
}

// httpTransport is the running HTTP listener of one plugin process.
type httpTransport struct {
	server   *http.Server
	listener net.Listener
	// socket is the Unix socket path, empty on a pipe or TCP.
	socket string
	// pipe is the named pipe on Windows.
	pipe string
	port int

	// shutdownDone is closed once the graceful shutdown returned.
	shutdownDone chan struct{}
	beginOnce    sync.Once
	closeOnce    sync.Once
}

// startHTTP opens the listener for h and starts serving in the background.
// network is "unix", "pipe" or "tcp", empty picks by platform.
func startHTTP(dataDir, network, secret string, h http.Handler) (*httpTransport, error) {
	if secret == "" {
		return nil, fmt.Errorf("%s is not set, the host provides it to every plugin serving the http capability", EnvPluginHTTPSecret)
	}
	if network == "" {
		network = defaultNetwork()
	}

	t := &httpTransport{shutdownDone: make(chan struct{})}
	var err error
	switch network {
	case "unix":
		err = t.listenUnix(dataDir)
	case "pipe":
		t.listener, t.pipe, err = listenPipe()
	case "tcp":
		err = t.listenTCP()
	default:
		err = fmt.Errorf("unknown network %q", network)
	}
	if err != nil {
		return nil, err
	}

	t.server = &http.Server{
		Handler:           requireSecret(secret, h),
		ReadHeaderTimeout: httpReadHeaderTimeout,
		ErrorLog:          log.New(loggerWriter{level: LevelWarn}, "http: ", 0),
	}
	go func() {
		if serveErr := t.server.Serve(t.listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			Logger.Errorf("HTTP server stopped: %v", serveErr)
		}
	}()
	return t, nil
}

func (t *httpTransport) listenUnix(dataDir string) error {
	if dataDir == "" {
		return errors.New("the plugin data directory is not set")
	}
	dir, err := filepath.Abs(dataDir)
	if err != nil {
		return fmt.Errorf("resolve the data directory: %w", err)
	}

	// The host reaches the socket at exactly this path, there is no fallback.
	path := filepath.Join(dir, HTTPSocketName)
	if limit := maxSocketPathLen(); len(path) > limit {
		return fmt.Errorf("socket path %s is longer than %d bytes", path, limit)
	}
	if err = os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create the data directory: %w", err)
	}

	// A crashed predecessor may have left its socket behind.
	if fi, statErr := os.Lstat(path); statErr == nil && fi.Mode()&os.ModeSocket != 0 {
		if err = os.Remove(path); err != nil {
			return fmt.Errorf("remove the stale socket: %w", err)
		}
	}

	l, err := net.Listen("unix", path)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", path, err)
	}
	// Only the user of the host process may talk to the plugin.
	if err = os.Chmod(path, 0o600); err != nil {
		Logger.Warnf("Could not restrict the permissions of %s: %v", path, err)
	}
	t.listener = l
	t.socket = path
	return nil
}

func (t *httpTransport) listenTCP() error {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("listen on a loopback port: %w", err)
	}
	t.listener = l
	t.port = l.Addr().(*net.TCPAddr).Port
	return nil
}

// requireSecret refuses every request that does not carry the secret of the
// host, WebSocket upgrades included, and hides the header from h. The
// comparison takes the same time whatever the value, and the secret is never
// written to a log or a response.
func requireSecret(secret string, next http.Handler) http.Handler {
	want := sha256.Sum256([]byte(secret))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		values := r.Header.Values(HeaderPluginSecret)
		r.Header.Del(HeaderPluginSecret)

		ok := len(values) == 1
		if ok {
			got := sha256.Sum256([]byte(values[0]))
			ok = subtle.ConstantTimeCompare(got[:], want[:]) == 1
		}
		if !ok {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// takeEnv returns the value of an environment variable and removes it from the
// process environment.
func takeEnv(key string) string {
	value := os.Getenv(key)
	_ = os.Unsetenv(key)
	return value
}

// advertise adds the pipe or loopback port to the plugin.initialize reply. A
// Unix socket needs no entry: the host takes it from the data directory.
func (t *httpTransport) advertise(res *protocol.InitializeResult) {
	switch {
	case t.pipe != "":
		res.HTTPPipe = t.pipe
	case t.socket == "":
		res.HTTPPort = t.port
	}
}

// beginStop closes the listener and lets the requests still running finish.
// It returns at once, wait collects the result.
func (t *httpTransport) beginStop() {
	t.beginOnce.Do(func() {
		go func() {
			defer close(t.shutdownDone)
			_ = t.server.Shutdown(context.Background())
		}()
	})
}

// wait blocks until the graceful shutdown finished, ctx ended or the grace
// period elapsed, then closes what is left and removes the socket.
func (t *httpTransport) wait(ctx context.Context) {
	timer := time.NewTimer(httpShutdownGrace)
	defer timer.Stop()

	select {
	case <-t.shutdownDone:
	case <-ctx.Done():
	case <-timer.C:
	}
	t.close()
}

// close ends the server at once and removes the socket.
func (t *httpTransport) close() {
	t.closeOnce.Do(func() {
		_ = t.server.Close()
		if t.socket != "" {
			_ = os.Remove(t.socket)
		}
	})
}

// loggerWriter forwards what net/http writes to its error log to the SDK
// logger.
type loggerWriter struct{ level Level }

func (w loggerWriter) Write(p []byte) (int, error) {
	Logger.Log(w.level, strings.TrimSpace(string(p)), nil)
	return len(p), nil
}
