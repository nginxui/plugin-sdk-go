// Package sdk turns a Go program into an nginx-ui plugin.
//
// A plugin speaks bidirectional JSON-RPC 2.0 framed as NDJSON: the host writes
// requests and notifications on the plugin's stdin, the plugin writes replies
// and host.* calls on its stdout. stdout belongs to the protocol alone, so
// every human readable line must go to stderr; the SDK logger does that for
// you.
//
// By default the SDK also serves the same handlers over gRPC on a Unix socket
// in the data directory (a loopback port with a bearer token on Windows) and
// advertises it in the plugin.initialize reply, so the host can send
// capability calls there. WithoutGRPC or NGINX_UI_PLUGIN_DISABLE_GRPC=1 keep
// the plugin on stdio only.
//
// The smallest plugin is a capability handler handed to Serve, a
// DNS01Handler, NotifyHandler, ProbeHandler, MCPHandler, StorageHandler,
// DeployHandler, BlocklistHandler, DiscoveryHandler or LogSinkHandler, or an
// http.Handler for the http capability:
//
//	func main() {
//		sdk.Serve(sdk.Plugin{DNS01: &myHandler{}})
//	}
//
// Serve registers the lifecycle methods (plugin.initialize, plugin.initialized,
// plugin.configure, plugin.ping, plugin.shutdown, plugin.exit), wires the
// capability methods the handler implements, and exits on SIGTERM, SIGINT,
// plugin.exit or end of stdin.
package sdk

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nginxui/plugin-sdk-go/jsonrpc"
	"github.com/nginxui/plugin-sdk-go/protocol"
)

// ShutdownDrain bounds how long plugin.shutdown waits for the capability calls
// that are still running.
const ShutdownDrain = 30 * time.Second

// Environment variables the host sets on the plugin process.
const (
	EnvPluginID         = "NGINX_UI_PLUGIN_ID"
	EnvPluginAPIVersion = "NGINX_UI_PLUGIN_API_VERSION"
	EnvPluginDataDir    = "NGINX_UI_PLUGIN_DATA_DIR"
	EnvHostVersion      = "NGINX_UI_VERSION"
)

// Handler serves one inbound method. Returning a *protocol.Error sends that
// error verbatim; any other error becomes an internal error.
type Handler = jsonrpc.Handler

// Plugin declares what a plugin process implements.
type Plugin struct {
	// DNS01 serves the dns01 capability. Nil disables it.
	DNS01 DNS01Handler

	// HTTP serves the http capability with the manifest setting
	// http.listen "unix". The SDK listens on <data dir>/http.sock (mode 0600,
	// a stale socket is replaced), or on 127.0.0.1 with a free port reported
	// in http_port on Windows, before it answers plugin.initialize, and shuts
	// the server down gracefully on plugin.shutdown. The host removes its
	// credentials from the request and identifies the user with the headers
	// HeaderUser and HeaderUserID, see UserFromRequest. Every request has to
	// carry the per process secret of the host in HeaderPluginSecret, the SDK
	// answers 401 to any other and the handler never sees the header. The
	// secret comes from EnvPluginHTTPSecret, without it the handshake fails.
	// Nil disables it.
	HTTP http.Handler

	// Notify serves the notify capability. Nil disables it.
	Notify NotifyHandler

	// Probe serves the probe capability. Nil disables it.
	Probe ProbeHandler

	// MCP serves the mcp capability. Nil disables it. MCPTools is the
	// ready-made handler that dispatches by tool name.
	MCP MCPHandler

	// Storage serves the storage capability. Nil disables it.
	Storage StorageHandler

	// Deploy serves the cert.deploy capability. Nil disables it.
	Deploy DeployHandler

	// Blocklist serves the security.blocklist capability. Nil disables it.
	Blocklist BlocklistHandler

	// Discovery serves the upstream.discovery capability. Nil disables it.
	Discovery DiscoveryHandler

	// LogSink serves the log.sink capability: it receives the access log
	// lines of the host as a stream on the gRPC transport. Nil disables it.
	// Setting it keeps the gRPC transport on even under WithoutGRPC or
	// NGINX_UI_PLUGIN_DISABLE_GRPC=1, since the lines never travel on stdio.
	LogSink LogSinkHandler

	// Configure receives the settings map on plugin.configure. Optional.
	Configure func(ctx context.Context, settings map[string]any) error

	// Shutdown is called on plugin.shutdown to finish in-flight work.
	// Optional. The host waits for the reply before sending plugin.exit.
	Shutdown func(ctx context.Context) error

	// Methods registers extra inbound methods, such as the cron targets named
	// in the manifest. Lifecycle and capability method names are reserved.
	Methods map[string]Handler

	// Events handles the events the manifest subscribes to, keyed by event
	// type, for example protocol.EventLogPathsChanged. Other event types are
	// ignored. It takes over events.on, so Methods must not register it.
	Events map[string]EventHandler

	// Capabilities overrides the capability list reported in
	// InitializeResult. Empty means derive it from the handlers.
	Capabilities []string
}

// capabilities returns the declared list, or the one derived from the handlers.
func (p Plugin) capabilities() []string {
	if len(p.Capabilities) > 0 {
		return append([]string(nil), p.Capabilities...)
	}

	caps := []string{}
	if p.DNS01 != nil {
		caps = append(caps, protocol.CapabilityDNS01)
	}
	if p.HTTP != nil {
		caps = append(caps, protocol.CapabilityHTTP)
	}
	if p.Notify != nil {
		caps = append(caps, protocol.CapabilityNotify)
	}
	if p.Probe != nil {
		caps = append(caps, protocol.CapabilityProbe)
	}
	if p.MCP != nil {
		caps = append(caps, protocol.CapabilityMCP)
	}
	if p.Storage != nil {
		caps = append(caps, protocol.CapabilityStorage)
	}
	if p.Deploy != nil {
		caps = append(caps, protocol.CapabilityCertDeploy)
	}
	if p.Blocklist != nil {
		caps = append(caps, protocol.CapabilitySecurityBlocklist)
	}
	if p.Discovery != nil {
		caps = append(caps, protocol.CapabilityUpstreamDiscovery)
	}
	if p.LogSink != nil {
		caps = append(caps, protocol.CapabilityLogSink)
	}
	return caps
}

// Serve runs the plugin on stdin/stdout and never returns: it calls os.Exit
// once the host asked the process to stop, stdin ended or a signal arrived.
func Serve(p Plugin, opts ...Option) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := Run(ctx, p, os.Stdin, os.Stdout, opts...); err != nil {
		Logger.Errorf("plugin stopped: %v", err)
		os.Exit(1)
	}
	os.Exit(0)
}

// Run serves the plugin over r and w and returns when the host asked the
// process to stop, r ended or ctx was cancelled. It is the testable core of
// Serve and installs no signal handler. The gRPC transport, when enabled, is
// started on plugin.initialize and stopped before Run returns.
func Run(ctx context.Context, p Plugin, r io.Reader, w io.Writer, opts ...Option) error {
	conn := jsonrpc.NewConn(r, w)

	rt := &runtime{
		plugin:  p,
		opts:    newOptions(opts),
		conn:    conn,
		host:    newHost(conn, envInfo()),
		exitCh:  make(chan struct{}),
		streams: map[string]func() streamHandler{},

		// Taken now, whatever the plugin serves, so that no child process
		// ever inherits it.
		httpSecret: takeEnv(EnvPluginHTTPSecret),
	}

	currentHost.Store(rt.host)
	defer currentHost.CompareAndSwap(rt.host, nil)
	defer rt.stopGRPC()
	defer rt.closeHTTP()

	rt.register()

	serveCtx := WithHost(ctx, rt.host)

	errCh := make(chan error, 1)
	go func() { errCh <- conn.Serve(serveCtx) }()

	select {
	case <-rt.exitCh:
		conn.Close()
		return nil
	case err := <-errCh:
		return err
	case <-ctx.Done():
		conn.Close()
		return nil
	}
}

func envInfo() Info {
	apiVersion := protocol.APIVersion
	if raw := os.Getenv(EnvPluginAPIVersion); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil {
			apiVersion = v
		}
	}

	return Info{
		PluginID:    os.Getenv(EnvPluginID),
		APIVersion:  apiVersion,
		DataDir:     os.Getenv(EnvPluginDataDir),
		HostVersion: os.Getenv(EnvHostVersion),
	}
}

// runtime wires one Plugin onto one connection.
type runtime struct {
	plugin Plugin
	opts   options
	conn   *jsonrpc.Conn
	host   *Host

	// grpcMu guards grpc, the optional second transport.
	grpcMu sync.Mutex
	grpc   *grpcTransport

	// httpSecret is the secret every request to Plugin.HTTP has to carry.
	httpSecret string

	// httpMu guards httpT, the listener of Plugin.HTTP.
	httpMu sync.Mutex
	httpT  *httpTransport

	// streams opens the handler of every streaming rpc the plugin serves,
	// keyed by rpc name. It is filled by register and read only afterwards.
	streams map[string]func() streamHandler

	exitOnce sync.Once
	exitCh   chan struct{}

	// inflight counts the capability calls plugin.shutdown waits for.
	inflight atomic.Int64
}

// track wraps h so plugin.shutdown can wait for it.
func (rt *runtime) track(h Handler) Handler {
	return func(ctx context.Context, raw json.RawMessage) (any, error) {
		rt.inflight.Add(1)
		defer rt.inflight.Add(-1)
		return h(ctx, raw)
	}
}

// waitInflight blocks until the tracked calls returned, ctx ended or the drain
// window elapsed.
func (rt *runtime) waitInflight(ctx context.Context) {
	if rt.inflight.Load() == 0 {
		return
	}

	deadline := time.NewTimer(ShutdownDrain)
	defer deadline.Stop()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for rt.inflight.Load() > 0 {
		select {
		case <-ctx.Done():
			return
		case <-deadline.C:
			return
		case <-ticker.C:
		}
	}
}

func (rt *runtime) requestExit() {
	rt.exitOnce.Do(func() { close(rt.exitCh) })
}

func (rt *runtime) register() {
	rt.conn.Handle(protocol.MethodInitialize, rt.onInitialize)
	rt.conn.Handle(protocol.MethodInitialized, rt.onInitialized)
	rt.conn.Handle(protocol.MethodConfigure, rt.onConfigure)
	rt.conn.Handle(protocol.MethodPing, rt.onPing)
	rt.conn.Handle(protocol.MethodShutdown, rt.onShutdown)
	rt.conn.Handle(protocol.MethodExit, rt.onExit)

	if rt.plugin.DNS01 != nil {
		rt.conn.Handle(protocol.MethodDNS01Present, rt.track(rt.onDNS01Present))
		rt.conn.Handle(protocol.MethodDNS01Cleanup, rt.track(rt.onDNS01Cleanup))

		// The optional dns01 methods answer Unsupported when the handler does
		// not implement them, so the host can fall back to its own code.
		if v, ok := rt.plugin.DNS01.(DNS01Validator); ok {
			rt.conn.Handle(protocol.MethodDNS01Validate, rt.track(rt.dns01Validate(v)))
		} else {
			rt.conn.Handle(protocol.MethodDNS01Validate, unsupported(protocol.MethodDNS01Validate))
		}

		if o, ok := rt.plugin.DNS01.(DNS01OptionsProvider); ok {
			rt.conn.Handle(protocol.MethodDNS01Options, rt.track(rt.dns01Options(o)))
		} else {
			rt.conn.Handle(protocol.MethodDNS01Options, unsupported(protocol.MethodDNS01Options))
		}

		if c, ok := rt.plugin.DNS01.(DNS01Checker); ok {
			rt.conn.Handle(protocol.MethodDNS01Check, rt.track(rt.dns01Check(c)))
		} else {
			rt.conn.Handle(protocol.MethodDNS01Check, unsupported(protocol.MethodDNS01Check))
		}
	}

	if rt.plugin.Notify != nil {
		rt.registerNotify()
	}
	if rt.plugin.Probe != nil {
		rt.registerProbe()
	}
	if rt.plugin.MCP != nil {
		rt.registerMCP()
	}
	if rt.plugin.Storage != nil {
		rt.registerStorage()
	}
	if rt.plugin.Deploy != nil {
		rt.registerDeploy()
	}
	if rt.plugin.Blocklist != nil {
		rt.registerBlocklist()
	}
	if rt.plugin.Discovery != nil {
		rt.registerDiscovery()
	}
	if rt.plugin.LogSink != nil {
		rt.registerLogSink()
	}

	if len(rt.plugin.Events) > 0 {
		rt.conn.Handle(protocol.MethodEventsOn, EventsHandler(rt.plugin.Events))
	}

	for name, h := range rt.plugin.Methods {
		if isStreamingRPC(name) {
			// A streaming rpc has no JSON-RPC form, stdio keeps answering
			// method not found for it (spec WIRE-12).
			Logger.Warnf("ignoring the stdio handler for %s, a streaming rpc travels on gRPC only", name)
			continue
		}
		rt.conn.Handle(name, rt.track(h))
	}
}

func unsupported(method string) Handler {
	return func(context.Context, json.RawMessage) (any, error) {
		return nil, Unsupported(method)
	}
}

// decode unmarshals params, mapping a malformed payload to -32602.
func decode[T any](raw json.RawMessage) (T, error) {
	var out T
	if len(raw) == 0 || string(raw) == "null" {
		return out, nil
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, InvalidParams(err.Error())
	}
	return out, nil
}

func (rt *runtime) onInitialize(_ context.Context, raw json.RawMessage) (any, error) {
	params, err := decode[protocol.InitializeParams](raw)
	if err != nil {
		return nil, err
	}

	rt.host.setHandshake(params)

	res := protocol.InitializeResult{
		APIVersion:   protocol.APIVersion,
		Capabilities: rt.plugin.capabilities(),
		Transports:   []string{protocol.TransportStdio},
	}
	if rt.plugin.HTTP != nil {
		t, err := rt.startHTTP()
		if err != nil {
			return nil, Internal("http listener: " + err.Error())
		}
		t.advertise(&res)
	}
	if t := rt.startGRPC(); t != nil {
		t.advertise(&res)
	}
	return res, nil
}

// startHTTP opens the http capability listener once.
func (rt *runtime) startHTTP() (*httpTransport, error) {
	rt.httpMu.Lock()
	defer rt.httpMu.Unlock()
	if rt.httpT != nil {
		return rt.httpT, nil
	}

	t, err := startHTTP(rt.host.Info().DataDir, rt.opts.httpNetwork, rt.httpSecret, rt.plugin.HTTP)
	if err != nil {
		return nil, err
	}
	rt.httpT = t
	return t, nil
}

// currentHTTP returns the running http listener, nil when there is none.
func (rt *runtime) currentHTTP() *httpTransport {
	rt.httpMu.Lock()
	defer rt.httpMu.Unlock()
	return rt.httpT
}

// closeHTTP ends the http listener at once and removes its socket.
func (rt *runtime) closeHTTP() {
	rt.httpMu.Lock()
	t := rt.httpT
	rt.httpT = nil
	rt.httpMu.Unlock()
	if t != nil {
		t.close()
	}
}

// startGRPC starts the gRPC transport once. It returns nil when gRPC is
// disabled or could not start, in which case the plugin stays on stdio.
func (rt *runtime) startGRPC() *grpcTransport {
	if rt.opts.disableGRPC {
		if rt.plugin.LogSink == nil {
			return nil
		}
		Logger.Warnf("the log.sink capability needs the gRPC transport, serving it although it was disabled")
	}

	rt.grpcMu.Lock()
	defer rt.grpcMu.Unlock()
	if rt.grpc != nil {
		return rt.grpc
	}

	t, err := startGRPC(rt, rt.opts.grpcNetwork)
	if err != nil {
		Logger.Warnf("gRPC transport unavailable, serving stdio only: %v", err)
		return nil
	}
	rt.grpc = t
	return t
}

// stopGRPC stops the gRPC transport and removes its socket.
func (rt *runtime) stopGRPC() {
	rt.grpcMu.Lock()
	t := rt.grpc
	rt.grpc = nil
	rt.grpcMu.Unlock()
	if t != nil {
		t.stop()
	}
}

// onInitialized is a notification: its return values are never sent.
func (rt *runtime) onInitialized(context.Context, json.RawMessage) (any, error) {
	rt.host.ready.Store(true)
	return nil, nil
}

func (rt *runtime) onConfigure(ctx context.Context, raw json.RawMessage) (any, error) {
	params, err := decode[protocol.ConfigureParams](raw)
	if err != nil {
		return nil, err
	}

	rt.host.setSettings(params.Settings)

	if rt.plugin.Configure != nil {
		if err := rt.plugin.Configure(WithHost(ctx, rt.host), rt.host.Settings()); err != nil {
			return nil, err
		}
	}
	return protocol.EmptyResult{}, nil
}

func (rt *runtime) onPing(context.Context, json.RawMessage) (any, error) {
	return protocol.EmptyResult{}, nil
}

func (rt *runtime) onShutdown(ctx context.Context, _ json.RawMessage) (any, error) {
	// Answer only once the capability calls still in flight are done.
	rt.waitInflight(ctx)

	// Stop taking new http requests first. The requests still running get
	// until Plugin.Shutdown returned, which may unblock the long lived ones,
	// plus a short grace period.
	httpT := rt.currentHTTP()
	if httpT != nil {
		httpT.beginStop()
	}

	var err error
	if rt.plugin.Shutdown != nil {
		err = rt.plugin.Shutdown(WithHost(ctx, rt.host))
	}
	if httpT != nil {
		httpT.wait(ctx)
	}
	if err != nil {
		return nil, err
	}
	return protocol.EmptyResult{}, nil
}

// onExit is a notification: the process stops without answering.
func (rt *runtime) onExit(context.Context, json.RawMessage) (any, error) {
	rt.requestExit()
	return nil, nil
}

func (rt *runtime) onDNS01Present(ctx context.Context, raw json.RawMessage) (any, error) {
	req, err := decode[DNS01Request](raw)
	if err != nil {
		return nil, err
	}
	if err := rt.plugin.DNS01.Present(WithHost(ctx, rt.host), req); err != nil {
		return nil, err
	}
	return protocol.EmptyResult{}, nil
}

func (rt *runtime) onDNS01Cleanup(ctx context.Context, raw json.RawMessage) (any, error) {
	req, err := decode[DNS01Request](raw)
	if err != nil {
		return nil, err
	}
	if err := rt.plugin.DNS01.CleanUp(WithHost(ctx, rt.host), req); err != nil {
		return nil, err
	}
	return protocol.EmptyResult{}, nil
}

func (rt *runtime) dns01Validate(v DNS01Validator) Handler {
	return func(ctx context.Context, raw json.RawMessage) (any, error) {
		params, err := decode[protocol.DNS01ValidateParams](raw)
		if err != nil {
			return nil, err
		}
		if err := v.Validate(WithHost(ctx, rt.host), params.Provider, params.Config); err != nil {
			return nil, err
		}
		return protocol.EmptyResult{}, nil
	}
}

func (rt *runtime) dns01Options(o DNS01OptionsProvider) Handler {
	return func(ctx context.Context, raw json.RawMessage) (any, error) {
		params, err := decode[protocol.DNS01OptionsParams](raw)
		if err != nil {
			return nil, err
		}
		return o.Options(WithHost(ctx, rt.host), params)
	}
}

func (rt *runtime) dns01Check(c DNS01Checker) Handler {
	return func(ctx context.Context, raw json.RawMessage) (any, error) {
		params, err := decode[protocol.DNS01CheckParams](raw)
		if err != nil {
			return nil, err
		}
		return c.Check(WithHost(ctx, rt.host), params)
	}
}
