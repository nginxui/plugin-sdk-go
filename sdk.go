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
// The smallest plugin is a DNS01Handler handed to Serve:
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
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/0xJacky/nginx-ui-plugin-sdk-go/jsonrpc"
	"github.com/0xJacky/nginx-ui-plugin-sdk-go/protocol"
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

	// Configure receives the settings map on plugin.configure. Optional.
	Configure func(ctx context.Context, settings map[string]any) error

	// Shutdown is called on plugin.shutdown to finish in-flight work.
	// Optional. The host waits for the reply before sending plugin.exit.
	Shutdown func(ctx context.Context) error

	// Methods registers extra inbound methods, such as the cron targets named
	// in the manifest. Lifecycle and capability method names are reserved.
	Methods map[string]Handler

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
		plugin: p,
		opts:   newOptions(opts),
		conn:   conn,
		host:   newHost(conn, envInfo()),
		exitCh: make(chan struct{}),
	}

	currentHost.Store(rt.host)
	defer currentHost.CompareAndSwap(rt.host, nil)
	defer rt.stopGRPC()

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

	for name, h := range rt.plugin.Methods {
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
	if t := rt.startGRPC(); t != nil {
		t.advertise(&res)
	}
	return res, nil
}

// startGRPC starts the gRPC transport once. It returns nil when gRPC is
// disabled or could not start, in which case the plugin stays on stdio.
func (rt *runtime) startGRPC() *grpcTransport {
	if rt.opts.disableGRPC {
		return nil
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

	if rt.plugin.Shutdown != nil {
		if err := rt.plugin.Shutdown(WithHost(ctx, rt.host)); err != nil {
			return nil, err
		}
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
