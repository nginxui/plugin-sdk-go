package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/0xJacky/nginx-ui-plugin-sdk-go/jsonrpc"
	pluginv1 "github.com/0xJacky/nginx-ui-plugin-sdk-go/pb"
	"github.com/0xJacky/nginx-ui-plugin-sdk-go/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

// grpcHandler implements every dns01 interface for the gRPC tests.
type grpcHandler struct{}

func (grpcHandler) Present(context.Context, DNS01Request) error { return nil }
func (grpcHandler) CleanUp(context.Context, DNS01Request) error { return nil }

func (grpcHandler) Validate(_ context.Context, _ string, config map[string]string) error {
	if config["TOKEN"] == "" {
		return InvalidConfig("TOKEN", "TOKEN is required")
	}
	return nil
}

func (grpcHandler) Options(_ context.Context, p protocol.DNS01OptionsParams) (protocol.DNS01OptionsResult, error) {
	if p.Provider == "panic" {
		panic("boom")
	}
	return protocol.DNS01OptionsResult{PropagationTimeoutSeconds: 120, PollingIntervalSeconds: 2}, nil
}

// grpcHarness runs a plugin over in-memory pipes and completes the handshake.
type grpcHarness struct {
	host    *jsonrpc.Conn
	init    protocol.InitializeResult
	cancel  context.CancelFunc
	runErr  chan error
	stopped bool
}

func newGRPCHarness(t *testing.T, p Plugin, dataDir string, opts ...Option) *grpcHarness {
	t.Helper()
	t.Setenv(EnvPluginDataDir, dataDir)

	pluginIn, hostW := io.Pipe()
	hostR, pluginOut := io.Pipe()

	h := &grpcHarness{host: jsonrpc.NewConn(hostR, hostW), runErr: make(chan error, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel

	go func() { _ = h.host.Serve(ctx) }()
	go func() { h.runErr <- Run(ctx, p, pluginIn, pluginOut, opts...) }()

	t.Cleanup(func() {
		h.stop(t)
		h.host.Close()
		_ = hostW.Close()
		_ = hostR.Close()
	})

	callCtx, callCancel := context.WithTimeout(ctx, 5*time.Second)
	defer callCancel()
	if err := h.host.Call(callCtx, protocol.MethodInitialize, protocol.InitializeParams{}, &h.init); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	if err := h.host.Notify(callCtx, protocol.MethodInitialized, nil); err != nil {
		t.Fatalf("initialized: %v", err)
	}
	return h
}

// stop ends Run and waits for it, which also stops the gRPC transport.
func (h *grpcHarness) stop(t *testing.T) {
	t.Helper()
	if h.stopped {
		return
	}
	h.stopped = true
	h.cancel()
	select {
	case <-h.runErr:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}
}

// dial connects to the advertised endpoint with the raw codec.
func (h *grpcHarness) dial(t *testing.T) *grpc.ClientConn {
	t.Helper()
	target := "unix://" + h.init.RPCSocket
	if h.init.RPCPort != 0 {
		target = "127.0.0.1:" + strconv.Itoa(h.init.RPCPort)
	}
	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(rawCodec{})),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// invoke sends one unary call with proto request bytes.
func invoke(ctx context.Context, conn *grpc.ClientConn, method string, req proto.Message) ([]byte, error) {
	var in []byte
	if req != nil {
		var err error
		if in, err = proto.Marshal(req); err != nil {
			return nil, err
		}
	}
	var out []byte
	err := conn.Invoke(ctx, method, &in, &out)
	return out, err
}

// pluginErrorOf returns the status and the PluginError detail of err.
func pluginErrorOf(t *testing.T, err error) (*status.Status, *pluginv1.PluginError) {
	t.Helper()
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("err = %v, want a gRPC status", err)
	}
	for _, d := range st.Details() {
		if pe, ok := d.(*pluginv1.PluginError); ok {
			return st, pe
		}
	}
	t.Fatalf("status %v has no PluginError detail", st)
	return nil, nil
}

// shortTempDir returns a directory whose socket path fits every platform.
func shortTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "sdkt")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func TestGRPCServesTheStdioHandlers(t *testing.T) {
	dataDir := shortTempDir(t)
	events := make(chan string, 1)
	h := newGRPCHarness(t, Plugin{
		DNS01: grpcHandler{},
		Methods: map[string]Handler{
			protocol.MethodEventsOn: func(_ context.Context, raw json.RawMessage) (any, error) {
				var ev protocol.EventNotification
				_ = json.Unmarshal(raw, &ev)
				events <- ev.Type
				return nil, nil
			},
		},
	}, dataDir, withGRPCNetwork("unix"))

	if got := h.init.Transports; len(got) != 2 || got[0] != protocol.TransportStdio || got[1] != protocol.TransportGRPC {
		t.Fatalf("transports = %v", got)
	}
	wantSocket := filepath.Join(dataDir, RPCSocketName)
	if abs, err := filepath.Abs(wantSocket); err == nil {
		wantSocket = abs
	}
	if h.init.RPCSocket != wantSocket {
		t.Fatalf("rpc_socket = %q, want %q", h.init.RPCSocket, wantSocket)
	}
	if h.init.RPCPort != 0 || h.init.RPCToken != "" {
		t.Fatalf("a Unix socket must not report a port or token: %+v", h.init)
	}

	conn := h.dial(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// A result travels as the response message.
	out, err := invoke(ctx, conn, "/nginxui.plugin.v1.DNS01/Options", &pluginv1.DNS01OptionsRequest{Provider: "demo"})
	if err != nil {
		t.Fatalf("Options: %v", err)
	}
	var options pluginv1.DNS01OptionsResponse
	if err = proto.Unmarshal(out, &options); err != nil {
		t.Fatal(err)
	}
	if options.GetPropagationTimeoutSeconds() != 120 || options.GetPollingIntervalSeconds() != 2 {
		t.Fatalf("options = %v", &options)
	}

	// A JSON-RPC error travels as a status with the PluginError detail.
	_, err = invoke(ctx, conn, "/nginxui.plugin.v1.DNS01/Validate", &pluginv1.DNS01ValidateRequest{Provider: "demo"})
	st, pe := pluginErrorOf(t, err)
	if st.Code() != codes.InvalidArgument || pe.GetCode() != protocol.CodeInvalidConfig || st.Message() != "TOKEN is required" {
		t.Fatalf("validate: status %v, detail %v", st, pe)
	}
	if pe.GetData().GetFields()["field"].GetStringValue() != "TOKEN" {
		t.Fatalf("detail data = %v", pe.GetData())
	}

	// A panic is an internal error, as on stdio.
	_, err = invoke(ctx, conn, "/nginxui.plugin.v1.DNS01/Options", &pluginv1.DNS01OptionsRequest{Provider: "panic"})
	st, pe = pluginErrorOf(t, err)
	if st.Code() != codes.Internal || pe.GetCode() != protocol.CodeInternalError {
		t.Fatalf("panic: status %v, detail %v", st, pe)
	}

	// plugin.ping is served so the host can probe the channel.
	if _, err = invoke(ctx, conn, "/nginxui.plugin.v1.Plugin/Ping", nil); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	// The handshake and the stop sequence stay on stdio.
	_, err = invoke(ctx, conn, "/nginxui.plugin.v1.Plugin/Exit", nil)
	st, pe = pluginErrorOf(t, err)
	if st.Code() != codes.Unimplemented || pe.GetCode() != protocol.CodeMethodNotFound {
		t.Fatalf("exit: status %v, detail %v", st, pe)
	}

	// A method outside the contract is unknown.
	_, err = invoke(ctx, conn, "/nginxui.plugin.v1.DNS01/DoesNotExist", nil)
	st, pe = pluginErrorOf(t, err)
	if st.Code() != codes.Unimplemented || pe.GetCode() != protocol.CodeMethodNotFound {
		t.Fatalf("unknown: status %v, detail %v", st, pe)
	}

	// A contract rpc without a handler answers like the stdio dispatcher.
	_, err = invoke(ctx, conn, "/nginxui.plugin.v1.HTTP/Handle", nil)
	st, pe = pluginErrorOf(t, err)
	if st.Code() != codes.Unimplemented || pe.GetCode() != protocol.CodeMethodNotFound || pe.GetMessage() != "unknown method: http.handle" {
		t.Fatalf("http.handle: status %v, detail %v", st, pe)
	}

	// Bytes that are no valid request message are invalid params.
	var in, reply []byte
	in = []byte{0xff, 0xff, 0xff}
	err = conn.Invoke(ctx, "/nginxui.plugin.v1.DNS01/Options", &in, &reply)
	st, pe = pluginErrorOf(t, err)
	if st.Code() != codes.InvalidArgument || pe.GetCode() != protocol.CodeInvalidParams {
		t.Fatalf("malformed: status %v, detail %v", st, pe)
	}

	// A notification rpc returns at once and still reaches its handler.
	if _, err = invoke(ctx, conn, "/nginxui.plugin.v1.Events/On", &pluginv1.EventsOnRequest{Type: "cert.renewed"}); err != nil {
		t.Fatalf("events.on: %v", err)
	}
	select {
	case got := <-events:
		if got != "cert.renewed" {
			t.Fatalf("event type = %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("events.on handler did not run")
	}

	h.stop(t)
	if _, err = os.Lstat(h.init.RPCSocket); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket still exists after stop: %v", err)
	}
}

func TestGRPCServesMCPCalls(t *testing.T) {
	h := newGRPCHarness(t, Plugin{MCP: MCPTools{
		"echo": func(_ context.Context, args map[string]any) (MCPResult, error) {
			paths, _ := args["paths"].([]any)
			return MCPText(fmt.Sprintf("%s:%d", args["zone"], len(paths))), nil
		},
	}}, shortTempDir(t), withGRPCNetwork("unix"))

	conn := h.dial(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// The Struct arguments reach the tool as a JSON object and the content
	// list comes back as repeated messages.
	args, err := structpb.NewStruct(map[string]any{"zone": "example.com", "paths": []any{"/a", "/b"}})
	if err != nil {
		t.Fatal(err)
	}
	out, err := invoke(ctx, conn, "/nginxui.plugin.v1.MCP/Call", &pluginv1.MCPCallRequest{Tool: "echo", Arguments: args})
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	var result pluginv1.MCPCallResponse
	if err = proto.Unmarshal(out, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.GetContent()) != 1 || result.GetContent()[0].GetText() != "example.com:2" || result.GetIsError() {
		t.Fatalf("result = %v", &result)
	}

	// An unknown tool is invalid params on gRPC as on stdio.
	_, err = invoke(ctx, conn, "/nginxui.plugin.v1.MCP/Call", &pluginv1.MCPCallRequest{Tool: "missing"})
	st, pe := pluginErrorOf(t, err)
	if st.Code() != codes.InvalidArgument || pe.GetCode() != protocol.CodeInvalidParams {
		t.Fatalf("unknown tool: status %v, detail %v", st, pe)
	}

	h.stop(t)
}

func TestGRPCSocketFallsBackForALongDataDir(t *testing.T) {
	base := shortTempDir(t)
	dataDir := filepath.Join(base, strings.Repeat("d", 120))
	h := newGRPCHarness(t, Plugin{DNS01: grpcHandler{}}, dataDir, withGRPCNetwork("unix"))

	if h.init.RPCSocket == "" || strings.HasPrefix(h.init.RPCSocket, dataDir) {
		t.Fatalf("rpc_socket = %q, want a fallback outside %s", h.init.RPCSocket, dataDir)
	}
	if len(h.init.RPCSocket) > maxSocketPathLen() {
		t.Fatalf("rpc_socket %q is longer than %d bytes", h.init.RPCSocket, maxSocketPathLen())
	}

	conn := h.dial(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := invoke(ctx, conn, "/nginxui.plugin.v1.Plugin/Ping", nil); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	h.stop(t)
	if _, err := os.Stat(filepath.Dir(h.init.RPCSocket)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("fallback directory still exists after stop: %v", err)
	}
}

func TestGRPCOverTCPRequiresTheToken(t *testing.T) {
	h := newGRPCHarness(t, Plugin{DNS01: grpcHandler{}}, shortTempDir(t), withGRPCNetwork("tcp"))

	if h.init.RPCPort == 0 || len(h.init.RPCToken) < 32 || h.init.RPCSocket != "" {
		t.Fatalf("tcp handshake = %+v", h.init)
	}

	conn := h.dial(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for name, callCtx := range map[string]context.Context{
		"no token":    ctx,
		"wrong token": metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer nope"),
		"no bearer":   metadata.AppendToOutgoingContext(ctx, "authorization", h.init.RPCToken),
	} {
		_, err := invoke(callCtx, conn, "/nginxui.plugin.v1.Plugin/Ping", nil)
		st, pe := pluginErrorOf(t, err)
		if st.Code() != codes.Unauthenticated || pe.GetCode() != protocol.CodePermissionDenied {
			t.Fatalf("%s: status %v, detail %v", name, st, pe)
		}
	}

	authed := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+h.init.RPCToken)
	if _, err := invoke(authed, conn, "/nginxui.plugin.v1.Plugin/Ping", nil); err != nil {
		t.Fatalf("Ping with token: %v", err)
	}
}

func TestGRPCCanBeDisabled(t *testing.T) {
	t.Run("option", func(t *testing.T) {
		dataDir := shortTempDir(t)
		h := newGRPCHarness(t, Plugin{DNS01: grpcHandler{}}, dataDir, WithoutGRPC())
		assertStdioOnly(t, h.init, dataDir)
	})
	t.Run("environment", func(t *testing.T) {
		t.Setenv(EnvDisableGRPC, "1")
		dataDir := shortTempDir(t)
		h := newGRPCHarness(t, Plugin{DNS01: grpcHandler{}}, dataDir)
		assertStdioOnly(t, h.init, dataDir)
	})
}

func assertStdioOnly(t *testing.T, init protocol.InitializeResult, dataDir string) {
	t.Helper()
	if len(init.Transports) != 1 || init.Transports[0] != protocol.TransportStdio {
		t.Fatalf("transports = %v", init.Transports)
	}
	if init.RPCSocket != "" || init.RPCPort != 0 || init.RPCToken != "" {
		t.Fatalf("stdio only handshake reports an endpoint: %+v", init)
	}
	if _, err := os.Lstat(filepath.Join(dataDir, RPCSocketName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket exists although gRPC is disabled: %v", err)
	}
}

func TestRawCodecRoundTrip(t *testing.T) {
	codec := rawCodec{}
	if codec.Name() != "proto" {
		t.Fatalf("name = %q, the wire must keep the standard proto subtype", codec.Name())
	}

	payload := []byte{1, 2, 3}
	out, err := codec.Marshal(&payload)
	if err != nil || string(out) != string(payload) {
		t.Fatalf("marshal = %v, %v", out, err)
	}

	var got []byte
	if err = codec.Unmarshal(payload, &got); err != nil || string(got) != string(payload) {
		t.Fatalf("unmarshal = %v, %v", got, err)
	}
	// The decoded bytes must not alias the transport buffer.
	payload[0] = 9
	if got[0] != 1 {
		t.Fatal("unmarshal aliases its input")
	}

	if _, err = codec.Marshal("text"); err == nil {
		t.Fatal("marshal of a non byte value succeeded")
	}
	if err = codec.Unmarshal(payload, new(string)); err == nil {
		t.Fatal("unmarshal into a non byte value succeeded")
	}
}

func TestGRPCStatusMapping(t *testing.T) {
	cases := []struct {
		name string
		err  error
		code codes.Code
		want int32
		data map[string]any
	}{
		{"invalid config", InvalidConfig("TOKEN", "bad"), codes.InvalidArgument, protocol.CodeInvalidConfig, map[string]any{"field": "TOKEN"}},
		{"invalid params", InvalidParams("bad"), codes.InvalidArgument, protocol.CodeInvalidParams, nil},
		{"unsupported", Unsupported("dns01.check"), codes.Unimplemented, protocol.CodeUnsupported, nil},
		{"not found", NewError(protocol.CodeMethodNotFound, "x", nil), codes.Unimplemented, protocol.CodeMethodNotFound, nil},
		{"permission", NewError(protocol.CodePermissionDenied, "x", nil), codes.PermissionDenied, protocol.CodePermissionDenied, nil},
		{"plain error", errors.New("vendor down"), codes.Internal, protocol.CodeInternalError, nil},
		{"method specific", NewError(-31000, "x", "scalar"), codes.Unknown, -31000, map[string]any{"value": "scalar"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, pe := pluginErrorOf(t, grpcStatus(tc.err, codes.OK))
			if st.Code() != tc.code || pe.GetCode() != tc.want || st.Message() != pe.GetMessage() {
				t.Fatalf("status %v, detail %v", st, pe)
			}
			var data map[string]any
			if pe.GetData() != nil {
				data = pe.GetData().AsMap()
			}
			if len(data) != len(tc.data) {
				t.Fatalf("data = %v, want %v", data, tc.data)
			}
			for k, v := range tc.data {
				if data[k] != v {
					t.Fatalf("data = %v, want %v", data, tc.data)
				}
			}
		})
	}

	st, _ := pluginErrorOf(t, grpcStatus(Internal("x"), codes.Unauthenticated))
	if st.Code() != codes.Unauthenticated {
		t.Fatalf("override ignored: %v", st)
	}
}

func TestRPCIndexCoversTheContract(t *testing.T) {
	index := rpcIndex()
	for full, want := range map[string]string{
		"/nginxui.plugin.v1.DNS01/Present": protocol.MethodDNS01Present,
		"/nginxui.plugin.v1.HTTP/Handle":   protocol.MethodHTTPHandle,
		"/nginxui.plugin.v1.Notify/Send":   protocol.MethodNotifySend,
		"/nginxui.plugin.v1.Probe/Check":   protocol.MethodProbeCheck,
		"/nginxui.plugin.v1.MCP/Call":      protocol.MethodMCPCall,
		"/nginxui.plugin.v1.Plugin/Ping":   protocol.MethodPing,
		"/nginxui.plugin.v1.Events/On":     protocol.MethodEventsOn,
		"/nginxui.plugin.v1.Host/KVGet":    protocol.MethodHostKVGet,
	} {
		rpc, ok := index[full]
		if !ok || rpc.name != want {
			t.Fatalf("%s resolves to %+v, want %s", full, rpc, want)
		}
	}
	if !index["/nginxui.plugin.v1.Events/On"].notification {
		t.Fatal("events.on is not marked as a notification")
	}

	// A Struct field survives the JSON round trip the handlers see.
	req := &pluginv1.DNS01OptionsRequest{Provider: "p", Options: &structpb.Struct{Fields: map[string]*structpb.Value{
		"credential_id": structpb.NewStringValue("7"),
	}}}
	in, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := decodeRequest(index["/nginxui.plugin.v1.DNS01/Options"].input, in)
	if err != nil {
		t.Fatal(err)
	}
	var params protocol.DNS01OptionsParams
	if err = json.Unmarshal(raw, &params); err != nil {
		t.Fatal(err)
	}
	if params.Provider != "p" || params.Options["credential_id"] != "7" {
		t.Fatalf("params = %+v", params)
	}
}
