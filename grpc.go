package sdk

// This file serves the optional gRPC transport (spec/03-wire-protocol.md
// WIRE-11). Every gRPC call is resolved through the proto descriptors of the
// contract to its JSON-RPC method name and runs the exact handler the stdio
// dispatcher runs, so both transports answer identically. A client streaming
// rpc (WIRE-12) has no stdio form: it is read until the end of the stream
// and handed to its stream handler.

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"sync"
	"time"

	pluginv1 "github.com/0xJacky/nginx-ui-plugin-sdk-go/pb"
	"github.com/0xJacky/nginx-ui-plugin-sdk-go/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/structpb"
)

// EnvDisableGRPC set to "1" keeps the plugin on stdio only, like WithoutGRPC.
const EnvDisableGRPC = "NGINX_UI_PLUGIN_DISABLE_GRPC"

// RPCSocketName is the file name of the gRPC socket inside the data directory.
const RPCSocketName = "rpc.sock"

// MaxGRPCMessageBytes bounds one gRPC message in either direction. gRPC exists
// for payloads the 4 MiB stdio frame cannot carry, so it is larger.
const MaxGRPCMessageBytes = 64 << 20

// grpcStopTimeout bounds the graceful stop of the gRPC server on exit.
const grpcStopTimeout = 2 * time.Second

// stdioOnlyMethods drive the handshake and the stop sequence. They always
// travel on stdio, a gRPC call for one of them is refused.
var stdioOnlyMethods = map[string]bool{
	protocol.MethodInitialize:  true,
	protocol.MethodInitialized: true,
	protocol.MethodShutdown:    true,
	protocol.MethodExit:        true,
}

// Option configures Serve and Run.
type Option func(*options)

type options struct {
	disableGRPC bool
	// grpcNetwork forces "unix" or "tcp". Empty picks by platform.
	grpcNetwork string
}

// WithoutGRPC keeps the plugin on stdio only: plugin.initialize does not
// advertise the grpc transport and no listener is opened. Setting the
// environment variable NGINX_UI_PLUGIN_DISABLE_GRPC=1 has the same effect.
func WithoutGRPC() Option {
	return func(o *options) { o.disableGRPC = true }
}

// withGRPCNetwork forces the listener network, "unix" or "tcp". Tests use it
// to exercise the Windows loopback path on any platform.
func withGRPCNetwork(network string) Option {
	return func(o *options) { o.grpcNetwork = network }
}

func newOptions(opts []Option) options {
	var o options
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if os.Getenv(EnvDisableGRPC) == "1" {
		o.disableGRPC = true
	}
	return o
}

// rpcMethod is one rpc of the contract, resolved from the descriptors.
type rpcMethod struct {
	name         string
	fullMethod   string
	input        protoreflect.MessageDescriptor
	output       protoreflect.MessageDescriptor
	notification bool
	// streaming marks a client streaming rpc, which travels on gRPC only.
	streaming bool
}

// rpcIndex maps every gRPC full method of the contract to its rpc.
var rpcIndex = sync.OnceValue(func() map[string]*rpcMethod {
	index := map[string]*rpcMethod{}
	pkg := pluginv1.File_nginxui_plugin_v1_options_proto.Package()
	protoregistry.GlobalFiles.RangeFilesByPackage(pkg, func(fd protoreflect.FileDescriptor) bool {
		services := fd.Services()
		for i := range services.Len() {
			sd := services.Get(i)
			methods := sd.Methods()
			for j := range methods.Len() {
				md := methods.Get(j)
				name, _ := proto.GetExtension(md.Options(), pluginv1.E_RpcName).(string)
				if name == "" {
					continue
				}
				notification, _ := proto.GetExtension(md.Options(), pluginv1.E_Notification).(bool)
				streaming, _ := proto.GetExtension(md.Options(), pluginv1.E_Streaming).(bool)
				full := fmt.Sprintf("/%s/%s", sd.FullName(), md.Name())
				index[full] = &rpcMethod{
					name:         name,
					fullMethod:   full,
					input:        md.Input(),
					output:       md.Output(),
					notification: notification,
					streaming:    streaming || md.IsStreamingClient(),
				}
			}
		}
		return true
	})
	return index
})

// isStreamingRPC reports whether name is the rpc_name of a streaming rpc,
// which must never be registered as a stdio handler.
func isStreamingRPC(name string) bool {
	for _, rpc := range rpcIndex() {
		if rpc.name == name && rpc.streaming {
			return true
		}
	}
	return false
}

// streamHandler consumes one client stream of a streaming rpc.
type streamHandler interface {
	// add takes one request message, in protobuf encoding.
	add(ctx context.Context, in []byte) error
	// finish runs once the caller closed the stream and returns the
	// response message in protobuf encoding.
	finish(ctx context.Context) ([]byte, error)
}

// rawCodec hands the undecoded message bytes to the handler. The bytes are
// protobuf, so it keeps the standard "proto" content subtype on the wire.
type rawCodec struct{}

func (rawCodec) Name() string { return "proto" }

func (rawCodec) Marshal(v any) ([]byte, error) {
	switch m := v.(type) {
	case []byte:
		return m, nil
	case *[]byte:
		return *m, nil
	default:
		return nil, fmt.Errorf("sdk: raw codec cannot marshal %T", v)
	}
}

func (rawCodec) Unmarshal(data []byte, v any) error {
	p, ok := v.(*[]byte)
	if !ok {
		return fmt.Errorf("sdk: raw codec cannot unmarshal into %T", v)
	}
	*p = append((*p)[:0], data...)
	return nil
}

// grpcTransport is the running gRPC listener of one plugin process.
type grpcTransport struct {
	rt       *runtime
	server   *grpc.Server
	listener net.Listener
	// socket is the Unix socket path, empty on TCP.
	socket string
	// tmpDir is the fallback directory holding socket, removed on stop.
	tmpDir string
	port   int
	// token must be presented on TCP as "authorization: Bearer <token>".
	token string

	stopOnce sync.Once
}

// startGRPC opens the listener and starts serving in the background.
func startGRPC(rt *runtime, network string) (*grpcTransport, error) {
	if network == "" {
		network = "unix"
		if goruntime.GOOS == "windows" {
			network = "tcp"
		}
	}

	t := &grpcTransport{rt: rt}
	var err error
	switch network {
	case "unix":
		err = t.listenUnix(rt.host.Info().DataDir)
	case "tcp":
		err = t.listenTCP()
	default:
		err = fmt.Errorf("unknown network %q", network)
	}
	if err != nil {
		return nil, err
	}

	t.server = grpc.NewServer(
		grpc.UnknownServiceHandler(t.handle),
		grpc.ForceServerCodec(rawCodec{}),
		grpc.MaxRecvMsgSize(MaxGRPCMessageBytes),
		grpc.MaxSendMsgSize(MaxGRPCMessageBytes),
	)
	go func() {
		if serveErr := t.server.Serve(t.listener); serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			Logger.Warnf("gRPC transport stopped: %v", serveErr)
		}
	}()
	return t, nil
}

// maxSocketPathLen is the longest Unix socket path the platform accepts,
// sun_path minus its terminating NUL.
func maxSocketPathLen() int {
	switch goruntime.GOOS {
	case "darwin", "ios", "freebsd", "netbsd", "openbsd", "dragonfly":
		return 103
	default:
		return 107
	}
}

func (t *grpcTransport) listenUnix(dataDir string) error {
	limit := maxSocketPathLen()

	if dataDir != "" {
		if dir, err := filepath.Abs(dataDir); err == nil {
			path := filepath.Join(dir, RPCSocketName)
			if len(path) <= limit && os.MkdirAll(dir, 0o700) == nil && t.listenOn(path) == nil {
				return nil
			}
		}
	}

	// The data directory is unusable or its path too long: use a private
	// directory under the temp dir and report the path in rpc_socket.
	var lastErr error
	for _, base := range []string{os.TempDir(), "/tmp"} {
		dir, err := os.MkdirTemp(base, "nuip-")
		if err != nil {
			lastErr = err
			continue
		}
		path := filepath.Join(dir, RPCSocketName)
		if len(path) > limit {
			_ = os.RemoveAll(dir)
			lastErr = fmt.Errorf("socket path %s is longer than %d bytes", path, limit)
			continue
		}
		if err = t.listenOn(path); err != nil {
			_ = os.RemoveAll(dir)
			lastErr = err
			continue
		}
		t.tmpDir = dir
		return nil
	}
	if lastErr == nil {
		lastErr = errors.New("no usable socket directory")
	}
	return lastErr
}

func (t *grpcTransport) listenOn(path string) error {
	// A crashed predecessor may have left its socket behind.
	if fi, err := os.Lstat(path); err == nil && fi.Mode()&os.ModeSocket != 0 {
		_ = os.Remove(path)
	}
	l, err := net.Listen("unix", path)
	if err != nil {
		return err
	}
	_ = os.Chmod(path, 0o600)
	t.listener = l
	t.socket = path
	return nil
}

func (t *grpcTransport) listenTCP() error {
	token := make([]byte, 32)
	if _, err := rand.Read(token); err != nil {
		return err
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	t.listener = l
	t.port = l.Addr().(*net.TCPAddr).Port
	t.token = hex.EncodeToString(token)
	return nil
}

// advertise adds the transport to the plugin.initialize reply.
func (t *grpcTransport) advertise(res *protocol.InitializeResult) {
	res.Transports = append(res.Transports, protocol.TransportGRPC)
	if t.socket != "" {
		res.RPCSocket = t.socket
		return
	}
	res.RPCPort = t.port
	res.RPCToken = t.token
}

// stop ends the server, waiting a moment for calls still running, and removes
// the socket.
func (t *grpcTransport) stop() {
	t.stopOnce.Do(func() {
		done := make(chan struct{})
		go func() {
			t.server.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(grpcStopTimeout):
			t.server.Stop()
		}
		if t.socket != "" {
			_ = os.Remove(t.socket)
		}
		if t.tmpDir != "" {
			_ = os.RemoveAll(t.tmpDir)
		}
	})
}

// handle serves every gRPC method. It is the server's unknown service
// handler, so no service needs to be registered.
func (t *grpcTransport) handle(_ any, stream grpc.ServerStream) error {
	ctx := stream.Context()
	if t.token != "" && !validToken(ctx, t.token) {
		return grpcStatus(&protocol.Error{Code: protocol.CodePermissionDenied, Message: "missing or invalid rpc token"}, codes.Unauthenticated)
	}

	fullMethod, _ := grpc.MethodFromServerStream(stream)
	if rpc, ok := rpcIndex()[fullMethod]; ok && rpc.streaming {
		return t.rt.serveGRPCStream(ctx, rpc, stream)
	}

	var in []byte
	if err := stream.RecvMsg(&in); err != nil {
		return err
	}

	out, err := t.rt.serveGRPC(ctx, fullMethod, in)
	if err != nil {
		return err
	}
	return stream.SendMsg(out)
}

func validToken(ctx context.Context, token string) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	values := md.Get("authorization")
	if len(values) != 1 {
		return false
	}
	want := "Bearer " + token
	return subtle.ConstantTimeCompare([]byte(values[0]), []byte(want)) == 1
}

// serveGRPC resolves one gRPC call to its JSON-RPC handler and runs it. The
// returned error is always a gRPC status.
func (rt *runtime) serveGRPC(ctx context.Context, fullMethod string, in []byte) ([]byte, error) {
	rpc, ok := rpcIndex()[fullMethod]
	if !ok {
		return nil, grpcStatus(&protocol.Error{Code: protocol.CodeMethodNotFound, Message: "unknown method: " + fullMethod}, 0)
	}
	if stdioOnlyMethods[rpc.name] {
		return nil, grpcStatus(&protocol.Error{Code: protocol.CodeMethodNotFound, Message: rpc.name + " is only served on stdio"}, 0)
	}

	h, ok := rt.conn.Handler(rpc.name)
	if !ok {
		return nil, grpcStatus(&protocol.Error{Code: protocol.CodeMethodNotFound, Message: "unknown method: " + rpc.name}, 0)
	}

	params, err := decodeRequest(rpc.input, in)
	if err != nil {
		return nil, grpcStatus(InvalidParams(err.Error()), 0)
	}

	ctx = WithHost(ctx, rt.host)
	if rpc.notification {
		// A notification is answered at once; its handler outlives the call.
		go func() { _, _ = invokeSafely(context.WithoutCancel(ctx), rpc.name, h, params) }()
		return encodeResponse(rpc.output, nil)
	}

	res, err := invokeSafely(ctx, rpc.name, h, params)
	if err != nil {
		return nil, grpcStatus(err, 0)
	}
	out, err := encodeResponse(rpc.output, res)
	if err != nil {
		return nil, grpcStatus(Internal("marshal result: "+err.Error()), 0)
	}
	return out, nil
}

// serveGRPCStream reads a client stream until its end, hands every message to
// the stream handler of the rpc and answers once. The returned error is
// always a gRPC status.
func (rt *runtime) serveGRPCStream(ctx context.Context, rpc *rpcMethod, stream grpc.ServerStream) (err error) {
	open, ok := rt.streams[rpc.name]
	if !ok {
		return grpcStatus(&protocol.Error{Code: protocol.CodeMethodNotFound, Message: "unknown method: " + rpc.name}, 0)
	}

	// An open stream counts for plugin.shutdown like any capability call.
	rt.inflight.Add(1)
	defer rt.inflight.Add(-1)
	defer func() {
		if r := recover(); r != nil {
			err = grpcStatus(Internal(fmt.Sprintf("panic in %s: %v", rpc.name, r)), 0)
		}
	}()

	ctx = WithHost(ctx, rt.host)
	h := open()
	for {
		var in []byte
		if recvErr := stream.RecvMsg(&in); recvErr != nil {
			if errors.Is(recvErr, io.EOF) {
				break
			}
			// The caller went away or the transport failed; the status of
			// the stream already says so.
			return recvErr
		}
		if addErr := h.add(ctx, in); addErr != nil {
			return grpcStatus(addErr, 0)
		}
	}

	out, err := h.finish(ctx)
	if err != nil {
		return grpcStatus(err, 0)
	}
	return stream.SendMsg(out)
}

// invokeSafely runs h and turns a panic into an internal error, like the
// stdio dispatcher does.
func invokeSafely(ctx context.Context, method string, h Handler, params json.RawMessage) (res any, err error) {
	defer func() {
		if r := recover(); r != nil {
			res, err = nil, Internal(fmt.Sprintf("panic in %s: %v", method, r))
		}
	}()
	return h(ctx, params)
}

// decodeRequest turns the protobuf request into the JSON params the handler
// expects, with proto field names.
func decodeRequest(md protoreflect.MessageDescriptor, in []byte) (json.RawMessage, error) {
	msg := dynamicpb.NewMessage(md)
	if err := proto.Unmarshal(in, msg); err != nil {
		return nil, fmt.Errorf("decode %s: %w", md.FullName(), err)
	}
	raw, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("encode %s as JSON: %w", md.FullName(), err)
	}
	return raw, nil
}

// encodeResponse turns the handler result into the protobuf response. Members
// the message does not know are dropped, as on stdio (WIRE-7).
func encodeResponse(md protoreflect.MessageDescriptor, res any) ([]byte, error) {
	msg := dynamicpb.NewMessage(md)
	if res != nil {
		raw, err := json.Marshal(res)
		if err != nil {
			return nil, err
		}
		if trimmed := strings.TrimSpace(string(raw)); trimmed != "" && trimmed != "null" {
			if err = (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, msg); err != nil {
				return nil, fmt.Errorf("decode result into %s: %w", md.FullName(), err)
			}
		}
	}
	return proto.Marshal(msg)
}

// grpcCodeFor maps a JSON-RPC error code onto a gRPC status code
// (spec/03-wire-protocol.md WIRE-11).
func grpcCodeFor(code int) codes.Code {
	switch code {
	case protocol.CodeParseError, protocol.CodeInvalidRequest, protocol.CodeInvalidParams, protocol.CodeInvalidConfig:
		return codes.InvalidArgument
	case protocol.CodeMethodNotFound, protocol.CodeUnsupported:
		return codes.Unimplemented
	case protocol.CodeInternalError:
		return codes.Internal
	case protocol.CodePermissionDenied:
		return codes.PermissionDenied
	default:
		return codes.Unknown
	}
}

// grpcStatus turns a handler error into a gRPC status carrying the JSON-RPC
// error as a PluginError detail. override replaces the mapped status code
// when it is not OK.
func grpcStatus(err error, override codes.Code) error {
	var pe *protocol.Error
	if !errors.As(err, &pe) {
		pe = Internal(err.Error())
	}

	code := grpcCodeFor(pe.Code)
	if override != codes.OK {
		code = override
	}
	st := status.New(code, pe.Message)
	detail := &pluginv1.PluginError{Code: int32(pe.Code), Message: pe.Message, Data: errorData(pe.Data)}
	if withDetail, detailErr := st.WithDetails(detail); detailErr == nil {
		st = withDetail
	}
	return st.Err()
}

// errorData converts PluginError data to a Struct. A value that is not a JSON
// object is wrapped as {"value": data} (spec/03-wire-protocol.md WIRE-5).
func errorData(data any) *structpb.Struct {
	if data == nil {
		return nil
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return nil
	}
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "null" {
		return nil
	}
	if strings.HasPrefix(trimmed, "{") {
		out := &structpb.Struct{}
		if err = protojson.Unmarshal(raw, out); err == nil {
			return out
		}
		return nil
	}
	value := &structpb.Value{}
	if err = protojson.Unmarshal(raw, value); err != nil {
		return nil
	}
	return &structpb.Struct{Fields: map[string]*structpb.Value{"value": value}}
}
