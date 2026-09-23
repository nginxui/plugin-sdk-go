package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	pluginv1 "github.com/0xJacky/nginx-ui-plugin-sdk-go/pb"
	"github.com/0xJacky/nginx-ui-plugin-sdk-go/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

const logPushPath = "/nginxui.plugin.v1.LogSink/Push"

// logRecorder keeps every batch and accepts all but the entries with status
// 500.
type logRecorder struct {
	mu      sync.Mutex
	batches [][]LogEntry
	fail    error
}

func (r *logRecorder) Push(_ context.Context, batch []LogEntry) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail != nil {
		return 0, r.fail
	}
	r.batches = append(r.batches, append([]LogEntry(nil), batch...))
	accepted := 0
	for _, entry := range batch {
		if entry.Status != 500 {
			accepted++
		}
	}
	return accepted, nil
}

func (r *logRecorder) sizes() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	sizes := make([]int, 0, len(r.batches))
	for _, batch := range r.batches {
		sizes = append(sizes, len(batch))
	}
	return sizes
}

// pushStream sends every request as one message of a log.push stream and
// returns the decoded answer.
func pushStream(ctx context.Context, conn *grpc.ClientConn, requests ...*pluginv1.LogSinkPushRequest) (*pluginv1.LogSinkPushResponse, error) {
	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true}, logPushPath)
	if err != nil {
		return nil, err
	}
	for _, req := range requests {
		in, err := proto.Marshal(req)
		if err != nil {
			return nil, err
		}
		if err = stream.SendMsg(&in); err != nil {
			return nil, err
		}
	}
	if err = stream.CloseSend(); err != nil {
		return nil, err
	}
	var out []byte
	if err = stream.RecvMsg(&out); err != nil {
		return nil, err
	}
	var res pluginv1.LogSinkPushResponse
	if err = proto.Unmarshal(out, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

func logRequest(status int32) *pluginv1.LogSinkPushRequest {
	return &pluginv1.LogSinkPushRequest{
		LogPath: "/var/log/nginx/access.log",
		Entry: &pluginv1.LogEntry{
			Timestamp:            "2026-09-23T08:15:02Z",
			RemoteAddr:           "203.0.113.7",
			RequestMethod:        "GET",
			RequestUri:           "/index.html?lang=en",
			Protocol:             "HTTP/1.1",
			Status:               status,
			BodyBytesSent:        float64(6 << 30),
			UserAgent:            "curl/8.9.1",
			RequestTime:          0.004,
			UpstreamResponseTime: 0.003,
			Raw:                  "203.0.113.7 - - [...]",
			Format:               protocol.LogFormatCombined,
		},
	}
}

func TestLogSinkStreamsOverGRPC(t *testing.T) {
	sink := &logRecorder{}
	h := newGRPCHarness(t, Plugin{LogSink: sink}, shortTempDir(t), withGRPCNetwork("unix"))

	if caps := h.init.Capabilities; len(caps) != 1 || caps[0] != protocol.CapabilityLogSink {
		t.Fatalf("capabilities = %v", caps)
	}

	conn := h.dial(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := pushStream(ctx, conn, logRequest(200), logRequest(500), logRequest(404))
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	if res.GetAccepted() != 2 || res.GetRejected() != 1 {
		t.Fatalf("answer = %v", res)
	}

	if sizes := sink.sizes(); len(sizes) != 1 || sizes[0] != 3 {
		t.Fatalf("batches = %v", sizes)
	}
	entry := sink.batches[0][0]
	if entry.LogPath != "/var/log/nginx/access.log" || entry.RequestURI != "/index.html?lang=en" ||
		entry.Status != 200 || entry.BodyBytesSent != 6<<30 || entry.UpstreamResponseTime != 0.003 || !entry.Parsed() {
		t.Fatalf("entry = %+v", entry)
	}
	if got := entry.Time(); !got.Equal(time.Date(2026, 9, 23, 8, 15, 2, 0, time.UTC)) {
		t.Fatalf("time = %v", got)
	}

	// An empty stream is valid and answers with zero counts.
	res, err = pushStream(ctx, conn)
	if err != nil || res.GetAccepted() != 0 || res.GetRejected() != 0 {
		t.Fatalf("empty stream: %v, %v", res, err)
	}

	// Undecodable bytes are invalid params.
	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true}, logPushPath)
	if err != nil {
		t.Fatal(err)
	}
	in := []byte{0xff, 0xff, 0xff}
	_ = stream.SendMsg(&in)
	_ = stream.CloseSend()
	var out []byte
	st, pe := pluginErrorOf(t, stream.RecvMsg(&out))
	if st.Code() != codes.InvalidArgument || pe.GetCode() != protocol.CodeInvalidParams {
		t.Fatalf("malformed: status %v, detail %v", st, pe)
	}

	// A failing handler loses the batch and says so.
	sink.mu.Lock()
	sink.fail = errors.New("destination down")
	sink.mu.Unlock()
	_, err = pushStream(ctx, conn, logRequest(200))
	st, pe = pluginErrorOf(t, err)
	if st.Code() != codes.Internal || pe.GetCode() != protocol.CodeInternalError {
		t.Fatalf("failure: status %v, detail %v", st, pe)
	}

	h.stop(t)
}

func TestLogSinkSplitsLongStreams(t *testing.T) {
	sink := &logRecorder{}
	h := newGRPCHarness(t, Plugin{LogSink: sink}, shortTempDir(t), withGRPCNetwork("unix"))
	conn := h.dial(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	requests := make([]*pluginv1.LogSinkPushRequest, MaxLogSinkBatch+4)
	for i := range requests {
		requests[i] = logRequest(200)
	}
	res, err := pushStream(ctx, conn, requests...)
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	if int(res.GetAccepted()) != len(requests) {
		t.Fatalf("accepted = %d, want %d", res.GetAccepted(), len(requests))
	}
	if sizes := sink.sizes(); len(sizes) != 2 || sizes[0] != MaxLogSinkBatch || sizes[1] != 4 {
		t.Fatalf("batches = %v", sizes)
	}
}

func TestLogSinkIsNeverServedOnStdio(t *testing.T) {
	called := false
	h := newGRPCHarness(t, Plugin{
		LogSink: &logRecorder{},
		Methods: map[string]Handler{
			// A stdio handler for a streaming rpc is ignored.
			protocol.MethodLogPush: func(context.Context, json.RawMessage) (any, error) {
				called = true
				return protocol.EmptyResult{}, nil
			},
		},
	}, shortTempDir(t), withGRPCNetwork("unix"))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := h.host.Call(ctx, protocol.MethodLogPush, protocol.LogSinkPushParams{LogPath: "/var/log/nginx/access.log"}, nil)
	var pe *protocol.Error
	if !errors.As(err, &pe) || pe.Code != protocol.CodeMethodNotFound {
		t.Fatalf("stdio log.push = %v, want -32601", err)
	}
	if called {
		t.Fatal("the stdio handler of log.push ran")
	}
}

func TestLogSinkKeepsGRPCOn(t *testing.T) {
	t.Run("option", func(t *testing.T) {
		h := newGRPCHarness(t, Plugin{LogSink: &logRecorder{}}, shortTempDir(t), WithoutGRPC(), withGRPCNetwork("unix"))
		if len(h.init.Transports) != 2 || h.init.Transports[1] != protocol.TransportGRPC || h.init.RPCSocket == "" {
			t.Fatalf("handshake = %+v", h.init)
		}
	})
	t.Run("environment", func(t *testing.T) {
		t.Setenv(EnvDisableGRPC, "1")
		h := newGRPCHarness(t, Plugin{LogSink: &logRecorder{}}, shortTempDir(t), withGRPCNetwork("unix"))
		if len(h.init.Transports) != 2 || h.init.Transports[1] != protocol.TransportGRPC {
			t.Fatalf("handshake = %+v", h.init)
		}
	})
}

func TestLogPushWithoutHandlerIsUnknown(t *testing.T) {
	h := newGRPCHarness(t, Plugin{DNS01: grpcHandler{}}, shortTempDir(t), withGRPCNetwork("unix"))
	conn := h.dial(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := pushStream(ctx, conn, logRequest(200))
	st, pe := pluginErrorOf(t, err)
	if st.Code() != codes.Unimplemented || pe.GetCode() != protocol.CodeMethodNotFound {
		t.Fatalf("status %v, detail %v", st, pe)
	}
}

func TestRPCIndexMarksStreams(t *testing.T) {
	rpc, ok := rpcIndex()[logPushPath]
	if !ok || rpc.name != protocol.MethodLogPush || !rpc.streaming {
		t.Fatalf("log.push resolves to %+v", rpc)
	}
	if !isStreamingRPC(protocol.MethodLogPush) || isStreamingRPC(protocol.MethodDNS01Present) {
		t.Fatal("isStreamingRPC does not match the contract")
	}
}
