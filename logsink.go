package sdk

import (
	"context"
	"time"

	pluginv1 "github.com/nginxui/plugin-sdk-go/pb"
	"github.com/nginxui/plugin-sdk-go/protocol"
	"google.golang.org/protobuf/proto"
)

// MaxLogSinkBatch bounds the entries one Push call receives. The host never
// sends more in one stream; a longer stream reaches Push in chunks.
const MaxLogSinkBatch = protocol.MaxLogSinkBatchSize

// LogEntry is one access log line the host streamed. The fields of
// protocol.LogEntry are promoted, e.g. entry.Status and entry.RequestURI.
type LogEntry struct {
	// LogPath is the absolute path of the access log the line was read from.
	LogPath string
	protocol.LogEntry
}

// Time parses Timestamp. It returns the zero time when the host sent none or
// one that is not RFC 3339.
func (e LogEntry) Time() time.Time {
	t, err := time.Parse(time.RFC3339Nano, e.Timestamp)
	if err != nil {
		return time.Time{}
	}
	return t
}

// Parsed reports whether the host parsed the line into fields. Otherwise only
// Raw and Timestamp are set.
func (e LogEntry) Parsed() bool {
	return e.Format == protocol.LogFormatCombined
}

// LogSinkHandler receives the nginx access log lines of the host while they
// are written. The manifest must request the log.read permission and may tune
// the batches in its log_sink block. The lines travel as a client stream on
// the gRPC transport only, so a plugin with a LogSinkHandler always serves
// gRPC.
//
// Treat every field as untrusted input and as personal data: clients choose
// the request URI, the referer and the user agent, and a query string may
// carry a token.
type LogSinkHandler interface {
	// Push handles one batch of at most MaxLogSinkBatch entries and returns
	// how many it kept; the rest counts as rejected. Answer promptly: the
	// host sends the next batch only after this one returned and drops lines
	// meanwhile, so buffer the entries instead of waiting for a slow
	// destination. An error means the whole batch is lost, the host does not
	// send it again. batch is not used after Push returned.
	Push(ctx context.Context, batch []LogEntry) (accepted int, err error)
}

// registerLogSink serves the log.push stream. It has no stdio handler, so a
// log.push request on stdio answers method not found (spec WIRE-12).
func (rt *runtime) registerLogSink() {
	rt.streams[protocol.MethodLogPush] = func() streamHandler { return &logPushStream{rt: rt} }
}

// logPushStream collects the entries of one log.push stream.
type logPushStream struct {
	rt       *runtime
	batch    []LogEntry
	accepted int
	total    int
}

func (s *logPushStream) add(ctx context.Context, in []byte) error {
	var req pluginv1.LogSinkPushRequest
	if err := proto.Unmarshal(in, &req); err != nil {
		return InvalidParams("decode log.push message: " + err.Error())
	}
	s.batch = append(s.batch, logEntryFromProto(&req))
	if len(s.batch) >= MaxLogSinkBatch {
		return s.flush(ctx)
	}
	return nil
}

func (s *logPushStream) flush(ctx context.Context) error {
	if len(s.batch) == 0 {
		return nil
	}
	batch := s.batch
	s.batch = nil

	accepted, err := s.rt.plugin.LogSink.Push(ctx, batch)
	if err != nil {
		return err
	}
	s.accepted += min(max(accepted, 0), len(batch))
	s.total += len(batch)
	return nil
}

func (s *logPushStream) finish(ctx context.Context) ([]byte, error) {
	if err := s.flush(ctx); err != nil {
		return nil, err
	}
	return proto.Marshal(&pluginv1.LogSinkPushResponse{
		Accepted: uint32(s.accepted),
		Rejected: uint32(s.total - s.accepted),
	})
}

// logEntryFromProto converts one stream message.
func logEntryFromProto(req *pluginv1.LogSinkPushRequest) LogEntry {
	e := req.GetEntry()
	return LogEntry{
		LogPath: req.GetLogPath(),
		LogEntry: protocol.LogEntry{
			Timestamp:            e.GetTimestamp(),
			RemoteAddr:           e.GetRemoteAddr(),
			RequestMethod:        e.GetRequestMethod(),
			RequestURI:           e.GetRequestUri(),
			Protocol:             e.GetProtocol(),
			Status:               int(e.GetStatus()),
			BodyBytesSent:        protocol.ByteSize(e.GetBodyBytesSent()),
			Referer:              e.GetReferer(),
			UserAgent:            e.GetUserAgent(),
			UpstreamAddr:         e.GetUpstreamAddr(),
			RequestTime:          e.GetRequestTime(),
			UpstreamResponseTime: e.GetUpstreamResponseTime(),
			Host:                 e.GetHost(),
			Raw:                  e.GetRaw(),
			Format:               e.GetFormat(),
		},
	}
}
