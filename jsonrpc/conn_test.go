package jsonrpc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nginxui/plugin-sdk-go/protocol"
)

// pair wires two peers back to back over in-memory pipes.
type pair struct {
	a, b *Conn
	stop context.CancelFunc
	wg   sync.WaitGroup
}

func newPair(t *testing.T) *pair {
	t.Helper()

	ar, bw := io.Pipe()
	br, aw := io.Pipe()

	_, cancel := context.WithCancel(t.Context())
	p := &pair{
		a:    NewConn(ar, aw),
		b:    NewConn(br, bw),
		stop: cancel,
	}

	t.Cleanup(func() {
		cancel()
		p.a.Close()
		p.b.Close()
		_ = ar.Close()
		_ = br.Close()
		_ = aw.Close()
		_ = bw.Close()
		p.wg.Wait()
	})

	return p
}

// serve starts both read loops. Handlers must be registered first.
func (p *pair) serve(ctx context.Context) {
	p.wg.Add(2)
	go func() { defer p.wg.Done(); _ = p.a.Serve(ctx) }()
	go func() { defer p.wg.Done(); _ = p.b.Serve(ctx) }()
}

func TestCallReturnsResult(t *testing.T) {
	p := newPair(t)

	p.b.Handle("sum", func(_ context.Context, raw json.RawMessage) (any, error) {
		var in struct{ A, B int }
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, err
		}
		return map[string]int{"total": in.A + in.B}, nil
	})
	p.serve(t.Context())

	var out struct {
		Total int `json:"total"`
	}
	if err := p.a.Call(t.Context(), "sum", map[string]int{"A": 2, "B": 40}, &out); err != nil {
		t.Fatalf("call: %v", err)
	}
	if out.Total != 42 {
		t.Fatalf("total = %d, want 42", out.Total)
	}
}

func TestCallWithoutParamsAndResult(t *testing.T) {
	p := newPair(t)

	p.b.Handle("ping", func(context.Context, json.RawMessage) (any, error) {
		return protocol.EmptyResult{}, nil
	})
	p.serve(t.Context())

	if err := p.a.Call(t.Context(), "ping", nil, nil); err != nil {
		t.Fatalf("call: %v", err)
	}
}

func TestUnknownMethodReturnsMethodNotFound(t *testing.T) {
	p := newPair(t)
	p.serve(t.Context())

	err := p.a.Call(t.Context(), "nope", nil, nil)

	var rpcErr *protocol.Error
	if !errors.As(err, &rpcErr) {
		t.Fatalf("err = %v, want *protocol.Error", err)
	}
	if rpcErr.Code != protocol.CodeMethodNotFound {
		t.Fatalf("code = %d, want %d", rpcErr.Code, protocol.CodeMethodNotFound)
	}
}

func TestHandlerProtocolErrorIsForwardedVerbatim(t *testing.T) {
	p := newPair(t)

	want := &protocol.Error{
		Code:    protocol.CodeInvalidConfig,
		Message: "missing token",
		Data:    protocol.InvalidConfigData{Field: "CF_DNS_API_TOKEN"},
	}
	p.b.Handle("check", func(context.Context, json.RawMessage) (any, error) {
		return nil, want
	})
	p.serve(t.Context())

	err := p.a.Call(t.Context(), "check", nil, nil)

	var rpcErr *protocol.Error
	if !errors.As(err, &rpcErr) {
		t.Fatalf("err = %v, want *protocol.Error", err)
	}
	if rpcErr.Code != want.Code || rpcErr.Message != want.Message {
		t.Fatalf("got %+v, want %+v", rpcErr, want)
	}

	data, ok := rpcErr.Data.(map[string]any)
	if !ok || data["field"] != "CF_DNS_API_TOKEN" {
		t.Fatalf("data = %#v, want field CF_DNS_API_TOKEN", rpcErr.Data)
	}
}

func TestPlainErrorBecomesInternalError(t *testing.T) {
	p := newPair(t)

	p.b.Handle("boom", func(context.Context, json.RawMessage) (any, error) {
		return nil, errors.New("exploded")
	})
	p.serve(t.Context())

	err := p.a.Call(t.Context(), "boom", nil, nil)

	var rpcErr *protocol.Error
	if !errors.As(err, &rpcErr) {
		t.Fatalf("err = %v, want *protocol.Error", err)
	}
	if rpcErr.Code != protocol.CodeInternalError || rpcErr.Message != "exploded" {
		t.Fatalf("got %+v", rpcErr)
	}
}

func TestPanicInHandlerBecomesInternalError(t *testing.T) {
	p := newPair(t)

	p.b.Handle("panic", func(context.Context, json.RawMessage) (any, error) {
		panic("kaboom")
	})
	p.serve(t.Context())

	err := p.a.Call(t.Context(), "panic", nil, nil)

	var rpcErr *protocol.Error
	if !errors.As(err, &rpcErr) {
		t.Fatalf("err = %v, want *protocol.Error", err)
	}
	if rpcErr.Code != protocol.CodeInternalError {
		t.Fatalf("code = %d, want %d", rpcErr.Code, protocol.CodeInternalError)
	}
}

func TestNotificationIsNeverAnswered(t *testing.T) {
	p := newPair(t)

	got := make(chan string, 1)
	p.b.Handle("event", func(_ context.Context, raw json.RawMessage) (any, error) {
		got <- string(raw)
		// A non-nil result and error must both be dropped for a notification.
		return map[string]string{"ignored": "yes"}, errors.New("ignored too")
	})
	// A follow-up request proves nothing was written in between.
	p.b.Handle("ping", func(context.Context, json.RawMessage) (any, error) {
		return protocol.EmptyResult{}, nil
	})
	p.serve(t.Context())

	if err := p.a.Notify(t.Context(), "event", map[string]int{"n": 1}); err != nil {
		t.Fatalf("notify: %v", err)
	}

	select {
	case raw := <-got:
		if !strings.Contains(raw, `"n":1`) {
			t.Fatalf("params = %s", raw)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("notification was not delivered")
	}

	if err := p.a.Call(t.Context(), "ping", nil, nil); err != nil {
		t.Fatalf("follow-up call: %v", err)
	}
}

func TestConcurrentCallsAreMatchedByID(t *testing.T) {
	p := newPair(t)

	p.b.Handle("echo", func(_ context.Context, raw json.RawMessage) (any, error) {
		var n int
		if err := json.Unmarshal(raw, &n); err != nil {
			return nil, err
		}
		// Reverse the natural ordering so replies cannot arrive in send order.
		time.Sleep(time.Duration(20-n) * time.Millisecond)
		return n, nil
	})
	p.serve(t.Context())

	const calls = 20
	var wg sync.WaitGroup
	errs := make([]error, calls)
	out := make([]int, calls)

	for i := range calls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = p.a.Call(t.Context(), "echo", i, &out[i])
		}()
	}
	wg.Wait()

	for i := range calls {
		if errs[i] != nil {
			t.Fatalf("call %d: %v", i, errs[i])
		}
		if out[i] != i {
			t.Fatalf("call %d returned %d", i, out[i])
		}
	}
}

func TestBidirectionalCalls(t *testing.T) {
	p := newPair(t)

	p.a.Handle("host.log", func(context.Context, json.RawMessage) (any, error) {
		return protocol.EmptyResult{}, nil
	})
	p.b.Handle("work", func(ctx context.Context, _ json.RawMessage) (any, error) {
		// Call back into the peer while serving its request.
		if err := p.b.Call(ctx, "host.log", map[string]string{"level": "info"}, nil); err != nil {
			return nil, err
		}
		return protocol.EmptyResult{}, nil
	})
	p.serve(t.Context())

	if err := p.a.Call(t.Context(), "work", nil, nil); err != nil {
		t.Fatalf("call: %v", err)
	}
}

func TestBatchInputProducesOneReplyPerRequest(t *testing.T) {
	pluginIn, hostW := io.Pipe()
	hostR, pluginOut := io.Pipe()

	conn := NewConn(pluginIn, pluginOut)
	conn.Handle("ping", func(context.Context, json.RawMessage) (any, error) {
		return protocol.EmptyResult{}, nil
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	done := make(chan struct{})
	go func() { defer close(done); _ = conn.Serve(ctx) }()

	t.Cleanup(func() {
		cancel()
		conn.Close()
		_ = hostW.Close()
		_ = hostR.Close()
		<-done
	})

	batch := `[{"jsonrpc":"2.0","id":1,"method":"ping"},` +
		`{"jsonrpc":"2.0","method":"ping"},` +
		`{"jsonrpc":"2.0","id":2,"method":"ping"}]` + "\n"

	go func() { _, _ = io.WriteString(hostW, batch) }()

	// Exactly two replies come back: the notification in the middle is silent.
	scanner := bufio.NewScanner(hostR)
	seen := map[float64]bool{}
	for range 2 {
		if !scanner.Scan() {
			t.Fatalf("scan: %v", scanner.Err())
		}
		var m struct {
			ID    float64         `json:"id"`
			Error *protocol.Error `json:"error"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &m); err != nil {
			t.Fatalf("unmarshal %s: %v", scanner.Text(), err)
		}
		if m.Error != nil {
			t.Fatalf("unexpected error %+v", m.Error)
		}
		seen[m.ID] = true
	}

	if !seen[1] || !seen[2] {
		t.Fatalf("replies = %v, want ids 1 and 2", seen)
	}
}

func TestOversizedMessageIsRejected(t *testing.T) {
	pluginIn, hostW := io.Pipe()
	hostR, pluginOut := io.Pipe()

	conn := NewConn(pluginIn, pluginOut)
	conn.Handle("ping", func(context.Context, json.RawMessage) (any, error) {
		return protocol.EmptyResult{}, nil
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	done := make(chan struct{})
	go func() { defer close(done); _ = conn.Serve(ctx) }()

	t.Cleanup(func() {
		cancel()
		conn.Close()
		_ = hostW.Close()
		_ = hostR.Close()
		<-done
	})

	go func() {
		huge := `{"jsonrpc":"2.0","id":1,"method":"ping","params":"` +
			strings.Repeat("x", MaxMessageBytes+16) + `"}` + "\n"
		_, _ = io.WriteString(hostW, huge)
		// The stream must stay usable for the next line.
		_, _ = io.WriteString(hostW, `{"jsonrpc":"2.0","id":2,"method":"ping"}`+"\n")
	}()

	scanner := bufio.NewScanner(hostR)
	scanner.Buffer(make([]byte, 0, 64<<10), MaxMessageBytes)

	if !scanner.Scan() {
		t.Fatalf("scan: %v", scanner.Err())
	}
	var rejected struct {
		ID    any             `json:"id"`
		Error *protocol.Error `json:"error"`
	}
	if err := json.Unmarshal(scanner.Bytes(), &rejected); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if rejected.Error == nil || rejected.Error.Code != protocol.CodeInvalidRequest {
		t.Fatalf("got %+v, want invalid request", rejected.Error)
	}
	if rejected.ID != nil {
		t.Fatalf("id = %v, want null", rejected.ID)
	}

	if !scanner.Scan() {
		t.Fatalf("scan after oversized message: %v", scanner.Err())
	}
	var ok struct {
		ID    float64         `json:"id"`
		Error *protocol.Error `json:"error"`
	}
	if err := json.Unmarshal(scanner.Bytes(), &ok); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if ok.Error != nil || ok.ID != 2 {
		t.Fatalf("got %+v (id %v), want a clean reply to id 2", ok.Error, ok.ID)
	}
}

func TestMalformedLineReturnsParseError(t *testing.T) {
	pluginIn, hostW := io.Pipe()
	hostR, pluginOut := io.Pipe()

	conn := NewConn(pluginIn, pluginOut)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	done := make(chan struct{})
	go func() { defer close(done); _ = conn.Serve(ctx) }()

	t.Cleanup(func() {
		cancel()
		conn.Close()
		_ = hostW.Close()
		_ = hostR.Close()
		<-done
	})

	go func() { _, _ = io.WriteString(hostW, "{not json}\n") }()

	scanner := bufio.NewScanner(hostR)
	if !scanner.Scan() {
		t.Fatalf("scan: %v", scanner.Err())
	}
	var m struct {
		Error *protocol.Error `json:"error"`
	}
	if err := json.Unmarshal(scanner.Bytes(), &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if m.Error == nil || m.Error.Code != protocol.CodeParseError {
		t.Fatalf("got %+v, want parse error", m.Error)
	}
}

func TestServeReturnsNilOnEOF(t *testing.T) {
	pluginIn, hostW := io.Pipe()
	_, pluginOut := io.Pipe()

	conn := NewConn(pluginIn, pluginOut)

	errCh := make(chan error, 1)
	go func() { errCh <- conn.Serve(t.Context()) }()

	_ = hostW.Close()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Serve = %v, want nil on EOF", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after EOF")
	}
}

func TestCallAfterCloseReturnsErrClosed(t *testing.T) {
	p := newPair(t)
	p.a.Close()

	if err := p.a.Call(t.Context(), "ping", nil, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
	if err := p.a.Notify(t.Context(), "ping", nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
}
