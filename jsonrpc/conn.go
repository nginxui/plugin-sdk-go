// Package jsonrpc implements the bidirectional JSON-RPC 2.0 peer used by
// nginx-ui plugins. Messages are framed as NDJSON: one compact JSON object per
// line. The same connection carries host->plugin requests and notifications as
// well as plugin->host host.* calls.
package jsonrpc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/0xJacky/nginx-ui-plugin-sdk-go/protocol"
)

// MaxMessageBytes is the largest single message accepted or produced.
const MaxMessageBytes = 4 << 20

// Version is the JSON-RPC protocol version written on every message.
const Version = "2.0"

// ErrClosed is returned by Call and Notify after the connection is closed.
var ErrClosed = errors.New("jsonrpc: connection closed")

// ErrTooLarge is returned when a message exceeds MaxMessageBytes.
var ErrTooLarge = errors.New("jsonrpc: message exceeds the 4 MiB limit")

// DrainTimeout bounds how long Serve waits for the handlers that are still
// running when the input ends, so their replies are not lost.
const DrainTimeout = 30 * time.Second

// Handler serves one inbound method. Returning a *protocol.Error sends that
// error verbatim; any other error becomes an internal error.
type Handler func(ctx context.Context, params json.RawMessage) (any, error)

// message is the union of a JSON-RPC request, notification and response.
type message struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *protocol.Error `json:"error,omitempty"`
}

// Conn is a concurrency safe JSON-RPC 2.0 peer over a reader/writer pair.
type Conn struct {
	br     *bufio.Reader
	w      io.Writer
	closer io.Closer

	writeMu sync.Mutex

	mu       sync.RWMutex
	handlers map[string]Handler
	pending  map[string]chan *message

	nextID atomic.Int64

	// inflight counts the handlers that have not returned yet.
	inflight sync.WaitGroup

	closeOnce sync.Once
	done      chan struct{}
}

// NewConn builds a peer reading requests from r and writing messages to w.
func NewConn(r io.Reader, w io.Writer) *Conn {
	c := &Conn{
		br:       bufio.NewReaderSize(r, 64<<10),
		w:        w,
		handlers: make(map[string]Handler),
		pending:  make(map[string]chan *message),
		done:     make(chan struct{}),
	}
	if closer, ok := r.(io.Closer); ok {
		c.closer = closer
	}
	return c
}

// Handle registers a handler for method. It must not be called concurrently
// with itself for the same method.
func (c *Conn) Handle(method string, h Handler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handlers[method] = h
}

// Handled reports whether a handler is registered for method.
func (c *Conn) Handled(method string) bool {
	_, ok := c.Handler(method)
	return ok
}

// Handler returns the handler registered for method. Other transports use it
// to serve a method exactly as this connection would.
func (c *Conn) Handler(method string) (Handler, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	h, ok := c.handlers[method]
	return h, ok
}

// Done is closed when the connection is closed.
func (c *Conn) Done() <-chan struct{} { return c.done }

// Close releases the connection and unblocks every pending call.
func (c *Conn) Close() {
	c.closeOnce.Do(func() {
		close(c.done)
		if c.closer != nil {
			_ = c.closer.Close()
		}
	})
}

// Call sends a request and waits for the matching reply. params may be nil.
// result may be nil to discard the reply payload.
func (c *Conn) Call(ctx context.Context, method string, params, result any) error {
	select {
	case <-c.done:
		return ErrClosed
	default:
	}

	id := json.RawMessage(strconv.FormatInt(c.nextID.Add(1), 10))
	key := string(id)

	ch := make(chan *message, 1)
	c.mu.Lock()
	c.pending[key] = ch
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.pending, key)
		c.mu.Unlock()
	}()

	msg := &message{JSONRPC: Version, ID: id, Method: method}
	if params != nil {
		raw, err := json.Marshal(params)
		if err != nil {
			return fmt.Errorf("jsonrpc: marshal params for %s: %w", method, err)
		}
		msg.Params = raw
	}
	if err := c.writeMessage(msg); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return ErrClosed
	case resp := <-ch:
		if resp.Error != nil {
			return resp.Error
		}
		if result == nil || len(resp.Result) == 0 {
			return nil
		}
		return json.Unmarshal(resp.Result, result)
	}
}

// Notify sends a notification, which carries no id and is never answered.
func (c *Conn) Notify(ctx context.Context, method string, params any) error {
	select {
	case <-c.done:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	msg := &message{JSONRPC: Version, Method: method}
	if params != nil {
		raw, err := json.Marshal(params)
		if err != nil {
			return fmt.Errorf("jsonrpc: marshal params for %s: %w", method, err)
		}
		msg.Params = raw
	}
	return c.writeMessage(msg)
}

// Serve reads messages until the reader ends, the connection is closed or ctx
// is cancelled. Inbound requests run in their own goroutine so that the host
// may pipeline them. A clean end of input returns nil.
func (c *Conn) Serve(ctx context.Context) error {
	defer c.Close()

	stop := context.AfterFunc(ctx, c.Close)
	defer stop()

	for {
		line, tooLong, err := c.readLine()

		if tooLong {
			c.writeError(json.RawMessage("null"), &protocol.Error{
				Code:    protocol.CodeInvalidRequest,
				Message: ErrTooLarge.Error(),
			})
		} else if trimmed := bytes.TrimSpace(line); len(trimmed) > 0 {
			c.dispatch(ctx, trimmed)
		}

		if err != nil {
			if isCleanEnd(err) {
				c.drain()
				return nil
			}
			return err
		}

		select {
		case <-c.done:
			return nil
		default:
		}
	}
}

// drain waits for the running handlers to answer, giving up after
// DrainTimeout so a stuck handler cannot hold the process open.
func (c *Conn) drain() {
	done := make(chan struct{})
	go func() {
		c.inflight.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(DrainTimeout):
	}
}

func isCleanEnd(err error) bool {
	return errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, context.Canceled)
}

// readLine reads one NDJSON line. tooLong reports that the line exceeded
// MaxMessageBytes, in which case the payload is dropped but the stream stays
// synchronised on the next newline.
func (c *Conn) readLine() (line []byte, tooLong bool, err error) {
	for {
		chunk, rerr := c.br.ReadSlice('\n')
		if len(chunk) > 0 {
			if tooLong || len(line)+len(chunk) > MaxMessageBytes {
				tooLong = true
				line = nil
			} else {
				line = append(line, chunk...)
			}
		}

		switch {
		case rerr == nil:
			return line, tooLong, nil
		case errors.Is(rerr, bufio.ErrBufferFull):
			continue
		default:
			return line, tooLong, rerr
		}
	}
}

// dispatch routes one raw line, which may hold a single message or a batch.
func (c *Conn) dispatch(ctx context.Context, raw []byte) {
	if raw[0] == '[' {
		var batch []json.RawMessage
		if err := json.Unmarshal(raw, &batch); err != nil {
			c.writeError(json.RawMessage("null"), &protocol.Error{
				Code:    protocol.CodeParseError,
				Message: "parse error",
			})
			return
		}
		// Replies to a batch are emitted as individual NDJSON lines.
		for _, item := range batch {
			c.handleOne(ctx, item)
		}
		return
	}
	c.handleOne(ctx, raw)
}

func (c *Conn) handleOne(ctx context.Context, raw []byte) {
	var msg message
	if err := json.Unmarshal(raw, &msg); err != nil {
		c.writeError(json.RawMessage("null"), &protocol.Error{
			Code:    protocol.CodeParseError,
			Message: "parse error",
		})
		return
	}

	if msg.Method == "" {
		c.deliverResponse(&msg)
		return
	}

	isNotification := len(msg.ID) == 0 || bytes.Equal(msg.ID, []byte("null"))

	c.mu.RLock()
	h := c.handlers[msg.Method]
	c.mu.RUnlock()

	if h == nil {
		if !isNotification {
			c.writeError(msg.ID, &protocol.Error{
				Code:    protocol.CodeMethodNotFound,
				Message: "unknown method: " + msg.Method,
			})
		}
		return
	}

	c.inflight.Add(1)
	go c.invoke(ctx, h, &msg, isNotification)
}

func (c *Conn) invoke(ctx context.Context, h Handler, msg *message, isNotification bool) {
	defer c.inflight.Done()
	defer func() {
		if r := recover(); r != nil && !isNotification {
			c.writeError(msg.ID, &protocol.Error{
				Code:    protocol.CodeInternalError,
				Message: fmt.Sprintf("panic in %s: %v", msg.Method, r),
			})
		}
	}()

	res, err := h(ctx, msg.Params)

	// A notification is never answered, whatever the handler returned.
	if isNotification {
		return
	}

	if err != nil {
		var pe *protocol.Error
		if errors.As(err, &pe) {
			c.writeError(msg.ID, pe)
			return
		}
		c.writeError(msg.ID, &protocol.Error{
			Code:    protocol.CodeInternalError,
			Message: err.Error(),
		})
		return
	}

	raw, mErr := json.Marshal(res)
	if mErr != nil {
		c.writeError(msg.ID, &protocol.Error{
			Code:    protocol.CodeInternalError,
			Message: "marshal result: " + mErr.Error(),
		})
		return
	}

	_ = c.writeMessage(&message{JSONRPC: Version, ID: msg.ID, Result: raw})
}

func (c *Conn) deliverResponse(msg *message) {
	if len(msg.ID) == 0 {
		return
	}

	c.mu.Lock()
	ch, ok := c.pending[string(msg.ID)]
	if ok {
		delete(c.pending, string(msg.ID))
	}
	c.mu.Unlock()

	if !ok {
		return
	}

	// The channel is buffered, so this never blocks.
	ch <- msg
}

func (c *Conn) writeError(id json.RawMessage, e *protocol.Error) {
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	_ = c.writeMessage(&message{JSONRPC: Version, ID: id, Error: e})
}

func (c *Conn) writeMessage(msg *message) error {
	if msg.Error == nil && len(msg.Result) == 0 && msg.Method == "" {
		msg.Result = json.RawMessage("null")
	}

	buf, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("jsonrpc: marshal message: %w", err)
	}
	if len(buf)+1 > MaxMessageBytes {
		return ErrTooLarge
	}
	buf = append(buf, '\n')

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	select {
	case <-c.done:
		return ErrClosed
	default:
	}

	if _, err := c.w.Write(buf); err != nil {
		return fmt.Errorf("jsonrpc: write: %w", err)
	}
	return nil
}
