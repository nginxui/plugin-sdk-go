package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"sync"
	"sync/atomic"

	"github.com/0xJacky/nginx-ui-plugin-sdk-go/jsonrpc"
	"github.com/0xJacky/nginx-ui-plugin-sdk-go/protocol"
)

// ErrHostNotReady is returned by host.* calls made before the host sent
// plugin.initialized.
var ErrHostNotReady = errors.New("sdk: host calls are not allowed before plugin.initialized")

// Info describes the running plugin process and the host behind it.
type Info struct {
	// PluginID comes from NGINX_UI_PLUGIN_ID.
	PluginID string
	// APIVersion comes from NGINX_UI_PLUGIN_API_VERSION.
	APIVersion int
	// DataDir comes from NGINX_UI_PLUGIN_DATA_DIR and is the only writable
	// directory the plugin owns.
	DataDir string
	// HostVersion comes from NGINX_UI_VERSION and is refined by the handshake.
	HostVersion string
	// Host is the handshake payload, empty until plugin.initialize arrives.
	Host protocol.HostInfo
	// Permissions are the manifest permissions the host granted.
	Permissions []string
}

// Host is the plugin's client for the host.* side of the protocol.
type Host struct {
	conn  *jsonrpc.Conn
	ready atomic.Bool

	mu       sync.RWMutex
	settings map[string]any
	info     Info
}

var currentHost atomic.Pointer[Host]

// CurrentHost returns the host client of the running plugin, or nil when the
// process is not serving. It is usable for logging from the moment Run starts.
func CurrentHost() *Host { return currentHost.Load() }

type hostContextKey struct{}

// WithHost attaches a host client to ctx.
func WithHost(ctx context.Context, h *Host) context.Context {
	return context.WithValue(ctx, hostContextKey{}, h)
}

// HostFromContext returns the host client carried by ctx, falling back to the
// process wide client so handlers always get a usable value.
func HostFromContext(ctx context.Context) *Host {
	if h, ok := ctx.Value(hostContextKey{}).(*Host); ok && h != nil {
		return h
	}
	return CurrentHost()
}

func newHost(conn *jsonrpc.Conn, info Info) *Host {
	return &Host{conn: conn, info: info, settings: map[string]any{}}
}

// Ready reports whether host.* calls are allowed yet.
func (h *Host) Ready() bool { return h != nil && h.ready.Load() }

// Info returns a snapshot of the plugin and host identity.
func (h *Host) Info() Info {
	h.mu.RLock()
	defer h.mu.RUnlock()

	out := h.info
	out.Permissions = append([]string(nil), h.info.Permissions...)
	return out
}

// Settings returns a copy of the latest settings the host pushed through
// plugin.initialize or plugin.configure.
func (h *Host) Settings() map[string]any {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return maps.Clone(h.settings)
}

func (h *Host) setSettings(s map[string]any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if s == nil {
		s = map[string]any{}
	}
	h.settings = maps.Clone(s)
}

func (h *Host) setHandshake(p protocol.InitializeParams) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.info.Host = p.Host
	if p.Host.Version != "" {
		h.info.HostVersion = p.Host.Version
	}
	h.info.Permissions = append([]string(nil), p.Permissions...)
	h.settings = maps.Clone(p.Settings)
	if h.settings == nil {
		h.settings = map[string]any{}
	}
}

func (h *Host) call(ctx context.Context, method string, params, result any) error {
	if h == nil || h.conn == nil {
		return ErrHostNotReady
	}
	if !h.ready.Load() {
		return ErrHostNotReady
	}
	return h.conn.Call(ctx, method, params, result)
}

// Log sends one line to the host log. It is a notification, so it never blocks
// on a reply; the SDK logger falls back to stderr when it fails.
func (h *Host) Log(level, msg string, fields map[string]any) error {
	if h == nil || h.conn == nil {
		return ErrHostNotReady
	}
	if !h.ready.Load() {
		return ErrHostNotReady
	}
	return h.conn.Notify(context.Background(), protocol.MethodHostLog, protocol.HostLogParams{
		Level:   level,
		Message: msg,
		Fields:  fields,
	})
}

// kvGetResult mirrors protocol.HostKVGetResult with a decodable value.
type kvGetResult struct {
	Value json.RawMessage `json:"value"`
	Found bool            `json:"found"`
}

// KVGet reads a key from the plugin's private store. out may be nil.
func (h *Host) KVGet(ctx context.Context, key string, out any) (bool, error) {
	var res kvGetResult
	if err := h.call(ctx, protocol.MethodHostKVGet, protocol.HostKVGetParams{Key: key}, &res); err != nil {
		return false, err
	}
	if !res.Found || out == nil || len(res.Value) == 0 {
		return res.Found, nil
	}
	if err := json.Unmarshal(res.Value, out); err != nil {
		return true, err
	}
	return true, nil
}

// KVSet stores a JSON value, up to 64 KiB encoded.
func (h *Host) KVSet(ctx context.Context, key string, value any) error {
	return h.call(ctx, protocol.MethodHostKVSet, protocol.HostKVSetParams{Key: key, Value: value}, nil)
}

// KVDelete removes a key.
func (h *Host) KVDelete(ctx context.Context, key string) error {
	return h.call(ctx, protocol.MethodHostKVDelete, protocol.HostKVGetParams{Key: key}, nil)
}

// KVList lists the keys under a prefix.
func (h *Host) KVList(ctx context.Context, prefix string) ([]string, error) {
	var res protocol.HostKVListResult
	if err := h.call(ctx, protocol.MethodHostKVList, protocol.HostKVListParams{Prefix: prefix}, &res); err != nil {
		return nil, err
	}
	return res.Keys, nil
}

// SettingsGet re-reads the plugin settings from the host.
func (h *Host) SettingsGet(ctx context.Context) (map[string]any, error) {
	var res protocol.HostSettingsGetResult
	if err := h.call(ctx, protocol.MethodHostSettingsGet, nil, &res); err != nil {
		return nil, err
	}
	h.setSettings(res.Settings)
	return res.Settings, nil
}

// Locale returns the host UI locale.
func (h *Host) Locale(ctx context.Context) (string, error) {
	var res protocol.HostI18nLocaleResult
	if err := h.call(ctx, protocol.MethodHostI18nLocale, nil, &res); err != nil {
		return "", err
	}
	return res.Locale, nil
}

// CredentialsGet resolves a stored credential the plugin was granted access to.
func (h *Host) CredentialsGet(ctx context.Context, kind, id string) (protocol.HostCredentialsGetResult, error) {
	var res protocol.HostCredentialsGetResult
	err := h.call(ctx, protocol.MethodHostCredentialsGet, protocol.HostCredentialsGetParams{Kind: kind, ID: id}, &res)
	return res, err
}

// CronRegister asks the host to call method on a schedule.
func (h *Host) CronRegister(ctx context.Context, id, schedule, method string) error {
	return h.call(ctx, protocol.MethodHostCronRegister, protocol.HostCronRegisterParams{
		ID:       id,
		Schedule: schedule,
		Method:   method,
	}, nil)
}

// CronUnregister drops a previously registered schedule.
func (h *Host) CronUnregister(ctx context.Context, id string) error {
	return h.call(ctx, protocol.MethodHostCronUnregister, protocol.HostCronUnregisterParams{ID: id}, nil)
}

// Notify raises a user facing notification in the nginx-ui UI.
func (h *Host) Notify(ctx context.Context, level, title, content string, details any) error {
	return h.call(ctx, protocol.MethodHostNotify, protocol.HostNotifyParams{
		Level:   level,
		Title:   title,
		Content: content,
		Details: details,
	}, nil)
}

// MetricsSnapshot reads the host analytics snapshot into out.
func (h *Host) MetricsSnapshot(ctx context.Context, out any) error {
	var res struct {
		Snapshot json.RawMessage `json:"snapshot"`
	}
	if err := h.call(ctx, protocol.MethodHostMetricsSnapshot, nil, &res); err != nil {
		return err
	}
	if out == nil || len(res.Snapshot) == 0 {
		return nil
	}
	return json.Unmarshal(res.Snapshot, out)
}
