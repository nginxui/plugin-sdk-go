package sdk_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	sdk "github.com/nginxui/plugin-sdk-go"
	"github.com/nginxui/plugin-sdk-go/jsonrpc"
	"github.com/nginxui/plugin-sdk-go/protocol"
)

// harness runs a plugin over in-memory pipes and exposes the host side of the
// connection.
type harness struct {
	host   *jsonrpc.Conn
	logs   chan protocol.HostLogParams
	runErr chan error
}

func newHarness(t *testing.T, p sdk.Plugin) *harness {
	t.Helper()

	pluginIn, hostW := io.Pipe()
	hostR, pluginOut := io.Pipe()

	h := &harness{
		host:   jsonrpc.NewConn(hostR, hostW),
		logs:   make(chan protocol.HostLogParams, 16),
		runErr: make(chan error, 1),
	}

	h.host.Handle(protocol.MethodHostLog, func(_ context.Context, raw json.RawMessage) (any, error) {
		var params protocol.HostLogParams
		if err := json.Unmarshal(raw, &params); err != nil {
			return nil, err
		}
		select {
		case h.logs <- params:
		default:
		}
		return protocol.EmptyResult{}, nil
	})

	ctx, cancel := context.WithCancel(t.Context())

	hostDone := make(chan struct{})
	go func() { defer close(hostDone); _ = h.host.Serve(ctx) }()
	go func() { h.runErr <- sdk.Run(ctx, p, pluginIn, pluginOut) }()

	t.Cleanup(func() {
		cancel()
		h.host.Close()
		_ = hostW.Close()
		_ = hostR.Close()
		<-hostDone
	})

	return h
}

// initialize performs the handshake and returns the plugin's reply.
func (h *harness) initialize(t *testing.T) protocol.InitializeResult {
	t.Helper()

	var res protocol.InitializeResult
	params := protocol.InitializeParams{
		Host: protocol.HostInfo{Version: "2.7.0", OS: "linux", Arch: "amd64", Locale: "en"},
		Settings: map[string]any{
			"default_propagation_timeout_seconds": float64(90),
		},
		Permissions: []string{protocol.PermissionNetwork},
	}
	if err := h.host.Call(t.Context(), protocol.MethodInitialize, params, &res); err != nil {
		t.Fatalf("initialize: %v", err)
	}
	if err := h.host.Notify(t.Context(), protocol.MethodInitialized, nil); err != nil {
		t.Fatalf("initialized: %v", err)
	}
	return res
}

// recorder is a DNS01Handler that only remembers what it was asked.
type recorder struct {
	present chan sdk.DNS01Request
	cleanup chan sdk.DNS01Request
	err     error
}

func newRecorder() *recorder {
	return &recorder{
		present: make(chan sdk.DNS01Request, 4),
		cleanup: make(chan sdk.DNS01Request, 4),
	}
}

func (r *recorder) Present(ctx context.Context, req sdk.DNS01Request) error {
	// The logger must reach the host once the handshake completed.
	sdk.Infof("presenting %s", req.FQDN)
	r.present <- req
	return r.err
}

func (r *recorder) CleanUp(_ context.Context, req sdk.DNS01Request) error {
	r.cleanup <- req
	return r.err
}

func TestLifecycle(t *testing.T) {
	rec := newRecorder()

	configured := make(chan map[string]any, 4)
	shutdown := make(chan struct{}, 1)

	h := newHarness(t, sdk.Plugin{
		DNS01: rec,
		Configure: func(_ context.Context, settings map[string]any) error {
			configured <- settings
			return nil
		},
		Shutdown: func(context.Context) error {
			shutdown <- struct{}{}
			return nil
		},
	})

	res := h.initialize(t)
	if res.APIVersion != protocol.APIVersion {
		t.Fatalf("api_version = %d, want %d", res.APIVersion, protocol.APIVersion)
	}
	if len(res.Capabilities) != 1 || res.Capabilities[0] != protocol.CapabilityDNS01 {
		t.Fatalf("capabilities = %v, want [dns01]", res.Capabilities)
	}

	// plugin.ping replies with an empty object.
	var ping map[string]any
	if err := h.host.Call(t.Context(), protocol.MethodPing, nil, &ping); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if len(ping) != 0 {
		t.Fatalf("ping = %v, want {}", ping)
	}

	// plugin.configure updates the settings the host client reports.
	cfg := protocol.ConfigureParams{Settings: map[string]any{"recursive_nameservers": "1.1.1.1:53"}}
	if err := h.host.Call(t.Context(), protocol.MethodConfigure, cfg, nil); err != nil {
		t.Fatalf("configure: %v", err)
	}
	select {
	case got := <-configured:
		if got["recursive_nameservers"] != "1.1.1.1:53" {
			t.Fatalf("settings = %v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("configure was not delivered")
	}

	// dns01.present reaches the handler and its log line reaches the host.
	req := protocol.DNS01ChallengeParams{
		Provider:      "exec",
		Domain:        "example.com",
		FQDN:          "_acme-challenge.example.com.",
		EffectiveFQDN: "_acme-challenge.example.com.",
		Value:         "token-value",
		Token:         "tok",
		KeyAuth:       "key",
	}
	if err := h.host.Call(t.Context(), protocol.MethodDNS01Present, req, nil); err != nil {
		t.Fatalf("dns01.present: %v", err)
	}
	select {
	case got := <-rec.present:
		if got.FQDN != req.FQDN {
			t.Fatalf("fqdn = %q", got.FQDN)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("present was not delivered")
	}
	select {
	case line := <-h.logs:
		if line.Level != string(sdk.LevelInfo) {
			t.Fatalf("log level = %q", line.Level)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("host.log was not received")
	}

	if err := h.host.Call(t.Context(), protocol.MethodDNS01Cleanup, req, nil); err != nil {
		t.Fatalf("dns01.cleanup: %v", err)
	}
	select {
	case <-rec.cleanup:
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup was not delivered")
	}

	// plugin.shutdown runs the hook and replies, then plugin.exit ends Run.
	if err := h.host.Call(t.Context(), protocol.MethodShutdown, nil, nil); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	select {
	case <-shutdown:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown hook did not run")
	}

	if err := h.host.Notify(t.Context(), protocol.MethodExit, nil); err != nil {
		t.Fatalf("exit: %v", err)
	}
	select {
	case err := <-h.runErr:
		if err != nil {
			t.Fatalf("Run = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after plugin.exit")
	}
}

func TestOptionalDNS01MethodsAreUnsupported(t *testing.T) {
	h := newHarness(t, sdk.Plugin{DNS01: newRecorder()})
	h.initialize(t)

	for _, method := range []string{
		protocol.MethodDNS01Validate,
		protocol.MethodDNS01Options,
		protocol.MethodDNS01Check,
	} {
		err := h.host.Call(t.Context(), method, map[string]string{"provider": "exec"}, nil)

		var rpcErr *protocol.Error
		if !errors.As(err, &rpcErr) {
			t.Fatalf("%s: err = %v, want *protocol.Error", method, err)
		}
		if rpcErr.Code != protocol.CodeUnsupported {
			t.Fatalf("%s: code = %d, want %d", method, rpcErr.Code, protocol.CodeUnsupported)
		}
	}
}

func TestUnknownMethodReturnsMethodNotFound(t *testing.T) {
	h := newHarness(t, sdk.Plugin{DNS01: newRecorder()})
	h.initialize(t)

	err := h.host.Call(t.Context(), "http.handle", nil, nil)

	var rpcErr *protocol.Error
	if !errors.As(err, &rpcErr) {
		t.Fatalf("err = %v, want *protocol.Error", err)
	}
	if rpcErr.Code != protocol.CodeMethodNotFound {
		t.Fatalf("code = %d, want %d", rpcErr.Code, protocol.CodeMethodNotFound)
	}
}

// fullHandler implements every optional dns01 interface.
type fullHandler struct{ *recorder }

func (fullHandler) Validate(_ context.Context, _ string, config map[string]string) error {
	if config["EXEC_PATH"] == "" {
		return sdk.InvalidConfig("EXEC_PATH", "missing program path")
	}
	return nil
}

func (fullHandler) Options(context.Context, protocol.DNS01OptionsParams) (protocol.DNS01OptionsResult, error) {
	return protocol.DNS01OptionsResult{PropagationTimeoutSeconds: 120, PollingIntervalSeconds: 2}, nil
}

func (fullHandler) Check(context.Context, protocol.DNS01CheckParams) (protocol.DNS01CheckResult, error) {
	return protocol.DNS01CheckResult{Ready: true, EffectiveFQDN: "_acme-challenge.example.com."}, nil
}

func TestOptionalDNS01MethodsAreRegisteredWhenImplemented(t *testing.T) {
	h := newHarness(t, sdk.Plugin{DNS01: fullHandler{newRecorder()}})
	h.initialize(t)

	var opts protocol.DNS01OptionsResult
	if err := h.host.Call(t.Context(), protocol.MethodDNS01Options,
		protocol.DNS01OptionsParams{Provider: "exec"}, &opts); err != nil {
		t.Fatalf("dns01.options: %v", err)
	}
	if opts.PropagationTimeoutSeconds != 120 {
		t.Fatalf("timeout = %d", opts.PropagationTimeoutSeconds)
	}

	var check protocol.DNS01CheckResult
	if err := h.host.Call(t.Context(), protocol.MethodDNS01Check,
		protocol.DNS01CheckParams{Provider: "exec"}, &check); err != nil {
		t.Fatalf("dns01.check: %v", err)
	}
	if !check.Ready {
		t.Fatal("check.Ready = false")
	}

	// An invalid config maps to -32003 with the offending field.
	err := h.host.Call(t.Context(), protocol.MethodDNS01Validate,
		protocol.DNS01ValidateParams{Provider: "exec", Config: map[string]string{}}, nil)

	var rpcErr *protocol.Error
	if !errors.As(err, &rpcErr) {
		t.Fatalf("err = %v, want *protocol.Error", err)
	}
	if rpcErr.Code != protocol.CodeInvalidConfig {
		t.Fatalf("code = %d, want %d", rpcErr.Code, protocol.CodeInvalidConfig)
	}
	data, ok := rpcErr.Data.(map[string]any)
	if !ok || data["field"] != "EXEC_PATH" {
		t.Fatalf("data = %#v", rpcErr.Data)
	}
}

func TestRunReturnsOnStdinEOF(t *testing.T) {
	pluginIn, hostW := io.Pipe()
	_, pluginOut := io.Pipe()

	errCh := make(chan error, 1)
	go func() { errCh <- sdk.Run(t.Context(), sdk.Plugin{}, pluginIn, pluginOut) }()

	_ = hostW.Close()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run = %v, want nil on EOF", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after EOF")
	}
}

func TestHostCallsBeforeInitializedAreRejected(t *testing.T) {
	blocked := make(chan error, 1)

	h := newHarness(t, sdk.Plugin{
		Methods: map[string]sdk.Handler{
			"test.early": func(ctx context.Context, _ json.RawMessage) (any, error) {
				blocked <- sdk.HostFromContext(ctx).KVSet(ctx, "k", "v")
				return protocol.EmptyResult{}, nil
			},
		},
	})

	if err := h.host.Call(t.Context(), "test.early", nil, nil); err != nil {
		t.Fatalf("test.early: %v", err)
	}
	select {
	case err := <-blocked:
		if !errors.Is(err, sdk.ErrHostNotReady) {
			t.Fatalf("err = %v, want ErrHostNotReady", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not run")
	}
}

func TestRedact(t *testing.T) {
	cases := map[string]string{
		"":                 "",
		"abc":              "***",
		"abcd":             "****",
		"1234567890abcdef": "************cdef",
	}
	for in, want := range cases {
		if got := sdk.Redact(in); got != want {
			t.Fatalf("Redact(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestCapabilitiesOverride(t *testing.T) {
	h := newHarness(t, sdk.Plugin{Capabilities: []string{protocol.CapabilityHTTP}})

	res := h.initialize(t)
	if len(res.Capabilities) != 1 || res.Capabilities[0] != protocol.CapabilityHTTP {
		t.Fatalf("capabilities = %v, want [http]", res.Capabilities)
	}
}

func TestHostLogsListAndActivity(t *testing.T) {
	events := make(chan protocol.EventNotification, 2)

	h := newHarness(t, sdk.Plugin{
		Events: map[string]sdk.EventHandler{
			protocol.EventLogPathsChanged: func(_ context.Context, ev protocol.EventNotification) {
				events <- ev
			},
		},
		Methods: map[string]sdk.Handler{
			"test.logs": func(ctx context.Context, _ json.RawMessage) (any, error) {
				host := sdk.HostFromContext(ctx)
				logs, err := host.LogsList(ctx)
				if err != nil {
					return nil, err
				}
				stop, err := host.Activity(ctx, "indexing", "Nginx Log Indexing...")
				if err != nil {
					return nil, err
				}
				stop()
				return map[string]int{"logs": len(logs)}, nil
			},
		},
	})

	var (
		mu       sync.Mutex
		activity []protocol.HostActivitySetParams
	)
	h.host.Handle(protocol.MethodHostLogsList, func(context.Context, json.RawMessage) (any, error) {
		return protocol.HostLogsListResult{Logs: []protocol.HostLogFile{
			{Path: "/var/log/nginx/access.log", Type: protocol.LogTypeAccess, Source: protocol.LogSourceDefault},
			{Path: "/var/log/nginx/a.error.log", Type: protocol.LogTypeError, Source: protocol.LogSourceConfig, ConfigFile: "/etc/nginx/a.conf"},
		}}, nil
	})
	h.host.Handle(protocol.MethodHostActivitySet, func(_ context.Context, raw json.RawMessage) (any, error) {
		var p protocol.HostActivitySetParams
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, err
		}
		mu.Lock()
		activity = append(activity, p)
		mu.Unlock()
		return protocol.EmptyResult{}, nil
	})

	h.initialize(t)

	var res map[string]int
	if err := h.host.Call(t.Context(), "test.logs", nil, &res); err != nil {
		t.Fatalf("test.logs: %v", err)
	}
	if res["logs"] != 2 {
		t.Fatalf("logs = %d, want 2", res["logs"])
	}

	mu.Lock()
	got := append([]protocol.HostActivitySetParams(nil), activity...)
	mu.Unlock()
	want := []protocol.HostActivitySetParams{
		{Key: "indexing", Label: "Nginx Log Indexing...", Active: true},
		{Key: "indexing", Label: "Nginx Log Indexing...", Active: false},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("activity = %+v, want %+v", got, want)
	}

	// An event without a handler is ignored, the subscribed one is delivered.
	ctx := t.Context()
	_ = h.host.Notify(ctx, protocol.MethodEventsOn, protocol.EventNotification{Type: protocol.EventCertIssued, TS: 1})
	_ = h.host.Notify(ctx, protocol.MethodEventsOn, protocol.EventNotification{Type: protocol.EventLogPathsChanged, TS: 2})
	select {
	case ev := <-events:
		if ev.Type != protocol.EventLogPathsChanged || ev.TS != 2 {
			t.Fatalf("event = %+v", ev)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("log.paths_changed was not delivered")
	}
}

func TestHostNginxCalls(t *testing.T) {
	h := newHarness(t, sdk.Plugin{
		Methods: map[string]sdk.Handler{
			"test.nginx": func(ctx context.Context, _ json.RawMessage) (any, error) {
				host := sdk.HostFromContext(ctx)
				changed, include, err := host.NginxSnippetPut(ctx, "cache", "expires 1d;\n")
				if err != nil {
					return nil, err
				}
				snippets, err := host.NginxSnippetList(ctx)
				if err != nil {
					return nil, err
				}
				removed, err := host.NginxSnippetDelete(ctx, "cache")
				if err != nil {
					return nil, err
				}
				files, err := host.NginxConfigList(ctx)
				if err != nil {
					return nil, err
				}
				content, err := host.NginxConfigGet(ctx, files[0])
				if err != nil {
					return nil, err
				}
				sites, err := host.SitesList(ctx)
				if err != nil {
					return nil, err
				}
				certs, err := host.CertsList(ctx)
				if err != nil {
					return nil, err
				}
				return map[string]any{
					"changed": changed, "include": include, "snippets": len(snippets), "removed": removed,
					"content": content, "site": sites[0].Name, "cert": certs[0].NotAfter,
				}, nil
			},
		},
	})

	var put protocol.HostNginxSnippetPutParams
	h.host.Handle(protocol.MethodHostNginxSnippetPut, func(_ context.Context, raw json.RawMessage) (any, error) {
		if err := json.Unmarshal(raw, &put); err != nil {
			return nil, err
		}
		return protocol.HostNginxSnippetPutResult{Changed: true, Include: "include snippets/plugins/x/cache.conf;"}, nil
	})
	h.host.Handle(protocol.MethodHostNginxSnippetList, func(context.Context, json.RawMessage) (any, error) {
		return protocol.HostNginxSnippetListResult{Snippets: []protocol.HostNginxSnippet{{Name: "cache"}}}, nil
	})
	h.host.Handle(protocol.MethodHostNginxSnippetDelete, func(context.Context, json.RawMessage) (any, error) {
		return protocol.HostNginxSnippetDeleteResult{Removed: true}, nil
	})
	h.host.Handle(protocol.MethodHostNginxConfigList, func(context.Context, json.RawMessage) (any, error) {
		return protocol.HostNginxConfigListResult{Files: []string{"nginx.conf"}}, nil
	})
	h.host.Handle(protocol.MethodHostNginxConfigGet, func(_ context.Context, raw json.RawMessage) (any, error) {
		var p protocol.HostNginxConfigGetParams
		if err := json.Unmarshal(raw, &p); err != nil || p.Path != "nginx.conf" {
			return nil, fmt.Errorf("path = %q", p.Path)
		}
		return protocol.HostNginxConfigGetResult{Content: "events {}"}, nil
	})
	h.host.Handle(protocol.MethodHostSitesList, func(context.Context, json.RawMessage) (any, error) {
		return protocol.HostSitesListResult{Sites: []protocol.HostSite{{Name: "a.test"}}}, nil
	})
	h.host.Handle(protocol.MethodHostCertsList, func(context.Context, json.RawMessage) (any, error) {
		return protocol.HostCertsListResult{Certs: []protocol.HostCert{{NotAfter: "2026-12-01T00:00:00Z"}}}, nil
	})

	h.initialize(t)

	var res map[string]any
	if err := h.host.Call(t.Context(), "test.nginx", nil, &res); err != nil {
		t.Fatalf("test.nginx: %v", err)
	}
	want := map[string]any{
		"changed": true, "include": "include snippets/plugins/x/cache.conf;", "snippets": float64(1), "removed": true,
		"content": "events {}", "site": "a.test", "cert": "2026-12-01T00:00:00Z",
	}
	if !maps.Equal(res, want) {
		t.Fatalf("result = %v, want %v", res, want)
	}
	if put.Name != "cache" || put.Content != "expires 1d;\n" {
		t.Fatalf("put = %+v", put)
	}
}
