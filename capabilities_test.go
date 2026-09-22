package sdk_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	sdk "github.com/0xJacky/nginx-ui-plugin-sdk-go"
	"github.com/0xJacky/nginx-ui-plugin-sdk-go/protocol"
)

// notifier is a NotifyHandler that remembers what it was asked to send.
type notifier struct {
	sent chan sdk.NotifyRequest
}

func (n *notifier) Send(_ context.Context, req sdk.NotifyRequest) error {
	if req.Config["webhook_url"] == "" {
		return sdk.InvalidConfig("webhook_url", "webhook_url is required")
	}
	n.sent <- req
	return nil
}

// validatingNotifier also implements NotifyValidator.
type validatingNotifier struct{ *notifier }

func (validatingNotifier) Validate(_ context.Context, channel string, config map[string]string) error {
	if channel != "mychat" {
		return sdk.InvalidConfig("channel", "unknown channel")
	}
	if config["webhook_url"] == "" {
		return sdk.InvalidConfig("webhook_url", "webhook_url is required")
	}
	return nil
}

// prober is a ProbeHandler that reports what the context deadline was.
type prober struct {
	deadline chan time.Duration
}

func (p *prober) Check(ctx context.Context, req sdk.ProbeRequest) (sdk.ProbeResult, error) {
	if deadline, ok := ctx.Deadline(); ok {
		p.deadline <- time.Until(deadline)
	} else {
		p.deadline <- 0
	}
	if req.Config["port"] == "ssh" {
		return sdk.ProbeResult{}, sdk.InvalidConfig("port", "port must be a number")
	}
	if req.Target == "http://down.invalid" {
		return sdk.ProbeDown(1500*time.Millisecond, "connection refused"), nil
	}
	return sdk.ProbeUp(42 * time.Millisecond), nil
}

func invalidConfigField(t *testing.T, err error) string {
	t.Helper()
	var rpcErr *protocol.Error
	if !errors.As(err, &rpcErr) {
		t.Fatalf("err = %v, want *protocol.Error", err)
	}
	if rpcErr.Code != protocol.CodeInvalidConfig {
		t.Fatalf("code = %d, want %d", rpcErr.Code, protocol.CodeInvalidConfig)
	}
	data, ok := rpcErr.Data.(map[string]any)
	if !ok {
		t.Fatalf("data = %#v, want an object", rpcErr.Data)
	}
	field, _ := data["field"].(string)
	return field
}

func errorCode(t *testing.T, err error) int {
	t.Helper()
	var rpcErr *protocol.Error
	if !errors.As(err, &rpcErr) {
		t.Fatalf("err = %v, want *protocol.Error", err)
	}
	return rpcErr.Code
}

func TestNotifySendReachesTheHandler(t *testing.T) {
	n := &notifier{sent: make(chan sdk.NotifyRequest, 1)}
	h := newHarness(t, sdk.Plugin{Notify: n})

	res := h.initialize(t)
	if !slices.Equal(res.Capabilities, []string{protocol.CapabilityNotify}) {
		t.Fatalf("capabilities = %v, want [notify]", res.Capabilities)
	}

	params := protocol.NotifySendParams{
		Channel:  "mychat",
		Config:   map[string]string{"webhook_url": "https://chat.example/hook"},
		Title:    "Certificate Expiring Soon",
		Content:  "example.com expires in 7 days",
		Severity: protocol.NotifySeverityWarning,
	}
	var result map[string]any
	if err := h.host.Call(t.Context(), protocol.MethodNotifySend, params, &result); err != nil {
		t.Fatalf("notify.send: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("result = %v, want {}", result)
	}
	got := <-n.sent
	if got.Channel != "mychat" || got.Title != params.Title || got.Severity != protocol.NotifySeverityWarning {
		t.Fatalf("handler got %+v", got)
	}

	params.Config = map[string]string{}
	err := h.host.Call(t.Context(), protocol.MethodNotifySend, params, nil)
	if field := invalidConfigField(t, err); field != "webhook_url" {
		t.Fatalf("field = %q, want webhook_url", field)
	}
}

func TestNotifyValidateIsUnsupportedWithoutAValidator(t *testing.T) {
	h := newHarness(t, sdk.Plugin{Notify: &notifier{sent: make(chan sdk.NotifyRequest, 1)}})
	h.initialize(t)

	err := h.host.Call(t.Context(), protocol.MethodNotifyValidate,
		protocol.NotifyValidateParams{Channel: "mychat"}, nil)
	if code := errorCode(t, err); code != protocol.CodeUnsupported {
		t.Fatalf("code = %d, want %d", code, protocol.CodeUnsupported)
	}
}

func TestNotifyValidateUsesTheValidator(t *testing.T) {
	h := newHarness(t, sdk.Plugin{Notify: validatingNotifier{&notifier{sent: make(chan sdk.NotifyRequest, 1)}}})
	h.initialize(t)

	err := h.host.Call(t.Context(), protocol.MethodNotifyValidate,
		protocol.NotifyValidateParams{Channel: "mychat", Config: map[string]string{}}, nil)
	if field := invalidConfigField(t, err); field != "webhook_url" {
		t.Fatalf("field = %q, want webhook_url", field)
	}

	if err = h.host.Call(t.Context(), protocol.MethodNotifyValidate, protocol.NotifyValidateParams{
		Channel: "mychat",
		Config:  map[string]string{"webhook_url": "https://chat.example/hook"},
	}, nil); err != nil {
		t.Fatalf("notify.validate: %v", err)
	}
}

func TestProbeCheckAppliesTheTimeout(t *testing.T) {
	p := &prober{deadline: make(chan time.Duration, 4)}
	h := newHarness(t, sdk.Plugin{Probe: p})

	res := h.initialize(t)
	if !slices.Equal(res.Capabilities, []string{protocol.CapabilityProbe}) {
		t.Fatalf("capabilities = %v, want [probe]", res.Capabilities)
	}

	var result protocol.ProbeCheckResult
	if err := h.host.Call(t.Context(), protocol.MethodProbeCheck, protocol.ProbeCheckParams{
		Kind: "tcp-banner", Target: "https://example.com", TimeoutSeconds: 5,
	}, &result); err != nil {
		t.Fatalf("probe.check: %v", err)
	}
	if result.Status != protocol.ProbeStatusUp || result.LatencyMS != 42 {
		t.Fatalf("result = %+v, want up in 42ms", result)
	}
	if remaining := <-p.deadline; remaining <= 0 || remaining > 5*time.Second {
		t.Fatalf("handler deadline in %s, want within 5s", remaining)
	}

	if err := h.host.Call(t.Context(), protocol.MethodProbeCheck, protocol.ProbeCheckParams{
		Kind: "tcp-banner", Target: "http://down.invalid",
	}, &result); err != nil {
		t.Fatalf("probe.check: %v", err)
	}
	<-p.deadline
	if result.Status != protocol.ProbeStatusDown || result.Message != "connection refused" || result.LatencyMS != 1500 {
		t.Fatalf("result = %+v, want down with a message", result)
	}

	err := h.host.Call(t.Context(), protocol.MethodProbeCheck, protocol.ProbeCheckParams{
		Kind: "tcp-banner", Target: "https://example.com", Config: map[string]string{"port": "ssh"},
	}, nil)
	<-p.deadline
	if field := invalidConfigField(t, err); field != "port" {
		t.Fatalf("field = %q, want port", field)
	}
}

func TestMCPToolsDispatchByName(t *testing.T) {
	var gotArgs map[string]any
	h := newHarness(t, sdk.Plugin{MCP: sdk.MCPTools{
		"purge_cache": func(_ context.Context, args map[string]any) (sdk.MCPResult, error) {
			gotArgs = args
			zone, _ := args["zone"].(string)
			if zone == "" {
				return sdk.MCPError("zone is required"), nil
			}
			return sdk.MCPText("purged " + zone), nil
		},
		"noop": func(context.Context, map[string]any) (sdk.MCPResult, error) {
			return sdk.MCPResult{}, nil
		},
	}})

	res := h.initialize(t)
	if !slices.Equal(res.Capabilities, []string{protocol.CapabilityMCP}) {
		t.Fatalf("capabilities = %v, want [mcp]", res.Capabilities)
	}

	var result protocol.MCPCallResult
	if err := h.host.Call(t.Context(), protocol.MethodMCPCall, protocol.MCPCallParams{
		Tool: "purge_cache", Arguments: map[string]any{"zone": "example.com"},
	}, &result); err != nil {
		t.Fatalf("mcp.call: %v", err)
	}
	if result.IsError || len(result.Content) != 1 || result.Content[0].Text != "purged example.com" ||
		result.Content[0].Type != protocol.MCPContentTypeText {
		t.Fatalf("result = %+v", result)
	}
	if gotArgs["zone"] != "example.com" {
		t.Fatalf("args = %v", gotArgs)
	}

	result = protocol.MCPCallResult{}
	if err := h.host.Call(t.Context(), protocol.MethodMCPCall, protocol.MCPCallParams{Tool: "purge_cache"}, &result); err != nil {
		t.Fatalf("mcp.call: %v", err)
	}
	if !result.IsError || result.Content[0].Text != "zone is required" {
		t.Fatalf("result = %+v, want a tool error", result)
	}
	if gotArgs == nil {
		t.Fatal("a call without arguments must hand the tool an empty map")
	}

	var raw map[string]any
	if err := h.host.Call(t.Context(), protocol.MethodMCPCall, protocol.MCPCallParams{Tool: "noop"}, &raw); err != nil {
		t.Fatalf("mcp.call: %v", err)
	}
	if content, ok := raw["content"].([]any); !ok || len(content) != 0 {
		t.Fatalf("result = %v, want an empty content list", raw)
	}

	err := h.host.Call(t.Context(), protocol.MethodMCPCall, protocol.MCPCallParams{Tool: "drop_database"}, nil)
	if code := errorCode(t, err); code != protocol.CodeInvalidParams {
		t.Fatalf("code = %d, want %d", code, protocol.CodeInvalidParams)
	}
}

func TestCapabilitiesFollowTheHandlers(t *testing.T) {
	h := newHarness(t, sdk.Plugin{
		DNS01:  newRecorder(),
		Notify: &notifier{sent: make(chan sdk.NotifyRequest, 1)},
		Probe:  &prober{deadline: make(chan time.Duration, 1)},
		MCP:    sdk.MCPTools{},
	})

	res := h.initialize(t)
	want := []string{protocol.CapabilityDNS01, protocol.CapabilityNotify, protocol.CapabilityProbe, protocol.CapabilityMCP}
	if !slices.Equal(res.Capabilities, want) {
		t.Fatalf("capabilities = %v, want %v", res.Capabilities, want)
	}
}

func TestUnservedCapabilityMethodsAreUnknown(t *testing.T) {
	h := newHarness(t, sdk.Plugin{DNS01: newRecorder()})
	h.initialize(t)

	for _, method := range []string{protocol.MethodNotifySend, protocol.MethodProbeCheck, protocol.MethodMCPCall} {
		err := h.host.Call(t.Context(), method, map[string]any{}, nil)
		if code := errorCode(t, err); code != protocol.CodeMethodNotFound {
			t.Fatalf("%s: code = %d, want %d", method, code, protocol.CodeMethodNotFound)
		}
	}
}
