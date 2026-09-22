package sdk_test

import (
	"context"
	"encoding/json"
	"slices"
	"testing"

	sdk "github.com/0xJacky/nginx-ui-plugin-sdk-go"
	"github.com/0xJacky/nginx-ui-plugin-sdk-go/protocol"
)

// feed is a BlocklistHandler with a fixed list.
type feed struct{}

func (feed) Fetch(_ context.Context, req sdk.BlocklistRequest) (sdk.BlocklistResult, error) {
	if req.Config["api_key"] == "" {
		return sdk.BlocklistResult{}, sdk.InvalidConfig("api_key", "api_key is required")
	}
	if req.Config["api_key"] == "empty" {
		return sdk.BlocklistResult{}, nil
	}
	return sdk.BlocklistResult{
		Entries: []sdk.BlocklistEntry{
			sdk.Deny("198.51.100.23", "SSH brute force"),
			sdk.Deny("2001:db8:bad::/48", ""),
		},
		TTLSeconds: 900,
	}, nil
}

// registry is a DiscoveryHandler that knows one service.
type registry struct{}

func (registry) Resolve(_ context.Context, req sdk.DiscoveryRequest) (sdk.DiscoveryResult, error) {
	if req.Config["address"] == "" {
		return sdk.DiscoveryResult{}, sdk.InvalidConfig("address", "address is required")
	}
	switch req.Service {
	case "api":
		return sdk.DiscoveryResult{
			Targets: []sdk.DiscoveryTarget{
				sdk.Target("10.0.1.12", 8080, "zone-a"),
				{Address: "10.0.2.7", Port: 8080, Weight: 2},
			},
			TTLSeconds: 30,
		}, nil
	case "idle":
		return sdk.DiscoveryResult{}, nil
	default:
		return sdk.DiscoveryResult{}, sdk.UnknownService(req.Service)
	}
}

func TestBlocklistFetchReachesTheHandler(t *testing.T) {
	h := newHarness(t, sdk.Plugin{Blocklist: feed{}})

	res := h.initialize(t)
	if !slices.Equal(res.Capabilities, []string{protocol.CapabilitySecurityBlocklist}) {
		t.Fatalf("capabilities = %v, want [security.blocklist]", res.Capabilities)
	}

	var result protocol.BlocklistFetchResult
	params := protocol.BlocklistFetchParams{Source: "threatfeed", Config: map[string]string{"api_key": "k"}}
	if err := h.host.Call(t.Context(), protocol.MethodBlocklistFetch, params, &result); err != nil {
		t.Fatalf("blocklist.fetch: %v", err)
	}
	if len(result.Entries) != 2 || result.Entries[0].CIDR != "198.51.100.23" || result.Entries[0].Reason != "SSH brute force" ||
		result.Entries[1].CIDR != "2001:db8:bad::/48" || result.TTLSeconds != 900 {
		t.Fatalf("result = %+v", result)
	}

	// An empty list is sent as an array.
	var raw map[string]any
	params.Config["api_key"] = "empty"
	if err := h.host.Call(t.Context(), protocol.MethodBlocklistFetch, params, &raw); err != nil {
		t.Fatalf("blocklist.fetch: %v", err)
	}
	if entries, ok := raw["entries"].([]any); !ok || len(entries) != 0 {
		t.Fatalf("result = %v, want an empty entries list", raw)
	}

	err := h.host.Call(t.Context(), protocol.MethodBlocklistFetch, protocol.BlocklistFetchParams{Source: "threatfeed"}, nil)
	if field := invalidConfigField(t, err); field != "api_key" {
		t.Fatalf("field = %q, want api_key", field)
	}

	// A malformed payload is invalid params, not a handler call.
	err = h.host.Call(t.Context(), protocol.MethodBlocklistFetch, json.RawMessage(`"threatfeed"`), nil)
	if code := errorCode(t, err); code != protocol.CodeInvalidParams {
		t.Fatalf("code = %d, want %d", code, protocol.CodeInvalidParams)
	}
}

func TestDiscoveryResolveReachesTheHandler(t *testing.T) {
	h := newHarness(t, sdk.Plugin{Discovery: registry{}})

	res := h.initialize(t)
	if !slices.Equal(res.Capabilities, []string{protocol.CapabilityUpstreamDiscovery}) {
		t.Fatalf("capabilities = %v, want [upstream.discovery]", res.Capabilities)
	}

	config := map[string]string{"address": "https://registry.example.com"}
	var result protocol.DiscoveryResolveResult
	if err := h.host.Call(t.Context(), protocol.MethodDiscoveryResolve, protocol.DiscoveryResolveParams{
		Provider: "registry", Config: config, Service: "api",
	}, &result); err != nil {
		t.Fatalf("discovery.resolve: %v", err)
	}
	if len(result.Targets) != 2 || result.TTLSeconds != 30 {
		t.Fatalf("result = %+v", result)
	}
	first, second := result.Targets[0], result.Targets[1]
	if first.Address != "10.0.1.12" || first.Port != 8080 || first.Weight != 1 || !slices.Equal(first.Tags, []string{"zone-a"}) {
		t.Fatalf("first target = %+v", first)
	}
	if second.Weight != 2 || second.Tags != nil {
		t.Fatalf("second target = %+v", second)
	}

	var raw map[string]any
	if err := h.host.Call(t.Context(), protocol.MethodDiscoveryResolve, protocol.DiscoveryResolveParams{
		Provider: "registry", Config: config, Service: "idle",
	}, &raw); err != nil {
		t.Fatalf("discovery.resolve: %v", err)
	}
	if targets, ok := raw["targets"].([]any); !ok || len(targets) != 0 {
		t.Fatalf("result = %v, want an empty targets list", raw)
	}

	err := h.host.Call(t.Context(), protocol.MethodDiscoveryResolve, protocol.DiscoveryResolveParams{
		Provider: "registry", Config: config, Service: "billing",
	}, nil)
	if field := invalidConfigField(t, err); field != "service" {
		t.Fatalf("field = %q, want service", field)
	}
	err = h.host.Call(t.Context(), protocol.MethodDiscoveryResolve, protocol.DiscoveryResolveParams{Provider: "registry", Service: "api"}, nil)
	if field := invalidConfigField(t, err); field != "address" {
		t.Fatalf("field = %q, want address", field)
	}
}
