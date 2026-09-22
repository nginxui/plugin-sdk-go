package sdk

import (
	"context"
	"encoding/json"

	"github.com/0xJacky/nginx-ui-plugin-sdk-go/protocol"
)

// DiscoveryRequest is the payload of discovery.resolve.
type DiscoveryRequest = protocol.DiscoveryResolveParams

// DiscoveryResult is the reply to discovery.resolve.
type DiscoveryResult = protocol.DiscoveryResolveResult

// DiscoveryTarget is one server a DiscoveryResult lists.
type DiscoveryTarget = protocol.DiscoveryTarget

// DiscoveryHandler resolves services into their servers through the
// providers the manifest declares in its discovery block. req.Provider is the
// provider code, req.Config holds the values of its form and req.Service
// names the service. The manifest must request the network permission.
type DiscoveryHandler interface {
	// Resolve returns every server that backs the service right now: an IP
	// address or a host name without a port, a port from 1 to 65535 and a
	// weight from 0 to 1000, where 0 means 1. When the service cannot be
	// resolved return an error, so the host keeps the servers it has:
	// InvalidConfig for a bad field (UnknownService for a service the
	// provider does not know), any other error for a provider failure.
	Resolve(ctx context.Context, req DiscoveryRequest) (DiscoveryResult, error)
}

// Target is a DiscoveryTarget with weight 1.
func Target(address string, port int, tags ...string) DiscoveryTarget {
	return DiscoveryTarget{Address: address, Port: port, Weight: 1, Tags: tags}
}

// UnknownService reports a service the provider does not know. It is an
// invalid configuration naming the service field, since the person typed it.
func UnknownService(service string) *protocol.Error {
	return InvalidConfig("service", "unknown service: "+service)
}

// registerDiscovery wires the upstream.discovery method onto the connection.
func (rt *runtime) registerDiscovery() {
	rt.conn.Handle(protocol.MethodDiscoveryResolve, rt.track(rt.onDiscoveryResolve))
}

func (rt *runtime) onDiscoveryResolve(ctx context.Context, raw json.RawMessage) (any, error) {
	req, err := decode[DiscoveryRequest](raw)
	if err != nil {
		return nil, err
	}
	result, err := rt.plugin.Discovery.Resolve(WithHost(ctx, rt.host), req)
	if err != nil {
		return nil, err
	}
	if result.Targets == nil {
		result.Targets = []DiscoveryTarget{}
	}
	return result, nil
}
