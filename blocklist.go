package sdk

import (
	"context"
	"encoding/json"

	"github.com/0xJacky/nginx-ui-plugin-sdk-go/protocol"
)

// BlocklistRequest is the payload of blocklist.fetch.
type BlocklistRequest = protocol.BlocklistFetchParams

// BlocklistResult is the reply to blocklist.fetch.
type BlocklistResult = protocol.BlocklistFetchResult

// BlocklistEntry is one address or network a BlocklistResult denies.
type BlocklistEntry = protocol.BlocklistEntry

// BlocklistHandler fetches lists of addresses to deny from the kinds of
// source the manifest declares in its blocklist block. req.Source is the
// source kind code and req.Config holds the values of its form. The manifest
// must request the network permission.
type BlocklistHandler interface {
	// Fetch returns the complete current list of the source, not the
	// changes since the last call. Each entry is an IPv4 or IPv6 address or
	// a CIDR network; the host drops anything else. An empty list denies
	// nothing, so when the list cannot be fetched return an error instead:
	// InvalidConfig for a bad field, any other error for a source failure.
	// The host then keeps the list it has.
	Fetch(ctx context.Context, req BlocklistRequest) (BlocklistResult, error)
}

// Deny is a BlocklistEntry for an address or a CIDR network.
func Deny(cidr, reason string) BlocklistEntry {
	return BlocklistEntry{CIDR: cidr, Reason: reason}
}

// registerBlocklist wires the security.blocklist method onto the connection.
func (rt *runtime) registerBlocklist() {
	rt.conn.Handle(protocol.MethodBlocklistFetch, rt.track(rt.onBlocklistFetch))
}

func (rt *runtime) onBlocklistFetch(ctx context.Context, raw json.RawMessage) (any, error) {
	req, err := decode[BlocklistRequest](raw)
	if err != nil {
		return nil, err
	}
	result, err := rt.plugin.Blocklist.Fetch(WithHost(ctx, rt.host), req)
	if err != nil {
		return nil, err
	}
	// An empty list travels as [] so the host never mistakes it for a
	// malformed reply.
	if result.Entries == nil {
		result.Entries = []BlocklistEntry{}
	}
	return result, nil
}
