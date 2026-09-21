package sdk

import (
	"context"

	"github.com/0xJacky/nginx-ui-plugin-sdk-go/protocol"
)

// DNS01Request is the payload of dns01.present and dns01.cleanup.
type DNS01Request = protocol.DNS01ChallengeParams

// DNS01Handler solves the DNS-01 challenge for one or more vendors.
type DNS01Handler interface {
	// Present publishes the TXT record for the challenge.
	Present(ctx context.Context, req DNS01Request) error
	// CleanUp removes the record Present published.
	CleanUp(ctx context.Context, req DNS01Request) error
}

// DNS01Validator is implemented by handlers that can check credentials without
// touching a certificate. Handlers that do not implement it make the SDK reply
// Unsupported to dns01.validate.
type DNS01Validator interface {
	Validate(ctx context.Context, provider string, config map[string]string) error
}

// DNS01OptionsProvider is implemented by handlers that report propagation
// timings. Handlers that do not implement it make the SDK reply Unsupported to
// dns01.options, and the host falls back to its own defaults.
type DNS01OptionsProvider interface {
	Options(ctx context.Context, params protocol.DNS01OptionsParams) (protocol.DNS01OptionsResult, error)
}

// DNS01Checker is implemented by handlers that run their own propagation
// check. Handlers that do not implement it make the SDK reply Unsupported to
// dns01.check.
type DNS01Checker interface {
	Check(ctx context.Context, params protocol.DNS01CheckParams) (protocol.DNS01CheckResult, error)
}
