# nginx-ui-plugin-sdk-go

Go SDK for writing [NGINX UI](https://github.com/0xJacky/nginx-ui) plugins.

A plugin is an ordinary executable. The host starts it, speaks bidirectional
JSON-RPC 2.0 framed as NDJSON over the plugin's **stdin** and **stdout**, and
stops it again. `stdout` carries protocol traffic only; every human readable
line must go to `stderr`, which the SDK logger does for you.

```bash
go get github.com/0xJacky/nginx-ui-plugin-sdk-go
```

## Example

```go
package main

import (
	"context"
	"fmt"

	sdk "github.com/0xJacky/nginx-ui-plugin-sdk-go"
	"github.com/0xJacky/nginx-ui-plugin-sdk-go/protocol"
)

type provider struct{}

// Present publishes the challenge TXT record.
func (provider) Present(ctx context.Context, req sdk.DNS01Request) error {
	token := req.Config["MY_API_TOKEN"]
	if token == "" {
		return sdk.InvalidConfig("MY_API_TOKEN", "the API token is required")
	}

	sdk.Infof("publishing %s for %s (token %s)", req.EffectiveFQDN, req.Domain, sdk.Redact(token))

	// Talk to the vendor API here.
	return nil
}

// CleanUp removes what Present published.
func (provider) CleanUp(ctx context.Context, req sdk.DNS01Request) error {
	sdk.Infof("removing %s", req.EffectiveFQDN)
	return nil
}

// Options is optional: report the propagation timings to the host.
func (provider) Options(context.Context, protocol.DNS01OptionsParams) (protocol.DNS01OptionsResult, error) {
	return protocol.DNS01OptionsResult{PropagationTimeoutSeconds: 120, PollingIntervalSeconds: 2}, nil
}

func main() {
	sdk.Serve(sdk.Plugin{
		DNS01: provider{},
		Configure: func(ctx context.Context, settings map[string]any) error {
			// Called on plugin.configure and whenever the user saves settings.
			if host := sdk.HostFromContext(ctx); host != nil {
				return host.Notify(ctx, "info", "Reconfigured", fmt.Sprint(len(settings), " settings"), nil)
			}
			return nil
		},
	})
}
```

`sdk.Serve` blocks: it registers the lifecycle methods, wires the capability
methods your handler implements, and exits on `plugin.exit`, on end of stdin
or on SIGINT/SIGTERM. Use `sdk.Run(ctx, plugin, r, w)` in tests to drive the
same wiring over an in-memory pipe.

## Packages

| Package | Contents |
| --- | --- |
| `sdk` (root) | `Plugin`, `Serve`, `Run`, the `DNS01*` interfaces, the `Host` client, errors and the logger |
| `sdk/protocol` | The wire types, method names, capability, permission and error-code constants. Mirrors `internal/plugin/protocol` of nginx-ui |
| `sdk/jsonrpc` | The bidirectional NDJSON JSON-RPC 2.0 peer, usable on its own |
| `sdk/pb` | Generated protobuf and gRPC bindings of the contract (package `pluginv1`), copied from the spec repository |

## The proto contract and `pb`

The wire contract is defined in proto, in
[nginx-ui-plugin-spec](https://github.com/0xJacky/nginx-ui-plugin-spec)
under `proto/nginxui/plugin/v1`. A JSON-RPC `method` is the rpc's `rpc_name`
option and `params` / `result` are the protobuf JSON mapping of its messages
with proto field names, so the JSON the SDK exchanges is exactly what the
proto describes.

`pb` is a verbatim copy of the spec repository's generated `gen/go` package:
message types such as `pluginv1.DNS01PresentRequest`, the `rpc_name` and
`notification` options, and gRPC clients and servers for the `Plugin`,
`Host`, `DNS01`, `HTTP` and `Events` services. The plugin runtime in this
module keeps using the hand-written `protocol` types; `pb` is there for
reflection, for gRPC and for code that prefers generated types.
`protocol/alignment_test.go` fails when a `protocol` type drifts from its
proto message, when a method constant has no rpc, or when an error code
differs from the `ErrorCode` enum.

To pick up a contract change, run `make generate` in the spec repository,
then `pb/regen.sh` here (it expects the spec checkout next to this
repository, or `SPEC_DIR`), then update `protocol` until `go test ./...`
passes. Do not link `pb` into one binary together with another copy of the
same generated package: the protobuf runtime rejects duplicate registrations
of `nginxui.plugin.v1`.

## Lifecycle

| Method | Direction | Meaning |
| --- | --- | --- |
| `plugin.initialize` | host → plugin | Handshake. The reply carries `api_version` and the implemented capabilities |
| `plugin.initialized` | host → plugin | Notification. `host.*` calls are allowed from here on |
| `plugin.configure` | host → plugin | New settings map |
| `plugin.ping` | host → plugin | Liveness probe, replies `{}` |
| `plugin.shutdown` | host → plugin | Finish in-flight work, then reply `{}` |
| `plugin.exit` | host → plugin | Notification. Exit now |

Requests are matched by id and may be concurrent. Notifications carry no id and
are never answered. A message larger than 4 MiB is rejected.

## Error codes

| Code | Helper | Meaning |
| --- | --- | --- |
| `-32601` | — | Unknown method |
| `-32602` | `sdk.InvalidParams` | Malformed params |
| `-32000` | `sdk.Internal` | Internal failure |
| `-32002` | `sdk.Unsupported` | Capability method the plugin does not implement |
| `-32003` | `sdk.InvalidConfig` | Bad credential or setting, `data.field` names it |

Returning any other Go error from a handler becomes `-32000`.

## The host client

Once `plugin.initialized` arrived, `sdk.HostFromContext(ctx)` (or
`sdk.CurrentHost()`) returns a client for the `host.*` side of the protocol:
`Log`, `KVGet` / `KVSet` / `KVDelete` / `KVList`, `SettingsGet`, `Locale`,
`CredentialsGet`, `CronRegister` / `CronUnregister`, `Notify` and
`MetricsSnapshot`. Each call needs the matching manifest permission; without it
the host answers `-32001`. `Settings()` returns the latest settings map and
`Info()` the plugin id, data directory and host information.

## Environment

| Variable | Meaning |
| --- | --- |
| `NGINX_UI_PLUGIN_ID` | The plugin id from the manifest |
| `NGINX_UI_PLUGIN_API_VERSION` | The protocol version the host speaks |
| `NGINX_UI_PLUGIN_DATA_DIR` | The only directory the plugin may write to |
| `NGINX_UI_VERSION` | The host version |

## License

AGPL-3.0. See [LICENSE](LICENSE).
