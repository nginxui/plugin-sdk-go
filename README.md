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
same wiring over an in-memory pipe. Both accept options, see
[Transports](#transports).

## Capabilities

Each capability is one field of `sdk.Plugin`. Setting it wires the methods of
the capability and adds its name to the `capabilities` the plugin reports in
the handshake, which must match the manifest (`Plugin.Capabilities` overrides
the derived list). The manifest block of each capability is described in the
[specification](https://github.com/0xJacky/nginx-ui-plugin-spec).

| Field | Capability | Methods | Handler |
| --- | --- | --- | --- |
| `DNS01` | `dns01` | `dns01.present`, `dns01.cleanup`, optional `dns01.validate`, `dns01.options`, `dns01.check` | `DNS01Handler`, plus `DNS01Validator`, `DNS01OptionsProvider`, `DNS01Checker` |
| `Notify` | `notify` | `notify.send`, optional `notify.validate` | `NotifyHandler`, plus `NotifyValidator` |
| `Probe` | `probe` | `probe.check` | `ProbeHandler` |
| `MCP` | `mcp` | `mcp.call` | `MCPHandler`, or the ready-made `MCPTools` map |
| `Storage` | `storage` | `storage.put`, `storage.get`, `storage.list`, `storage.delete`, optional `storage.validate` | `StorageHandler`, plus `StorageValidator` |
| `Deploy` | `cert.deploy` | `deploy.push`, optional `deploy.validate` | `DeployHandler`, plus `DeployValidator` |
| `Blocklist` | `security.blocklist` | `blocklist.fetch` | `BlocklistHandler` |
| `Discovery` | `upstream.discovery` | `discovery.resolve` | `DiscoveryHandler` |

An optional method the handler does not implement answers `-32002`
(Unsupported), and the host falls back or treats it as "no opinion".

### Notification channels

The host offers every channel of the manifest's `notify` block next to its
built-in notification channels and calls `Send` whenever a notification is
routed to one. `req.Config` holds the values of the channel form, `req.Title`
and `req.Content` are already translated plain text, and `req.Severity` is
`info`, `success`, `warning` or `error`.

```go
type chat struct{}

func (chat) Send(ctx context.Context, req sdk.NotifyRequest) error {
	hook := req.Config["webhook_url"]
	if hook == "" {
		return sdk.InvalidConfig("webhook_url", "webhook_url is required")
	}
	// Post req.Title and req.Content to the vendor here.
	return nil
}

// Validate is optional. It must not send anything.
func (chat) Validate(ctx context.Context, channel string, config map[string]string) error {
	if !strings.HasPrefix(config["webhook_url"], "https://") {
		return sdk.InvalidConfig("webhook_url", "webhook_url must be an https URL")
	}
	return nil
}
```

### Health check probes

The host offers every kind of the manifest's `probe` block as a check method
of a site's health check and calls `Check` on its schedule. An unhealthy or
unreachable target is a result, not an error: return `sdk.ProbeDown` with a
message, and an error only when the check itself could not run. The context
expires after `req.TimeoutSeconds`.

```go
type banner struct{}

func (banner) Check(ctx context.Context, req sdk.ProbeRequest) (sdk.ProbeResult, error) {
	started := time.Now()
	u, err := url.Parse(req.Target)
	if err != nil {
		return sdk.ProbeResult{}, sdk.InvalidConfig("target", "target is not a URL")
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort(u.Hostname(), req.Config["port"]))
	if err != nil {
		return sdk.ProbeDown(time.Since(started), err.Error()), nil
	}
	defer conn.Close()
	return sdk.ProbeUp(time.Since(started)), nil
}
```

### MCP tools

The host publishes every tool of the manifest's `mcp` block on its Model
Context Protocol server under the name `<plugin id with dots replaced by
underscores>__<tool name>` and forwards each call with the unprefixed name.
The manifest must request the `mcp` permission. Arguments come from an AI
assistant: validate them before use. Return `sdk.MCPError` for a tool that
ran and failed, so the assistant sees why; `MCPTools` answers an unknown tool
with `-32602`.

```go
sdk.Serve(sdk.Plugin{MCP: sdk.MCPTools{
	"purge_cache": func(ctx context.Context, args map[string]any) (sdk.MCPResult, error) {
		zone, _ := args["zone"].(string)
		if zone == "" {
			return sdk.MCPError("zone is required"), nil
		}
		return sdk.MCPText("purged " + zone), nil
	},
}})
```

### Storage backends

The host offers every backend of the manifest's `storage` block next to its
built-in storage, for example as a destination of automatic backups. File
contents never travel in a message: for `Put` the host has placed the file at
`req.SourcePath`, for `Get` you write the object to `req.TargetPath`, both
inside `<data dir>/exchange/`, and the host removes them after your reply.
Keys are relative `/` separated paths; the SDK answers a malformed key with
`-32602` before your handler runs, so building a vendor path from it is safe.
`Delete` of a missing object must succeed, and `List` takes a plain string
prefix.

```go
type dav struct{}

func (dav) Put(ctx context.Context, req sdk.StoragePutRequest) (int64, error) {
	f, err := os.Open(req.SourcePath)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	// Upload f to req.Config["url"] + "/" + req.Key here.
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (dav) Get(ctx context.Context, req sdk.StorageGetRequest) (int64, error) {
	// Download req.Key into a new file at req.TargetPath here.
	return 0, sdk.Internal("not implemented")
}

func (dav) List(ctx context.Context, req sdk.StorageListRequest) ([]sdk.StorageObject, error) {
	// Return sdk.StoredObject(key, size, modified) for every key with req.Prefix.
	return nil, nil
}

func (dav) Delete(ctx context.Context, req sdk.StorageDeleteRequest) error {
	return nil
}
```

### Certificate deployment

The host pushes a certificate to every target a person bound to it after
each issuance or renewal, and on demand. `req.Certificate` carries the leaf,
its chain and its private key as PEM (`sdk.FullChainPEM` joins leaf and
chain); `req.DryRun` asks you to check the target without changing anything.
Because the request carries the private key, the manifest must request the
`cert.deploy` permission, and the key must never reach a log line or an
error. A push must be idempotent: the host retries failures.

```go
type cdn struct{}

func (cdn) Push(ctx context.Context, req sdk.DeployRequest) (string, error) {
	zone := req.Config["zone_id"]
	if zone == "" {
		return "", sdk.InvalidConfig("zone_id", "zone_id is required")
	}
	if req.DryRun {
		// Read-only checks against the CDN here.
		return "zone " + zone + " is reachable", nil
	}
	// Upload sdk.FullChainPEM(req.Certificate) and req.Certificate.PrivateKeyPEM here.
	return "certificate bound to zone " + zone, nil
}
```

### Blocklists

The host fetches every source a person configured from the manifest's
`blocklist` block on its refresh interval and writes the entries as nginx
`deny` rules to a file the person includes where the list should apply.
Return the complete list every time, as addresses or CIDR networks
(`sdk.Deny` builds an entry); the host validates each one and drops the
rest. An empty list denies nothing, so when the source cannot be read return
an error and the host keeps the list it has. `TTLSeconds` asks for an
earlier refresh. The manifest must request the `network` permission.

```go
type feed struct{}

func (feed) Fetch(ctx context.Context, req sdk.BlocklistRequest) (sdk.BlocklistResult, error) {
	key := req.Config["api_key"]
	if key == "" {
		return sdk.BlocklistResult{}, sdk.InvalidConfig("api_key", "api_key is required")
	}
	// Download the list with key here.
	return sdk.BlocklistResult{
		Entries:    []sdk.BlocklistEntry{sdk.Deny("203.0.113.0/24", "botnet")},
		TTLSeconds: 900,
	}, nil
}
```

### Service discovery

The host resolves every upstream a person bound to a service of one of the
manifest's `discovery` providers on its refresh interval and writes the
servers as an nginx `upstream` block. Return every server of `req.Service`
(`sdk.Target` builds one with weight 1): an IP address or a host name, a port
and a weight. Return `sdk.UnknownService` for a service the provider does
not know and any other error when the provider cannot be reached; the host
then keeps the servers it has. The manifest must request the `network`
permission.

```go
type registry struct{}

func (registry) Resolve(ctx context.Context, req sdk.DiscoveryRequest) (sdk.DiscoveryResult, error) {
	if req.Config["address"] == "" {
		return sdk.DiscoveryResult{}, sdk.InvalidConfig("address", "address is required")
	}
	// Look req.Service up in the registry here.
	return sdk.DiscoveryResult{Targets: []sdk.DiscoveryTarget{sdk.Target("10.0.1.12", 8080)}}, nil
}
```

### Content plugins

Config templates and translation files need no process and no SDK: declare
them in the manifest's `content` block and ship the files in the package.
See `spec/17-content-plugins.md` of the specification.

## Transports

stdio is always served. On top of it the SDK serves the same handlers over
gRPC by default and advertises it in the `plugin.initialize` reply
(`transports: ["stdio", "grpc"]`), so the host can send capability calls
(`dns01.*`, `http.handle`, `notify.*`, `probe.check`, `mcp.call`,
`storage.*`, `deploy.*`, `blocklist.fetch`, `discovery.resolve`) there. Lifecycle methods, `host.*` calls, host log
lines and notifications stay on stdio. Nothing changes for your handlers: a
gRPC call is decoded into the same JSON params and runs the same handler, and
a returned `*protocol.Error` reaches the host with the same code, message and
data on either transport.

* On Linux and macOS the server listens on the Unix socket
  `$NGINX_UI_PLUGIN_DATA_DIR/rpc.sock`. When that path is longer than the
  platform allows (103 bytes on macOS and the BSDs, 107 on Linux) or the data
  directory is unusable, the SDK uses a private directory under the system
  temp dir instead. The path is always reported in `rpc_socket`, and the
  socket is removed when the plugin exits.
* On Windows it listens on a loopback TCP port, reported in `rpc_port`
  together with a random `rpc_token`. Calls without the header
  `authorization: Bearer <rpc_token>` are rejected.

To stay on stdio only, pass `sdk.WithoutGRPC()` to `Serve` or `Run`, or set
`NGINX_UI_PLUGIN_DISABLE_GRPC=1` in the plugin environment:

```go
sdk.Serve(sdk.Plugin{DNS01: provider{}}, sdk.WithoutGRPC())
```

When the listener cannot be opened the SDK logs a warning and advertises
stdio only; the host never depends on gRPC being present.

## Packages

| Package | Contents |
| --- | --- |
| `sdk` (root) | `Plugin`, `Serve`, `Run`, the capability handler interfaces and helpers, the `Host` client, errors and the logger |
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
`Host`, `DNS01`, `HTTP`, `Notify`, `Probe`, `MCP`, `Storage`, `Deploy`,
`Blocklist`, `Discovery` and `Events` services. The plugin runtime in this
module keeps using the hand-written `protocol` types; `pb` is there for
reflection, for gRPC and for code that prefers generated types. The gRPC
transport resolves every call through the descriptors in `pb`, so a new rpc
in the contract is served as soon as `pb` is updated and a handler exists.
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
| `-32602` | `sdk.InvalidParams`, `sdk.UnknownTool` | Malformed params, an MCP tool the plugin does not serve, or a malformed storage key |
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

A cron entry, from the manifest or from `CronRegister`, names a method of the
plugin. When it fires, the host calls that method as an ordinary request with
params `{"type": "<cron id>", "ts": <unix seconds>}` and waits for the reply,
so register the handler under `Plugin.Methods`. Cron invocations do not go
through `events.on`.

## Environment

| Variable | Meaning |
| --- | --- |
| `NGINX_UI_PLUGIN_ID` | The plugin id from the manifest |
| `NGINX_UI_PLUGIN_API_VERSION` | The protocol version the host speaks |
| `NGINX_UI_PLUGIN_DATA_DIR` | The only directory the plugin may write to |
| `NGINX_UI_VERSION` | The host version |
| `NGINX_UI_PLUGIN_DISABLE_GRPC` | Set to `1` to serve stdio only, like `sdk.WithoutGRPC()` |

## License

AGPL-3.0. See [LICENSE](LICENSE).
