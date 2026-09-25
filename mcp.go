package sdk

import (
	"context"
	"encoding/json"

	"github.com/nginxui/plugin-sdk-go/protocol"
)

// MCPRequest is the payload of mcp.call.
type MCPRequest = protocol.MCPCallParams

// MCPResult is the reply to mcp.call.
type MCPResult = protocol.MCPCallResult

// MCPHandler runs the Model Context Protocol tools the manifest declares in
// its mcp block. The host publishes them under a prefixed name and forwards
// every call with the unprefixed name in req.Tool.
type MCPHandler interface {
	// Call runs one tool. A tool that ran and failed returns MCPError; an
	// unknown tool returns UnknownTool. req.Arguments come from an AI
	// assistant and must be validated before use.
	Call(ctx context.Context, req MCPRequest) (MCPResult, error)
}

// MCPToolFunc runs one tool with the arguments object of the call.
type MCPToolFunc func(ctx context.Context, args map[string]any) (MCPResult, error)

// MCPTools is an MCPHandler that dispatches a call by tool name.
//
//	sdk.Plugin{MCP: sdk.MCPTools{"purge_cache": purgeCache}}
type MCPTools map[string]MCPToolFunc

// Call implements MCPHandler.
func (t MCPTools) Call(ctx context.Context, req MCPRequest) (MCPResult, error) {
	tool, ok := t[req.Tool]
	if !ok || tool == nil {
		return MCPResult{}, UnknownTool(req.Tool)
	}
	args := req.Arguments
	if args == nil {
		args = map[string]any{}
	}
	return tool(ctx, args)
}

// MCPText is a successful tool result with one text block.
func MCPText(text string) MCPResult {
	return MCPResult{Content: []protocol.MCPContent{{Type: protocol.MCPContentTypeText, Text: text}}}
}

// MCPError is the result of a tool that ran and failed. The text tells the
// AI assistant what went wrong so it can correct itself.
func MCPError(text string) MCPResult {
	result := MCPText(text)
	result.IsError = true
	return result
}

// UnknownTool reports a tool the plugin does not serve, with the -32602 code
// the Model Context Protocol uses for it.
func UnknownTool(name string) *protocol.Error {
	return &protocol.Error{Code: protocol.CodeInvalidParams, Message: "unknown tool: " + name}
}

// registerMCP wires the mcp methods onto the connection.
func (rt *runtime) registerMCP() {
	rt.conn.Handle(protocol.MethodMCPCall, rt.track(rt.onMCPCall))
}

func (rt *runtime) onMCPCall(ctx context.Context, raw json.RawMessage) (any, error) {
	req, err := decode[MCPRequest](raw)
	if err != nil {
		return nil, err
	}
	result, err := rt.plugin.MCP.Call(WithHost(ctx, rt.host), req)
	if err != nil {
		return nil, err
	}
	// The contract requires a content list, even an empty one.
	if result.Content == nil {
		result.Content = []protocol.MCPContent{}
	}
	return result, nil
}
