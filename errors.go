package sdk

import (
	"strings"

	"github.com/0xJacky/nginx-ui-plugin-sdk-go/protocol"
)

// NewError builds a JSON-RPC error with an arbitrary code and payload.
func NewError(code int, msg string, data any) *protocol.Error {
	return &protocol.Error{Code: code, Message: msg, Data: data}
}

// InvalidConfig reports a bad credential or setting. field names the offending
// input so the host can highlight it in the form.
func InvalidConfig(field, msg string) *protocol.Error {
	return &protocol.Error{
		Code:    protocol.CodeInvalidConfig,
		Message: msg,
		Data:    protocol.InvalidConfigData{Field: field},
	}
}

// Unsupported reports that the plugin does not implement a capability method,
// which lets the host fall back to its own implementation.
func Unsupported(method string) *protocol.Error {
	return &protocol.Error{
		Code:    protocol.CodeUnsupported,
		Message: "unsupported method: " + method,
	}
}

// InvalidParams reports params that could not be decoded.
func InvalidParams(msg string) *protocol.Error {
	return &protocol.Error{Code: protocol.CodeInvalidParams, Message: msg}
}

// Internal reports an unexpected failure.
func Internal(msg string) *protocol.Error {
	return &protocol.Error{Code: protocol.CodeInternalError, Message: msg}
}

// Redact masks everything but the last four characters of s. Use it before
// putting any credential-derived string in a log line.
func Redact(s string) string {
	runes := []rune(s)
	if len(runes) <= 4 {
		return strings.Repeat("*", len(runes))
	}
	return strings.Repeat("*", len(runes)-4) + string(runes[len(runes)-4:])
}
