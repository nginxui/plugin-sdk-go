package sdk

import (
	"context"
	"encoding/json"

	"github.com/nginxui/plugin-sdk-go/protocol"
)

// EventHandler handles one event delivered through events.on.
type EventHandler func(ctx context.Context, ev protocol.EventNotification)

// EventsHandler returns the events.on handler that dispatches a delivered
// event to the handler registered for its type. An event without a handler
// is ignored, as the spec requires (HOST-15). Every call runs on its own
// goroutine. Set Plugin.Events instead of registering it by hand.
func EventsHandler(handlers map[string]EventHandler) Handler {
	return func(ctx context.Context, raw json.RawMessage) (any, error) {
		var ev protocol.EventNotification
		if err := json.Unmarshal(raw, &ev); err != nil {
			return nil, InvalidParams("events.on: " + err.Error())
		}
		if h := handlers[ev.Type]; h != nil {
			h(ctx, ev)
		}
		return nil, nil
	}
}
