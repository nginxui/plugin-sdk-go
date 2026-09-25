package sdk

import (
	"context"
	"encoding/json"

	"github.com/nginxui/plugin-sdk-go/protocol"
)

// NotifyRequest is the payload of notify.send.
type NotifyRequest = protocol.NotifySendParams

// NotifyHandler delivers notifications through the channels the manifest
// declares in its notify block.
type NotifyHandler interface {
	// Send delivers one notification. req.Channel is the channel code and
	// req.Config holds the values of its form. Return InvalidConfig for a bad
	// field and any other error for a vendor failure.
	Send(ctx context.Context, req NotifyRequest) error
}

// NotifyValidator is implemented by handlers that can check a channel
// configuration without sending anything. Handlers that do not implement it
// make the SDK reply Unsupported to notify.validate.
type NotifyValidator interface {
	Validate(ctx context.Context, channel string, config map[string]string) error
}

// registerNotify wires the notify methods onto the connection.
func (rt *runtime) registerNotify() {
	rt.conn.Handle(protocol.MethodNotifySend, rt.track(rt.onNotifySend))

	if v, ok := rt.plugin.Notify.(NotifyValidator); ok {
		rt.conn.Handle(protocol.MethodNotifyValidate, rt.track(rt.notifyValidate(v)))
	} else {
		rt.conn.Handle(protocol.MethodNotifyValidate, unsupported(protocol.MethodNotifyValidate))
	}
}

func (rt *runtime) onNotifySend(ctx context.Context, raw json.RawMessage) (any, error) {
	req, err := decode[NotifyRequest](raw)
	if err != nil {
		return nil, err
	}
	if err := rt.plugin.Notify.Send(WithHost(ctx, rt.host), req); err != nil {
		return nil, err
	}
	return protocol.EmptyResult{}, nil
}

func (rt *runtime) notifyValidate(v NotifyValidator) Handler {
	return func(ctx context.Context, raw json.RawMessage) (any, error) {
		params, err := decode[protocol.NotifyValidateParams](raw)
		if err != nil {
			return nil, err
		}
		if err := v.Validate(WithHost(ctx, rt.host), params.Channel, params.Config); err != nil {
			return nil, err
		}
		return protocol.EmptyResult{}, nil
	}
}
