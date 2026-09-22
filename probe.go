package sdk

import (
	"context"
	"encoding/json"
	"time"

	"github.com/0xJacky/nginx-ui-plugin-sdk-go/protocol"
)

// ProbeRequest is the payload of probe.check.
type ProbeRequest = protocol.ProbeCheckParams

// ProbeResult is the reply to probe.check.
type ProbeResult = protocol.ProbeCheckResult

// ProbeHandler checks the health of a target with the probe kinds the
// manifest declares in its probe block.
type ProbeHandler interface {
	// Check probes req.Target once with the kind req.Kind. An unhealthy or
	// unreachable target is a result with status down, not an error: return
	// an error only when the check could not run, InvalidConfig for a bad
	// field. The context expires with req.TimeoutSeconds.
	Check(ctx context.Context, req ProbeRequest) (ProbeResult, error)
}

// ProbeUp reports a healthy target.
func ProbeUp(latency time.Duration) ProbeResult {
	return ProbeResult{Status: protocol.ProbeStatusUp, LatencyMS: int(latency.Milliseconds())}
}

// ProbeDown reports a target that is unhealthy or did not answer.
func ProbeDown(latency time.Duration, message string) ProbeResult {
	return ProbeResult{Status: protocol.ProbeStatusDown, LatencyMS: int(latency.Milliseconds()), Message: message}
}

// ProbeDegraded reports a target that answers, but not as well as it should.
func ProbeDegraded(latency time.Duration, message string) ProbeResult {
	return ProbeResult{Status: protocol.ProbeStatusDegraded, LatencyMS: int(latency.Milliseconds()), Message: message}
}

// registerProbe wires the probe methods onto the connection.
func (rt *runtime) registerProbe() {
	rt.conn.Handle(protocol.MethodProbeCheck, rt.track(rt.onProbeCheck))
}

func (rt *runtime) onProbeCheck(ctx context.Context, raw json.RawMessage) (any, error) {
	req, err := decode[ProbeRequest](raw)
	if err != nil {
		return nil, err
	}
	if req.TimeoutSeconds > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.TimeoutSeconds)*time.Second)
		defer cancel()
	}
	return rt.plugin.Probe.Check(WithHost(ctx, rt.host), req)
}
