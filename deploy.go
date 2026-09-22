package sdk

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/0xJacky/nginx-ui-plugin-sdk-go/protocol"
)

// DeployRequest is the payload of deploy.push.
type DeployRequest = protocol.DeployPushParams

// DeployCertificate is the certificate a DeployRequest carries.
type DeployCertificate = protocol.DeployCertificate

// DeployHandler pushes certificates to the kinds of target the manifest
// declares in its deploy block. req.Kind is the target kind code and
// req.Config holds the values of its form. The manifest must request the
// cert.deploy permission, since the request carries the private key.
type DeployHandler interface {
	// Push pushes req.Certificate to the target and returns a short summary
	// of what it did. When req.DryRun is true it must not change anything:
	// it checks what a real push needs and says what it would do. Pushing a
	// certificate the target already serves must succeed. Return
	// InvalidConfig for a bad field and any other error for a target
	// failure; never put the private key in an error or a log line.
	Push(ctx context.Context, req DeployRequest) (message string, err error)
}

// DeployValidator is implemented by handlers that can check a target
// configuration without contacting the target. Handlers that do not
// implement it make the SDK reply Unsupported to deploy.validate.
type DeployValidator interface {
	Validate(ctx context.Context, kind string, config map[string]string) error
}

// FullChainPEM returns the leaf followed by its intermediates, for a target
// that wants the whole chain in one piece.
func FullChainPEM(c DeployCertificate) string {
	leaf := c.CertificatePEM
	if c.ChainPEM == "" {
		return leaf
	}
	if leaf != "" && !strings.HasSuffix(leaf, "\n") {
		leaf += "\n"
	}
	return leaf + c.ChainPEM
}

// registerDeploy wires the cert.deploy methods onto the connection.
func (rt *runtime) registerDeploy() {
	rt.conn.Handle(protocol.MethodDeployPush, rt.track(rt.onDeployPush))

	if v, ok := rt.plugin.Deploy.(DeployValidator); ok {
		rt.conn.Handle(protocol.MethodDeployValidate, rt.track(rt.deployValidate(v)))
	} else {
		rt.conn.Handle(protocol.MethodDeployValidate, unsupported(protocol.MethodDeployValidate))
	}
}

func (rt *runtime) onDeployPush(ctx context.Context, raw json.RawMessage) (any, error) {
	req, err := decode[DeployRequest](raw)
	if err != nil {
		return nil, err
	}
	message, err := rt.plugin.Deploy.Push(WithHost(ctx, rt.host), req)
	if err != nil {
		return nil, err
	}
	return protocol.DeployPushResult{Message: message}, nil
}

func (rt *runtime) deployValidate(v DeployValidator) Handler {
	return func(ctx context.Context, raw json.RawMessage) (any, error) {
		params, err := decode[protocol.DeployValidateParams](raw)
		if err != nil {
			return nil, err
		}
		if err := v.Validate(WithHost(ctx, rt.host), params.Kind, params.Config); err != nil {
			return nil, err
		}
		return protocol.EmptyResult{}, nil
	}
}
