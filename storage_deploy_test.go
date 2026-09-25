package sdk_test

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	sdk "github.com/nginxui/plugin-sdk-go"
	"github.com/nginxui/plugin-sdk-go/protocol"
)

// memoryStorage is a StorageHandler that keeps objects in memory and reads
// and writes the exchange files it is handed.
type memoryStorage struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newMemoryStorage() *memoryStorage {
	return &memoryStorage{objects: map[string][]byte{}}
}

func (m *memoryStorage) Put(_ context.Context, req sdk.StoragePutRequest) (int64, error) {
	if req.Config["url"] == "" {
		return 0, sdk.InvalidConfig("url", "url is required")
	}
	data, err := os.ReadFile(req.SourcePath)
	if err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[req.Key] = data
	return int64(len(data)), nil
}

func (m *memoryStorage) Get(_ context.Context, req sdk.StorageGetRequest) (int64, error) {
	m.mu.Lock()
	data, ok := m.objects[req.Key]
	m.mu.Unlock()
	if !ok {
		return 0, sdk.Internal("no object is stored under " + req.Key)
	}
	if err := os.WriteFile(req.TargetPath, data, 0o600); err != nil {
		return 0, err
	}
	return int64(len(data)), nil
}

func (m *memoryStorage) List(_ context.Context, req sdk.StorageListRequest) ([]sdk.StorageObject, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var objects []sdk.StorageObject
	for key, data := range m.objects {
		if strings.HasPrefix(key, req.Prefix) {
			objects = append(objects, sdk.StoredObject(key, int64(len(data)), time.Date(2026, 9, 21, 3, 0, 5, 0, time.UTC)))
		}
	}
	return objects, nil
}

func (m *memoryStorage) Delete(_ context.Context, req sdk.StorageDeleteRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, req.Key)
	return nil
}

// validatingStorage also implements StorageValidator.
type validatingStorage struct{ *memoryStorage }

func (validatingStorage) Validate(_ context.Context, backend string, config map[string]string) error {
	if backend != "webdav" {
		return sdk.InvalidConfig("backend", "unknown backend")
	}
	if !strings.HasPrefix(config["url"], "https://") {
		return sdk.InvalidConfig("url", "url must be an https URL")
	}
	return nil
}

func TestStorageMovesFilesThroughTheExchangeDirectory(t *testing.T) {
	store := newMemoryStorage()
	h := newHarness(t, sdk.Plugin{Storage: store})

	res := h.initialize(t)
	if !slices.Equal(res.Capabilities, []string{protocol.CapabilityStorage}) {
		t.Fatalf("capabilities = %v, want [storage]", res.Capabilities)
	}

	exchange := t.TempDir()
	source := filepath.Join(exchange, "daily_1.zip")
	if err := os.WriteFile(source, []byte("archive"), 0o600); err != nil {
		t.Fatal(err)
	}
	config := map[string]string{"url": "https://dav.example"}

	var put protocol.StorageSizeResult
	if err := h.host.Call(t.Context(), protocol.MethodStoragePut, protocol.StoragePutParams{
		Backend: "webdav", Config: config, Key: "nginx-ui/daily_1.zip", SourcePath: source,
	}, &put); err != nil {
		t.Fatalf("storage.put: %v", err)
	}
	if put.Size != 7 {
		t.Fatalf("size = %d, want 7", put.Size)
	}

	var list protocol.StorageListResult
	if err := h.host.Call(t.Context(), protocol.MethodStorageList, protocol.StorageListParams{
		Backend: "webdav", Config: config, Prefix: "nginx-ui/daily_",
	}, &list); err != nil {
		t.Fatalf("storage.list: %v", err)
	}
	if len(list.Objects) != 1 || list.Objects[0].Key != "nginx-ui/daily_1.zip" || list.Objects[0].Size != 7 ||
		list.Objects[0].ModifiedAt != "2026-09-21T03:00:05Z" {
		t.Fatalf("objects = %+v", list.Objects)
	}

	target := filepath.Join(exchange, "fetched.zip")
	var got protocol.StorageSizeResult
	if err := h.host.Call(t.Context(), protocol.MethodStorageGet, protocol.StorageGetParams{
		Backend: "webdav", Config: config, Key: "nginx-ui/daily_1.zip", TargetPath: target,
	}, &got); err != nil {
		t.Fatalf("storage.get: %v", err)
	}
	if data, _ := os.ReadFile(target); string(data) != "archive" || got.Size != 7 {
		t.Fatalf("fetched %q (%d bytes)", data, got.Size)
	}

	for range 2 {
		// Deleting twice succeeds, a missing object is not an error.
		if err := h.host.Call(t.Context(), protocol.MethodStorageDelete, protocol.StorageDeleteParams{
			Backend: "webdav", Config: config, Key: "nginx-ui/daily_1.zip",
		}, nil); err != nil {
			t.Fatalf("storage.delete: %v", err)
		}
	}

	var raw map[string]any
	if err := h.host.Call(t.Context(), protocol.MethodStorageList, protocol.StorageListParams{
		Backend: "webdav", Config: config,
	}, &raw); err != nil {
		t.Fatalf("storage.list: %v", err)
	}
	if objects, ok := raw["objects"].([]any); !ok || len(objects) != 0 {
		t.Fatalf("result = %v, want an empty objects list", raw)
	}

	err := h.host.Call(t.Context(), protocol.MethodStoragePut, protocol.StoragePutParams{
		Backend: "webdav", Key: "nginx-ui/daily_2.zip", SourcePath: source,
	}, nil)
	if field := invalidConfigField(t, err); field != "url" {
		t.Fatalf("field = %q, want url", field)
	}
}

func TestStorageRejectsMalformedKeys(t *testing.T) {
	h := newHarness(t, sdk.Plugin{Storage: newMemoryStorage()})
	h.initialize(t)

	for _, key := range []string{"", "/abs", "a//b", "a/../b", "./a", "a/", `a\b`, "a\x00b"} {
		err := h.host.Call(t.Context(), protocol.MethodStorageDelete, protocol.StorageDeleteParams{
			Backend: "webdav", Key: key,
		}, nil)
		if code := errorCode(t, err); code != protocol.CodeInvalidParams {
			t.Fatalf("key %q: code = %d, want %d", key, code, protocol.CodeInvalidParams)
		}
	}
	if !sdk.ValidStorageKey("nginx-ui/daily_1790000000.zip.key") {
		t.Fatal("a host key is rejected")
	}
}

func TestStorageValidate(t *testing.T) {
	h := newHarness(t, sdk.Plugin{Storage: newMemoryStorage()})
	h.initialize(t)
	err := h.host.Call(t.Context(), protocol.MethodStorageValidate, protocol.StorageValidateParams{Backend: "webdav"}, nil)
	if code := errorCode(t, err); code != protocol.CodeUnsupported {
		t.Fatalf("code = %d, want %d", code, protocol.CodeUnsupported)
	}

	h = newHarness(t, sdk.Plugin{Storage: validatingStorage{newMemoryStorage()}})
	h.initialize(t)
	err = h.host.Call(t.Context(), protocol.MethodStorageValidate, protocol.StorageValidateParams{
		Backend: "webdav", Config: map[string]string{"url": "ftp://dav.example"},
	}, nil)
	if field := invalidConfigField(t, err); field != "url" {
		t.Fatalf("field = %q, want url", field)
	}
	if err = h.host.Call(t.Context(), protocol.MethodStorageValidate, protocol.StorageValidateParams{
		Backend: "webdav", Config: map[string]string{"url": "https://dav.example"},
	}, nil); err != nil {
		t.Fatalf("storage.validate: %v", err)
	}
}

// deployer is a DeployHandler that remembers what it was asked to push.
type deployer struct {
	pushed chan sdk.DeployRequest
}

func (d *deployer) Push(_ context.Context, req sdk.DeployRequest) (string, error) {
	if req.Config["zone_id"] == "" {
		return "", sdk.InvalidConfig("zone_id", "zone_id is required")
	}
	d.pushed <- req
	if req.DryRun {
		return "zone " + req.Config["zone_id"] + " is reachable", nil
	}
	return "uploaded to zone " + req.Config["zone_id"], nil
}

// validatingDeployer also implements DeployValidator.
type validatingDeployer struct{ *deployer }

func (validatingDeployer) Validate(_ context.Context, _ string, config map[string]string) error {
	if config["zone_id"] == "" {
		return sdk.InvalidConfig("zone_id", "zone_id is required")
	}
	return nil
}

func TestDeployPushReachesTheHandler(t *testing.T) {
	d := &deployer{pushed: make(chan sdk.DeployRequest, 2)}
	h := newHarness(t, sdk.Plugin{Deploy: d})

	res := h.initialize(t)
	if !slices.Equal(res.Capabilities, []string{protocol.CapabilityCertDeploy}) {
		t.Fatalf("capabilities = %v, want [cert.deploy]", res.Capabilities)
	}

	params := protocol.DeployPushParams{
		Kind:   "mycdn",
		Config: map[string]string{"zone_id": "zone_123"},
		Certificate: protocol.DeployCertificate{
			Name:           "example.com",
			Domains:        []string{"example.com"},
			CertificatePEM: "LEAF",
			PrivateKeyPEM:  "KEY",
			ChainPEM:       "ISSUER\n",
			NotAfter:       "2026-12-20T08:15:00Z",
		},
	}
	var result protocol.DeployPushResult
	if err := h.host.Call(t.Context(), protocol.MethodDeployPush, params, &result); err != nil {
		t.Fatalf("deploy.push: %v", err)
	}
	if result.Message != "uploaded to zone zone_123" {
		t.Fatalf("message = %q", result.Message)
	}
	got := <-d.pushed
	if got.Certificate.PrivateKeyPEM != "KEY" || got.DryRun || sdk.FullChainPEM(got.Certificate) != "LEAF\nISSUER\n" {
		t.Fatalf("handler got %+v", got)
	}

	params.DryRun = true
	if err := h.host.Call(t.Context(), protocol.MethodDeployPush, params, &result); err != nil {
		t.Fatalf("deploy.push dry run: %v", err)
	}
	if got = <-d.pushed; !got.DryRun || result.Message != "zone zone_123 is reachable" {
		t.Fatalf("dry run got %+v, message %q", got, result.Message)
	}

	params.Config = map[string]string{}
	err := h.host.Call(t.Context(), protocol.MethodDeployPush, params, nil)
	if field := invalidConfigField(t, err); field != "zone_id" {
		t.Fatalf("field = %q, want zone_id", field)
	}

	err = h.host.Call(t.Context(), protocol.MethodDeployValidate, protocol.DeployValidateParams{Kind: "mycdn"}, nil)
	if code := errorCode(t, err); code != protocol.CodeUnsupported {
		t.Fatalf("code = %d, want %d", code, protocol.CodeUnsupported)
	}
}

func TestDeployValidateUsesTheValidator(t *testing.T) {
	h := newHarness(t, sdk.Plugin{Deploy: validatingDeployer{&deployer{pushed: make(chan sdk.DeployRequest, 1)}}})
	h.initialize(t)

	err := h.host.Call(t.Context(), protocol.MethodDeployValidate, protocol.DeployValidateParams{Kind: "mycdn"}, nil)
	if field := invalidConfigField(t, err); field != "zone_id" {
		t.Fatalf("field = %q, want zone_id", field)
	}
	if err = h.host.Call(t.Context(), protocol.MethodDeployValidate, protocol.DeployValidateParams{
		Kind: "mycdn", Config: map[string]string{"zone_id": "zone_123"},
	}, nil); err != nil {
		t.Fatalf("deploy.validate: %v", err)
	}
}

func TestFullChainPEM(t *testing.T) {
	if got := sdk.FullChainPEM(sdk.DeployCertificate{CertificatePEM: "LEAF"}); got != "LEAF" {
		t.Fatalf("leaf only = %q", got)
	}
	if got := sdk.FullChainPEM(sdk.DeployCertificate{CertificatePEM: "LEAF\n", ChainPEM: "ISSUER\n"}); got != "LEAF\nISSUER\n" {
		t.Fatalf("with chain = %q", got)
	}
}
