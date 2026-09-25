package sdk

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/nginxui/plugin-sdk-go/protocol"
)

// StoragePutRequest is the payload of storage.put.
type StoragePutRequest = protocol.StoragePutParams

// StorageGetRequest is the payload of storage.get.
type StorageGetRequest = protocol.StorageGetParams

// StorageListRequest is the payload of storage.list.
type StorageListRequest = protocol.StorageListParams

// StorageDeleteRequest is the payload of storage.delete.
type StorageDeleteRequest = protocol.StorageDeleteParams

// StorageObject is one entry of the storage.list reply.
type StorageObject = protocol.StorageObject

// StorageHandler keeps host files in the backends the manifest declares in
// its storage block. File contents never travel in a message: the host puts
// the file to store at req.SourcePath and expects a fetched object at
// req.TargetPath, both inside the exchange directory of the data directory.
// req.Backend is the backend code and req.Config holds the values of its
// form. Return InvalidConfig for a bad field and any other error for a
// vendor failure.
type StorageHandler interface {
	// Put stores the file at req.SourcePath under req.Key, replacing an
	// object stored there, and returns the number of bytes stored. It must
	// only read the source file.
	Put(ctx context.Context, req StoragePutRequest) (size int64, err error)
	// Get writes the object stored under req.Key to req.TargetPath, which
	// does not exist yet, and returns the number of bytes written.
	Get(ctx context.Context, req StorageGetRequest) (size int64, err error)
	// List returns every object whose key starts with req.Prefix, a plain
	// string prefix. StoredObject builds an entry.
	List(ctx context.Context, req StorageListRequest) ([]StorageObject, error)
	// Delete removes the object stored under req.Key. A missing object is
	// not an error.
	Delete(ctx context.Context, req StorageDeleteRequest) error
}

// StorageValidator is implemented by handlers that can check a backend
// configuration without storing anything. Handlers that do not implement it
// make the SDK reply Unsupported to storage.validate.
type StorageValidator interface {
	Validate(ctx context.Context, backend string, config map[string]string) error
}

// StoredObject builds a storage.list entry. A zero modified time is left out.
func StoredObject(key string, size int64, modified time.Time) StorageObject {
	object := StorageObject{Key: key, Size: protocol.ByteSize(size)}
	if !modified.IsZero() {
		object.ModifiedAt = modified.UTC().Format(time.RFC3339)
	}
	return object
}

// maxStorageKeyLength bounds a key, in bytes.
const maxStorageKeyLength = 1024

// ValidStorageKey reports whether key has the form the host sends: a
// relative path of "/" separated segments, none of them empty, "." or "..",
// without a backslash or a control character, at most 1024 bytes long.
func ValidStorageKey(key string) bool {
	if key == "" || len(key) > maxStorageKeyLength {
		return false
	}
	for _, r := range key {
		if r < 0x20 || r == 0x7f || r == '\\' {
			return false
		}
	}
	for _, segment := range strings.Split(key, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return false
		}
	}
	return true
}

// checkStorageKey rejects a malformed key before it reaches the handler, so
// a handler can build vendor paths from it safely.
func checkStorageKey(key string) error {
	if !ValidStorageKey(key) {
		return InvalidParams("malformed storage key: " + key)
	}
	return nil
}

// registerStorage wires the storage methods onto the connection.
func (rt *runtime) registerStorage() {
	rt.conn.Handle(protocol.MethodStoragePut, rt.track(rt.onStoragePut))
	rt.conn.Handle(protocol.MethodStorageGet, rt.track(rt.onStorageGet))
	rt.conn.Handle(protocol.MethodStorageList, rt.track(rt.onStorageList))
	rt.conn.Handle(protocol.MethodStorageDelete, rt.track(rt.onStorageDelete))

	if v, ok := rt.plugin.Storage.(StorageValidator); ok {
		rt.conn.Handle(protocol.MethodStorageValidate, rt.track(rt.storageValidate(v)))
	} else {
		rt.conn.Handle(protocol.MethodStorageValidate, unsupported(protocol.MethodStorageValidate))
	}
}

func (rt *runtime) onStoragePut(ctx context.Context, raw json.RawMessage) (any, error) {
	req, err := decode[StoragePutRequest](raw)
	if err != nil {
		return nil, err
	}
	if err = checkStorageKey(req.Key); err != nil {
		return nil, err
	}
	size, err := rt.plugin.Storage.Put(WithHost(ctx, rt.host), req)
	if err != nil {
		return nil, err
	}
	return protocol.StorageSizeResult{Size: protocol.ByteSize(size)}, nil
}

func (rt *runtime) onStorageGet(ctx context.Context, raw json.RawMessage) (any, error) {
	req, err := decode[StorageGetRequest](raw)
	if err != nil {
		return nil, err
	}
	if err = checkStorageKey(req.Key); err != nil {
		return nil, err
	}
	size, err := rt.plugin.Storage.Get(WithHost(ctx, rt.host), req)
	if err != nil {
		return nil, err
	}
	return protocol.StorageSizeResult{Size: protocol.ByteSize(size)}, nil
}

func (rt *runtime) onStorageList(ctx context.Context, raw json.RawMessage) (any, error) {
	req, err := decode[StorageListRequest](raw)
	if err != nil {
		return nil, err
	}
	objects, err := rt.plugin.Storage.List(WithHost(ctx, rt.host), req)
	if err != nil {
		return nil, err
	}
	if objects == nil {
		objects = []StorageObject{}
	}
	return protocol.StorageListResult{Objects: objects}, nil
}

func (rt *runtime) onStorageDelete(ctx context.Context, raw json.RawMessage) (any, error) {
	req, err := decode[StorageDeleteRequest](raw)
	if err != nil {
		return nil, err
	}
	if err = checkStorageKey(req.Key); err != nil {
		return nil, err
	}
	if err = rt.plugin.Storage.Delete(WithHost(ctx, rt.host), req); err != nil {
		return nil, err
	}
	return protocol.EmptyResult{}, nil
}

func (rt *runtime) storageValidate(v StorageValidator) Handler {
	return func(ctx context.Context, raw json.RawMessage) (any, error) {
		params, err := decode[protocol.StorageValidateParams](raw)
		if err != nil {
			return nil, err
		}
		if err := v.Validate(WithHost(ctx, rt.host), params.Backend, params.Config); err != nil {
			return nil, err
		}
		return protocol.EmptyResult{}, nil
	}
}
