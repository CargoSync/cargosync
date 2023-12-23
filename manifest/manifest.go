package manifest

// Code modified from https://github.com/godarch/darch/blob/develop/pkg/repository/manifest/manifest.go
// Many thanks to the darch team for their work on this!

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/images"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

const containerdUncompressed = "containerd.io/uncompressed"

// Manifest The manifest that can be mutated.
type Manifest interface {
	ReplaceWithLayer(ctx context.Context, contentStore content.Store, layer ocispec.Descriptor, imageConfig []byte) error
	Descriptor() ocispec.Descriptor
}

type ManifestImpl struct {
	D    map[string]json.RawMessage
	Desc ocispec.Descriptor
}

func (m *ManifestImpl) getManifestImpl() *ManifestImpl {
	return m
}

// LoadManifest Load a manifest in-memory for easy interaction.
func LoadManifest(ctx context.Context, contentStore content.Store, desc ocispec.Descriptor) (Manifest, error) {
	p, err := content.ReadBlob(ctx, contentStore, desc)
	if err != nil {
		return nil, err
	}

	m := map[string]json.RawMessage{}
	if err := json.Unmarshal(p, &m); err != nil {
		return nil, err
	}

	return &ManifestImpl{
		D:    m,
		Desc: desc,
	}, nil
}

// For accessing the ManifestImpl fields from outside the package
func LoadManifestImpl(ctx context.Context, contentStore content.Store, desc ocispec.Descriptor) (*ManifestImpl, error) {
	p, err := content.ReadBlob(ctx, contentStore, desc)
	if err != nil {
		return nil, err
	}

	m := map[string]json.RawMessage{}
	if err := json.Unmarshal(p, &m); err != nil {
		return nil, err
	}

	return &ManifestImpl{
		D:    m,
		Desc: desc,
	}, nil
}

func LoadManifestFromList(ctx context.Context, desc ocispec.Descriptor, contentStore content.Store, os string, arch string) (Manifest, error) {

	p, err := content.ReadBlob(ctx, contentStore, desc)
	if err != nil {
		return nil, err
	}

	m := map[string]json.RawMessage{}
	if err := json.Unmarshal(p, &m); err != nil {
		return nil, err
	}

	manifestListJSON, err := m["manifests"].MarshalJSON()
	if err != nil {
		return nil, err
	}
	var manifestList []json.RawMessage

	if err = json.Unmarshal(manifestListJSON, &manifestList); err != nil {
		return nil, err
	}

	// iterate over the manifests and choose the one with architecture the same as the one wanted
	manifest_json := map[string]json.RawMessage{}
	for _, manifest := range manifestList {
		if err := json.Unmarshal(manifest, &manifest_json); err != nil {
			fmt.Println("error unmarshalling manifest")
			return nil, err
		}

		platform := map[string]json.RawMessage{}
		if err := json.Unmarshal(manifest_json["platform"], &platform); err != nil {
			fmt.Println("error unmarshalling platform")
			return nil, err
		}

		// check if the platform is the same as the current machine's platform
		// if it is, then we have found the manifest we want to use, so we retrieve
		// the descriptor and load the manifest!

		if string(platform["os"]) == `"`+os+`"` && strings.EqualFold(string(platform["architecture"]), `"`+arch+`"`) {
			desc, err := GetDescriptor(manifest)
			if err != nil {
				return nil, err
			}
			return LoadManifest(ctx, contentStore, desc)
		}
	}
	return nil, fmt.Errorf("no manifest found for %s/%s", runtime.GOOS, runtime.GOARCH)
}

func LoadManifestImplFromList(ctx context.Context, desc ocispec.Descriptor, contentStore content.Store, os string, arch string) (*ManifestImpl, error) {
	// Same as LoadManifestFromList, but returns a ManifestImpl instead of a Manifest

	p, err := content.ReadBlob(ctx, contentStore, desc)
	if err != nil {
		return nil, err
	}

	m := map[string]json.RawMessage{}
	if err := json.Unmarshal(p, &m); err != nil {
		return nil, err
	}

	manifestListJSON, err := m["manifests"].MarshalJSON()
	if err != nil {
		return nil, err
	}
	var manifestList []json.RawMessage

	if err = json.Unmarshal(manifestListJSON, &manifestList); err != nil {
		return nil, err
	}

	// iterate over the manifests and choose the one with architecture the same as the one wanted
	manifest_json := map[string]json.RawMessage{}
	for _, manifest := range manifestList {
		if err := json.Unmarshal(manifest, &manifest_json); err != nil {
			fmt.Println("error unmarshalling manifest")
			return nil, err
		}

		platform := map[string]json.RawMessage{}
		if err := json.Unmarshal(manifest_json["platform"], &platform); err != nil {
			fmt.Println("error unmarshalling platform")
			return nil, err
		}

		// check if the platform is the same as the current machine's platform
		// if it is, then we have found the manifest we want to use, so we retrieve
		// the descriptor and load the manifest!

		if string(platform["os"]) == `"`+os+`"` && strings.EqualFold(string(platform["architecture"]), `"`+arch+`"`) {
			desc, err := GetDescriptor(manifest)
			if err != nil {
				return nil, err
			}
			return LoadManifestImpl(ctx, contentStore, desc)
		}
	}
	return nil, fmt.Errorf("no manifest found for %s/%s", runtime.GOOS, runtime.GOARCH)
}

func (m *ManifestImpl) ReplaceWithLayer(ctx context.Context, contentStore content.Store, layer ocispec.Descriptor, imageConfig []byte) error {
	d := m.D

	// These builds can be done on docker images, or OCI image.
	// Let's make sure the new layer uses the same content type as the manifest expects.
	switch m.Desc.MediaType {
	case images.MediaTypeDockerSchema2Manifest:
		layer.MediaType = images.MediaTypeDockerSchema2LayerGzip
	case ocispec.MediaTypeImageManifest:
		layer.MediaType = ocispec.MediaTypeImageLayerGzip
	default:
		return fmt.Errorf("unknown parent image manifest type: %s", m.Desc.MediaType)
	}

	// Get the diffId for the diff descriptor.
	info, err := contentStore.Info(ctx, layer.Digest)
	if err != nil {
		return err
	}
	diffIDStr, ok := info.Labels[containerdUncompressed]
	if !ok {
		return fmt.Errorf("invalid differ response with no diffID")
	}
	diffIDDigest, err := digest.Parse(diffIDStr)
	if err != nil {
		return err
	}

	// Deserialize the image config
	imageConfigDesc, err := GetDescriptor(d["config"])
	if err != nil {
		return err
	}

	// Patch the config and store it in the content store.
	imageConfigDesc, err = patchImageConfig(ctx, contentStore, imageConfigDesc, diffIDDigest, imageConfig)
	if err != nil {
		return err
	}

	// Store the image config back into our json object.
	imageConfigJSON, err := json.Marshal(imageConfigDesc)
	if err != nil {
		return err
	}
	d["config"] = imageConfigJSON

	// Update the layers on the manifest.
	layers := []ocispec.Descriptor{}
	layersJSON, err := d["layers"].MarshalJSON()
	if err != nil {
		return err
	}
	if err = json.Unmarshal(layersJSON, &layers); err != nil {
		return err
	}
	layers = nil // IMPORTANT: clear the layers
	layers = append(layers, layer)
	layersJSON, err = json.Marshal(layers)
	if err != nil {
		return err
	}
	d["layers"] = layersJSON

	// Now that we have all the data ready, let's try to store it.
	// Prepare the labels that will tell the garbage collector
	// to NOT delete the content this manifest references.
	labels := map[string]string{
		"containerd.io/gc.ref.content.0": imageConfigDesc.Digest.String(),
	}
	for i, layer := range layers {
		labels[fmt.Sprintf("containerd.io/gc.ref.content.%d", i+1)] = layer.Digest.String()
	}

	// Save our new image manifest, which now hows our new layer,
	// and a patched image config with a reference to the new layer.
	newDesc := m.Desc
	manifestBytes, err := json.Marshal(d)
	if err != nil {
		return err
	}
	newDesc.Digest = digest.FromBytes(manifestBytes)
	newDesc.Size = int64(len(manifestBytes))
	if err := content.WriteBlob(ctx,
		contentStore,
		"custom-ref",
		bytes.NewReader(manifestBytes),
		newDesc,
		content.WithLabels(labels)); err != nil {
		return err
	}

	m.Desc = newDesc
	m.D = d

	return nil
}

func (m *ManifestImpl) Descriptor() ocispec.Descriptor {
	return m.Desc
}

func patchImageConfig(ctx context.Context, contentStore content.Store, imageConfig ocispec.Descriptor, newLayer digest.Digest, imageConfigBytes []byte) (ocispec.Descriptor, error) {
	result := imageConfig

	var p []byte
	var err error
	if imageConfigBytes == nil {
		// Get the current image configuration.
		p, err = content.ReadBlob(ctx, contentStore, imageConfig)
		if err != nil {
			return result, err
		}
	} else {
		p = imageConfigBytes
	}

	// Deserialize the image configuration to a generic json object.
	// We do this so that we can patch it, without requiring knowledge
	// of the entire schema.
	m := map[string]json.RawMessage{}
	if err := json.Unmarshal(p, &m); err != nil {
		return result, err
	}

	// Pull the rootfs section out, so that we can append a layer to the diff_ids array.
	var rootFS ocispec.RootFS
	p, err = m["rootfs"].MarshalJSON()
	if err != nil {
		return result, err
	}
	if err = json.Unmarshal(p, &rootFS); err != nil {
		return result, err
	}
	rootFS.DiffIDs = nil // IMPORTANT: clear the diff_ids
	rootFS.DiffIDs = append(rootFS.DiffIDs, newLayer)
	p, err = json.Marshal(rootFS)
	if err != nil {
		return ocispec.Descriptor{}, err
	}
	m["rootfs"] = p

	// Convert our entire image configuration back to bytes, and write it to the content store.
	p, err = json.Marshal(m)
	if err != nil {
		return ocispec.Descriptor{}, err
	}

	result.Digest = digest.FromBytes(p)
	result.Size = int64(len(p))
	err = content.WriteBlob(ctx, contentStore,
		"custom-ref",
		bytes.NewReader(p),
		result,
	)
	if err != nil {
		return result, err
	}

	return result, nil
}

func GetDescriptor(m json.RawMessage) (ocispec.Descriptor, error) {
	var r ocispec.Descriptor
	p, err := m.MarshalJSON()
	if err != nil {
		return r, err
	}
	if err = json.Unmarshal(p, &r); err != nil {
		return r, err
	}
	return r, nil
}
