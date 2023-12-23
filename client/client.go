package main

import (
	"context"
	"deltadiff/api"
	"deltadiff/manifest"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/diff"
	"github.com/containerd/containerd/images"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/snapshots"
	"github.com/opencontainers/image-spec/identity"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/mackerelio/go-osstat/cpu"
	"go.mongodb.org/mongo-driver/bson"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	image1ref, image2ref string
)

func main() {

	var SERVER_ADDRESS string

	if len(os.Args) != 3 {
		fmt.Println("Usage: client <target-image>")
		return
	} else {
		//image1ref = os.Args[1]
		image2ref = os.Args[1]
		SERVER_ADDRESS = os.Args[2]
	}

	before, _ := cpu.Get()
	timeStart := time.Now()

	// Create a gRPC connection to the server.
	conn, err := grpc.Dial(SERVER_ADDRESS, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer conn.Close()

	// Create a gRPC client for the service.
	diffClient := api.NewDeltaDiffServiceClient(conn)

	client, err := containerd.New("/run/containerd/containerd.sock", containerd.WithDefaultNamespace("default"))
	if err != nil {
		fmt.Printf("error creating client: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	// Create a context for the request.
	ctx := context.Background()
	ctx, done, err := client.WithLease(ctx)
	if err != nil {
		fmt.Println("error getting lease")
		fmt.Println(err)
	}
	defer done(ctx)

	snapshotter := client.SnapshotService(containerd.DefaultSnapshotter)

	// Locate if there are any existing older versions of image2
	image_list, err := client.ListImages(context.Background())
	if err != nil {
		fmt.Printf("error listing images: %v\n", err)
		return
	}
	for _, image := range image_list {
		if strings.Contains(image.Name(), strings.Split(image2ref, ":")[0]) && image.Name() != image2ref {
			fmt.Printf("Found existing image %s, will fetch diffs between existing and target image...\n", image.Name())
			image1ref = image.Name()
		}
	}

	image1name := strings.Split(image1ref, "/")[len(strings.Split(image1ref, "/"))-1]
	image2name := strings.Split(image2ref, "/")[len(strings.Split(image2ref, "/"))-1]

	// Make a request to the server.
	req := api.CalcImageDiffsRequest{
		Image1: &api.Image{Reference: image1ref}, // example: "docker.io/library/alpine:3.15.10"
		Image2: &api.Image{Reference: image2ref}, // example: "docker.io/library/alpine:latest"
	}

	timeRequestStart := time.Now()

	resp, err := diffClient.CalculateDeltaDiffs(ctx, &req)
	if err != nil {
		fmt.Printf("rpc request error: %v\n", err)
		return
	}

	filepath := fmt.Sprintf("/tmp/delta-diff-patch-from-%s-to-%s.zst", image1name, image2name)
	// Delete file if it already exists
	if _, err := os.Stat(filepath); err == nil {
		os.Remove(filepath)
	}
	// Create file to write stream to
	f, err := os.OpenFile(filepath, // example: "delta-diff-patch-from-alpine:3.15.10-to-alpine:latest
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("error creating delta file: %v\n", err)
		return
	}
	defer f.Close()

	// Receive stream (of delta diff file chunks) and write to file
	for {
		chunk, err := resp.Recv()
		if err != nil {
			break
		}
		if _, err := f.Write(chunk.DeltaDiff); err != nil {
			fmt.Printf("error writing to file: %v\n", err)
			return
		}
	}

	// Decompress the delta diff file
	cmd := exec.Command("zstd", "-fd", filepath)
	output, err := cmd.CombinedOutput()
	fmt.Println(string(output))
	if err != nil {
		fmt.Printf("WARNING (decompress command): %s", err)
		//return err
	}

	filepath = fmt.Sprintf("/tmp/delta-diff-patch-from-%s-to-%s", image1name, image2name)

	timeRequestEnd := time.Since(timeRequestStart)

	fmt.Printf("Successfully wrote delta diff file to %s\n", filepath)

	// Request manifest of image 2 from server
	// For multi-platform images, we pecify the OS and Arch
	// as the one used by the client
	req2 := api.ManifestRequest{
		Image: &api.Image{Reference: image2ref},
		Os:    runtime.GOOS,
		Arch:  runtime.GOARCH,
	}

	manifest2_bytes, err := diffClient.GetManifest(ctx, &req2)
	if err != nil {
		fmt.Printf("rpc request error: %v\n", err)
		return
	}

	var manifest2_impl manifest.ManifestImpl
	if err := bson.Unmarshal(manifest2_bytes.Manifest, &manifest2_impl); err != nil {
		fmt.Printf("error unmarshalling manifest: %v\n", err)
		return
	}

	// Get the image config (in bytes) from the response
	imageConfig := manifest2_bytes.ImageConfig

	var manifest2 manifest.Manifest
	manifest2 = &manifest.ManifestImpl{
		D:    manifest2_impl.D,
		Desc: manifest2_impl.Desc,
	}

	image1, err := client.GetImage(context.Background(), image1ref)
	if err != nil {
		fmt.Printf("error getting image %v. You should have the image pulled. errormsg: %v\n", image1ref, err)
		os.Exit(1)
	}
	// unpack the image if not unpacked
	isUnpacked, err := image1.IsUnpacked(context.Background(), "overlayfs")
	if err != nil {
		fmt.Printf("error checking if image is unpacked: %v\n", err)
		os.Exit(1)
	}
	if !isUnpacked {
		err = image1.Unpack(context.Background(), "overlayfs")
		if err != nil {
			fmt.Printf("error unpacking image: %v\n", err)
			return
		}
	}
	
	if err := snapshotter.Remove(ctx, "from"); err == nil {
		fmt.Println("cleaned up a snapshot from before:)")
	}
	mounts_from := PrepareSnapshot(ctx, snapshotter, image1, "from")
	defer snapshotter.Remove(ctx, "from")

	if err := mount.WithTempMount(ctx, mounts_from, func(from_root string) error {
		fmt.Println("from-dir: " + from_root)

		timeApplyDeltaStart := time.Now()

		cmd := exec.Command("rsync",
			"-avH",
			"--partial",
			"--delete",
			"--read-batch="+filepath,
			"--checksum",
			"--no-i-r",
			"--one-file-system",
			from_root+"/")

		output, err := cmd.CombinedOutput()
		fmt.Println(string(output))
		if err != nil {
			fmt.Printf("WARNING (apply batch command): %s", err)
			//return err
		}

		timeToApplyDelta := time.Since(timeApplyDeltaStart)

		// Retrieve the empty image
		image_empty, err := client.GetImage(ctx, "docker.io/jprotogtwi/blank-canvas:latest")
		if err != nil {
			fmt.Printf("Image %v not found, pulling...\n", "docker.io/jprotogtwi/blank-canvas:latest")
			_, err = client.Pull(ctx, "docker.io/jprotogtwi/blank-canvas:latest", containerd.WithPullUnpack)
			if err != nil {
				fmt.Println(err)
				return err
			}
			image_empty, err = client.GetImage(ctx, "docker.io/jprotogtwi/blank-canvas:latest")
			if err != nil {
				fmt.Println(err)
				return err
			}
		}
		
		if err := snapshotter.Remove(ctx, "empty"); err == nil {
                fmt.Println("cleaned up a snapshot from before:)")
        	}
		mounts_empty := PrepareSnapshot(ctx, snapshotter, image_empty, "empty")

		// write diffs between patched filesystem and empty mount to content store
		// this is basically a layer

		timeToCreateLayerStart := time.Now()

		diffs, err := client.DiffService().Compare(ctx, mounts_empty, mounts_from, diff.WithMediaType(ocispec.MediaTypeImageLayerGzip), diff.WithReference("custom-ref"))
		if err != nil {
			fmt.Printf("error creating diff: %v\n", err)
			return err
		}

		if err := manifest2.ReplaceWithLayer(ctx, client.ContentStore(), diffs, imageConfig); err != nil {
			fmt.Printf("error modifying target manifest: %v\n", err)
			return err
		}

		timeToCreateLayer := time.Since(timeToCreateLayerStart)

		timeCreateImageStart := time.Now()

		// Create a new image from the modified manifest

		_, err = client.ImageService().Create(ctx,
			images.Image{
				Name: image2ref,
				Target: ocispec.Descriptor{
					Digest:    manifest2.Descriptor().Digest,
					Size:      manifest2.Descriptor().Size,
					MediaType: manifest2.Descriptor().MediaType,
				},
			},
		)
		if err != nil {
			fmt.Println(err)
		}

		timeToCreateImage := time.Since(timeCreateImageStart)

		new_image, err := client.GetImage(ctx, image2ref)
		if err != nil {
			fmt.Println("ERROR GETTING NEW IMAGE: ", err)
			return err
		}

		timeToUnpackStart := time.Now()

		if err := new_image.Unpack(ctx, containerd.DefaultSnapshotter); err != nil {
			fmt.Println("ERROR UNPACKING IMAGE: ", err)
			return err
		}

		timeToUnpack := time.Since(timeToUnpackStart)

		// Get the delta file size in bytes
		fileInfo, err := os.Stat(filepath + ".zst")
		if err != nil {
			fmt.Println("Error:", err)
			return err
		}

		// Get the file size in bytes from the file info
		fileSizeBytes := fileInfo.Size()

		// Convert file size to megabytes
		fileSizeMB := float64(fileSizeBytes) / 1048576.0

		// Get the image size
		imageSizeBytes, err := new_image.Size(ctx)
		if err != nil {
			fmt.Println("Error getting image info:", err)
			return err
		}

		// Convert image size to megabytes
		imageSizeMB := float64(imageSizeBytes) / 1048576.0

		fmt.Printf("Delta diff file size: %v MB\n", fileSizeMB)
		fmt.Printf("Image size: %v MB\n", imageSizeMB)
		fmt.Printf("Compression ratio (original:compressed): %v\n", imageSizeMB/fileSizeMB)

		fmt.Printf("Time to receive delta diff file since request: %v\n", timeRequestEnd)
		fmt.Printf("Time to apply delta: %v\n", timeToApplyDelta)
		fmt.Printf("Time to create layer: %v\n", timeToCreateLayer)
		fmt.Printf("Time to create image: %v\n", timeToCreateImage)
		fmt.Printf("Time to unpack image: %v\n", timeToUnpack)

		after, _ := cpu.Get()
		totalTime := time.Since(timeStart)

		totalDiff := float64(after.Total - before.Total)
		userDiff := float64(after.User - before.User)
		usage := (userDiff / totalDiff) * 100

		fmt.Printf("\nCPU Time: %v s\n", float64(totalTime)/10e+8 * usage / 100)
		fmt.Printf("Total Time: %v\n", totalTime)
		fmt.Printf("CPU Usage: %.2f%%\n", usage)
		return nil

	}); err != nil {
		fmt.Printf("error mounting from-image: %v\n", err)
		return
	}

	fmt.Printf("Successfully patched image %s with delta diff file %s\n", image2name, filepath)

}

func PrepareSnapshot(ctx context.Context, snapshotter snapshots.Snapshotter, image containerd.Image, key string) []mount.Mount {

	diffIDs, err := image.RootFS(ctx)
	if err != nil {
		panic(err)
	}

	parent := identity.ChainID(diffIDs).String()

	mounts, err := snapshotter.Prepare(ctx, key, parent)
	if err != nil {
		panic(err)
	}

	return mounts
}
