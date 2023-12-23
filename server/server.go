package main

import (
	"context"
	"deltadiff/api"
	"deltadiff/manifest"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strings"
	"strconv"
	"sync"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/images"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/snapshots"
	"github.com/opencontainers/image-spec/identity"
)

var SERVER_ADDRESS string

const CHUNK_SIZE = 32 * 1024
const RSYNC_BLOCK_SIZE = 382

type deltaDiffService struct {
	client *containerd.Client

	// embed the unimplemented server
	api.UnimplementedDeltaDiffServiceServer
}

func (c *deltaDiffService) GetManifest(ctx context.Context, r *api.ManifestRequest) (*api.ManifestResponse, error) {
	fmt.Println("GetManifest was called")

	contentStore := c.client.ContentStore()

	// Check if image reference is provided
	if r.Image.Reference == "" {
		return &api.ManifestResponse{Manifest: nil}, status.Errorf(codes.InvalidArgument, "image reference is required")
	}

	image, err := c.client.GetImage(ctx, r.Image.Reference)
	if err != nil {
		fmt.Printf("Image %v not found, pulling...\n", r.Image.Reference)
		image, err = c.client.Pull(ctx, r.Image.Reference, containerd.WithPullUnpack)
		if err != nil {
			return &api.ManifestResponse{Manifest: nil}, status.Errorf(codes.InvalidArgument, "error pulling image %v: %v", r.Image.Reference, err)
		}
	}

	target_manifest, err := manifest.LoadManifest(ctx, contentStore, image.Target())
	if err != nil {
		fmt.Println(err)
		return nil, status.Errorf(codes.InvalidArgument, "error loading manifest: %v", err)
	}

	m_impl, err := manifest.LoadManifestImpl(ctx, contentStore, image.Target())
	if err != nil {
		fmt.Println(err)
		return nil, status.Errorf(codes.InvalidArgument, "error loading manifest impl: %v", err)
	}

	if target_manifest.Descriptor().MediaType == images.MediaTypeDockerSchema2ManifestList {
		// Retrieve manifest for specific platform
		target_manifest, err = manifest.LoadManifestFromList(ctx, image.Target(), contentStore, r.Os, r.Arch)
		if err != nil {
			fmt.Println(err)
			return nil, status.Errorf(codes.InvalidArgument, "error loading manifest from list: %v", err)
		}

		m_impl, err = manifest.LoadManifestImplFromList(ctx, image.Target(), contentStore, runtime.GOOS, runtime.GOARCH)
		if err != nil {
			fmt.Println(err)
			return nil, status.Errorf(codes.InvalidArgument, "error loading manifest from list: %v", err)
		}
	}

	// Deserialize the image config
	imageConfigDesc, err := manifest.GetDescriptor(m_impl.D["config"])
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "error getting image config descriptor: %v", err)
	}

	// Get the image configuration.
	fmt.Println(imageConfigDesc)
	p, err := content.ReadBlob(ctx, contentStore, imageConfigDesc)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "error reading image config blob: %v", err)
	}

	manifest_bytes, err := bson.Marshal(target_manifest)
	if err != nil {
		fmt.Println("Error Marshalling Manifest", err)
		return &api.ManifestResponse{Manifest: nil}, status.Errorf(codes.InvalidArgument, "error marshalling manifest: %v", err)
	}

	return &api.ManifestResponse{
		Manifest:    manifest_bytes,
		ImageConfig: p,
	}, nil

}



var mutexes map[string]*sync.Mutex

func init() {
    mutexes = make(map[string]*sync.Mutex)
}


func (c *deltaDiffService) CalculateDeltaDiffs(r *api.CalcImageDiffsRequest, stream api.DeltaDiffService_CalculateDeltaDiffsServer) error {
	fmt.Println("CalculateDeltaDiffs was called")

	ctx := context.Background()

	// Before anything, check if the diff file already exists
	// If it does, we can just send it to the client
	image1name := strings.Split(r.Image1.Reference, "/")[len(strings.Split(r.Image1.Reference, "/"))-1]
	image2name := strings.Split(r.Image2.Reference, "/")[len(strings.Split(r.Image2.Reference, "/"))-1]
	patch_location := fmt.Sprintf("/tmp/delta-patch-from-%s-to-%s.zst", image1name, image2name)
	
	// We use mutexes to check whether another proccess is currently creating a patch file.
	// If it does, there is no need to create it twice.
	mutex, exists := mutexes[patch_location]
    if !exists {
        mutex = &sync.Mutex{}
        mutexes[patch_location] = mutex
    }
	
	// The critical section is checking the if condition.
	mutexes[patch_location].Lock()
	if _, err := os.Stat(patch_location); err == nil {
		// If the patchfile already exists, it can be sent and there is no need for the mutex to be locked
		mutexes[patch_location].Unlock()
		fmt.Printf("File %s already exists, sending to client...\n", patch_location)

		file, err := os.Open(patch_location)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "error reading diff patch file: %v", err)
		}
		defer file.Close()

		timeToTransferDeltaStart := time.Now()

		buf := make([]byte, CHUNK_SIZE)
		for {
			n, err := file.Read(buf)
			//fmt.Println(n)
			if err != nil {
				// fmt.Println(err)
				break
			}
			if err := stream.Send(&api.CalculateDeltaDiffsResponse{DeltaDiff: buf[:n]}); err != nil {
				return status.Errorf(codes.InvalidArgument, "error sending diff patch file: %v", err)
			}
		}

		timeToTransferDelta := time.Since(timeToTransferDeltaStart)

		fileInfo, err := os.Stat(patch_location)
		if err != nil {
			fmt.Println("Error:", err)
			return err
		}

		// Get the file size in bytes from the file info
		fileSizeBytes := fileInfo.Size()

		// Convert file size to megabytes
		fileSizeMB := float64(fileSizeBytes) / 1048576.0

		fmt.Printf("Transferred %v MB worth of Δ in %vs\n", fileSizeMB, timeToTransferDelta.Seconds())

		return nil
	}

	// Check if image references are provided
	if r.Image1.Reference == "" || r.Image2.Reference == "" {
		return status.Errorf(codes.InvalidArgument, "image references are required")
	}

	timeStartPullImages := time.Now()

	// Get images; if they don't exist, pull them
	image1, err := c.client.GetImage(ctx, r.Image1.Reference)
	if err != nil {
		fmt.Printf("Image %v not found, pulling...\n", r.Image1.Reference)
		image1, err = c.client.Pull(ctx, r.Image1.Reference, containerd.WithPullUnpack)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "error pulling image %v: %v", r.Image1.Reference, err)
		}
	}

	image2, err := c.client.GetImage(ctx, r.Image2.Reference)
	if err != nil {
		fmt.Printf("Image %v not found, pulling...\n", r.Image2.Reference)
		image2, err = c.client.Pull(ctx, r.Image2.Reference, containerd.WithPullUnpack)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "error pulling image %v: %v", r.Image2.Reference, err)
		}
	}

	// Most useful when images are not available locally
	timeToPullImages := time.Since(timeStartPullImages)

	// Get image snapshots
	snapshotter := c.client.SnapshotService("overlayfs")
	defer snapshotter.Close()

	// Get mounts for snapshots
	var mounts1, mounts2 []mount.Mount
	var key1, key2 string
	mounts1, key1, err = getMounts(ctx, snapshotter, image1)
	if err != nil {
		fmt.Println("Could not get mounts for image1.")
		return status.Errorf(codes.InvalidArgument, "error getting mounts (lower): %v", err)
	}
	defer snapshotter.Remove(ctx, key1)

	mounts2, key2, err = getMounts(ctx, snapshotter, image2)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "error getting mounts (upper): %v", err)
	}
	defer snapshotter.Remove(ctx, key2)

	fmt.Println("mounts2: ", mounts2)

	if err := mount.WithTempMount(ctx, mounts1, func(from_root string) error {
		return mount.WithTempMount(ctx, mounts2, func(to_root string) error {
			fmt.Println("from_root: ", from_root)
			fmt.Println("to_root: ", to_root)

			timeCreateDeltaStart := time.Now()
			rsyncBlockSize := strconv.Itoa(RSYNC_BLOCK_SIZE)
			patch_filename := fmt.Sprintf("delta-patch-from-%s-to-%s", image1name, image2name)
			// execute rsync between from and to and create binary diff file
			cmd := exec.Command("rsync",
				"-avH",
				"--partial",
				"--delete",
				"--only-write-batch="+patch_filename,
				"--block-size="+rsyncBlockSize,
				"--no-i-r",
				"--one-file-system",
				to_root+"/", from_root+"/")
			cmd.Dir = "/tmp"

			output, err := cmd.CombinedOutput()
			fmt.Println(string(output))

			if err != nil {
				return status.Errorf(codes.InvalidArgument, "error creating diff patch: %v", err)
			}		
			
			// At this point the patch is created, so the other processes can continue
			
			fmt.Println(patch_location)
			fmt.Println(mutexes)
			
			
			patch_location := "/tmp/" + patch_filename

			// Compress the diff patch file with zstd
			cmd = exec.Command("zstd", "-f", "-q", "-9", "-o", patch_location+".zst", patch_location)
			cmd.Dir = "/tmp"
			output, err = cmd.CombinedOutput()
			fmt.Println(string(output))
			mutex.Unlock()
			if err != nil {
				return status.Errorf(codes.InvalidArgument, "error creating diff patch: %v", err)
			}

			file, err := os.Open(patch_location + ".zst")
			if err != nil {
				return status.Errorf(codes.InvalidArgument, "error reading diff patch file: %v", err)
			}
			defer file.Close()

			timeToCreateDelta := time.Since(timeCreateDeltaStart)

			timeToTransferDeltaStart := time.Now()

			buf := make([]byte, CHUNK_SIZE)
			for {
				n, err := file.Read(buf)
				//fmt.Println(n)
				if err != nil {
					// fmt.Println(err)
					break
				}
				if err := stream.Send(&api.CalculateDeltaDiffsResponse{DeltaDiff: buf[:n]}); err != nil {
					return status.Errorf(codes.InvalidArgument, "error sending diff patch file: %v", err)
				}
			}

			timeToTransferDelta := time.Since(timeToTransferDeltaStart)

			// get file size of /tmp/delta-patch

			filepath := patch_location + ".zst"
			fileInfo, err := os.Stat(filepath)
			if err != nil {
				fmt.Println("Error:", err)
				return err
			}

			// Get the file size in bytes from the file info
			fileSizeBytes := fileInfo.Size()

			// Convert file size to megabytes
			fileSizeMB := float64(fileSizeBytes) / 1048576.0

			// Get the image size
			imageSizeBytes, err := image2.Size(ctx)
			if err != nil {
				fmt.Println("Error getting image info:", err)
				return err
			}

			// Convert image size to megabytes
			imageSizeMB := float64(imageSizeBytes) / 1048576.0

			fmt.Printf("File size of %s: %.2f MB\n", filepath, fileSizeMB)
			fmt.Printf("Size of compressed image is %.2f MB\n", float64(imageSizeMB))
			fmt.Printf("Compression ratio: %.2f\n", float64((imageSizeMB))/fileSizeMB)
			fmt.Printf("Compressed to: %.2f%% of initial image size \n", float64(fileSizeMB/imageSizeMB)*100)
			fmt.Println("------TIME STATISTICS------")
			fmt.Printf("Time to pull images: %v\n", timeToPullImages)
			fmt.Printf("Time to create delta: %v\n", timeToCreateDelta)
			fmt.Printf("Time to transfer delta: %v\n", timeToTransferDelta)

			return nil

		})
	}); err != nil {
		return status.Errorf(codes.InvalidArgument, "error creating snapshot diffs: %v", err)
	}
	return nil
}

func main() {

	// Create a gRPC server
	rpc := grpc.NewServer()

	client, e := containerd.New("/run/containerd/containerd.sock", containerd.WithDefaultNamespace("default"))
	if e != nil {
		fmt.Printf("error: %v\n", e)
		os.Exit(1)
	}

	api.RegisterDeltaDiffServiceServer(rpc, &deltaDiffService{client: client})

	// Listen and serve
	// For IPv4, use:   ("tcp", IP_ADDRESS:PORT)
	// For unix sockets, use: ("unix", "/var/run/mydiffer.sock")

	if len(os.Args) != 2 {
		fmt.Println("Usage: server <addr>")
		return
	}

	SERVER_ADDRESS = os.Args[1]

	// Check if address is unix socket or ip address

	var addressType string

	if strings.HasPrefix(SERVER_ADDRESS, "/") {
		addressType = "unix"
	} else {
		addressType = "tcp"
	}

	l, err := net.Listen(addressType, SERVER_ADDRESS)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		return
	}
	defer l.Close()

	go func() {
		if err := rpc.Serve(l); err != nil {
			fmt.Printf("error: %v\n", err)
			return
		}
		defer rpc.Stop()
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	// Block until a signal is received.
	s := <-c
	fmt.Println("Got signal, will now exit gracefully...:", s)

}

func getMounts(ctx context.Context, sn snapshots.Snapshotter, image containerd.Image) ([]mount.Mount, string, error) {
	// get diffIDs of image
	diffIDs, err := image.RootFS(ctx)
	if err != nil {
		return nil, "", status.Errorf(codes.InvalidArgument, "error getting image rootfs: %v", err)
	}

	// get snapshot info - image should be unpacked
	info, err := sn.Stat(ctx, identity.ChainID(diffIDs).String())
	if err != nil {
		return nil, "", status.Errorf(codes.InvalidArgument, "error getting snapshot info: %v", err)
	}

	// get mounts for snapshots
	var key string
	var mounts []mount.Mount
	if info.Kind == snapshots.KindActive {
		key = ""
		mounts, err = sn.Mounts(ctx, identity.ChainID(diffIDs).String())
		if err != nil {
			return nil, "", err
		}
	} else {
		key := fmt.Sprintf("%s-view-%s", identity.ChainID(diffIDs).String(), time.Now().Format(time.RFC3339Nano))
		mounts, err = sn.View(ctx, key, identity.ChainID(diffIDs).String())
		if err != nil {
			return nil, "", err
		}
	}
	return mounts, key, nil
}

func RetrieveImage(ctx context.Context, client *containerd.Client, imageRef string) (containerd.Image, error) {
	image, err := client.GetImage(ctx, imageRef)
	if err != nil {
		fmt.Printf("Image %v not found, pulling...\n", imageRef)
		image, err = client.Pull(ctx, imageRef, containerd.WithPullUnpack)
		if err != nil {
			return nil, err
		}
	}
	return image, nil
}
