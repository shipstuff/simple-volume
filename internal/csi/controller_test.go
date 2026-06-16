package csi

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/shipstuff/simple-volume/internal/controller"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCreateVolumeReturnsLogicalHandle(t *testing.T) {
	resp, err := CreateVolume(CreateVolumeRequest{
		Namespace:     "games",
		Name:          "demo",
		StoragePool:   "default",
		CapacityBytes: 1024,
		ReplicaCount:  1,
	}, []controller.AgentStatus{
		{Node: "sf-west-1", Pool: "default", Healthy: true, FreeBytes: 2048},
	})
	if err != nil {
		t.Fatalf("CreateVolume returned error: %v", err)
	}
	if resp.VolumeHandle != "games/demo" {
		t.Fatalf("handle = %q", resp.VolumeHandle)
	}
	if resp.ActiveNode != "sf-west-1" {
		t.Fatalf("active = %q", resp.ActiveNode)
	}
}

func TestServerCreateVolumeIncludesPVCContext(t *testing.T) {
	s := &Server{poolName: "default"}
	resp, err := s.CreateVolume(context.Background(), &csipb.CreateVolumeRequest{
		Name: "pvc-123",
		Parameters: map[string]string{
			provisionerPVCNamespace: "games",
			provisionerPVCName:      "windrose-canary-data",
		},
	})
	if err != nil {
		t.Fatalf("CreateVolume returned error: %v", err)
	}
	ctx := resp.GetVolume().GetVolumeContext()
	if ctx[volumeContextPVCNamespace] != "games" {
		t.Fatalf("namespace context = %q", ctx[volumeContextPVCNamespace])
	}
	if ctx[volumeContextPVCName] != "windrose-canary-data" {
		t.Fatalf("name context = %q", ctx[volumeContextPVCName])
	}
}

func TestServerCreateVolumeRequiresPVCMetadata(t *testing.T) {
	s := &Server{poolName: "default"}
	_, err := s.CreateVolume(context.Background(), &csipb.CreateVolumeRequest{
		Name: "pvc-123",
	})
	if err == nil {
		t.Fatal("CreateVolume returned nil error, want missing metadata error")
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("CreateVolume error code = %s, want %s", status.Code(err), codes.FailedPrecondition)
	}
}

func TestServerVolumePathUsesPVCNamespace(t *testing.T) {
	pool := t.TempDir()
	s := &Server{poolName: "default", poolPath: pool}
	path, err := s.volumePath("pvc-123", map[string]string{
		volumeContextPVCNamespace: "games",
	})
	if err != nil {
		t.Fatalf("volumePath returned error: %v", err)
	}
	want := filepath.Join(pool, "games", "pvc-123")
	if path != want {
		t.Fatalf("path = %q, want %q", path, want)
	}
}

func TestServerVolumePathRequiresNamespaceForNewPV(t *testing.T) {
	pool := t.TempDir()
	s := &Server{poolName: "default", poolPath: pool}
	_, err := s.volumePath("pvc-123", nil)
	if err == nil {
		t.Fatal("volumePath returned nil error, want missing namespace error")
	}
	if _, statErr := os.Stat(filepath.Join(pool, "default", "pvc-123")); !os.IsNotExist(statErr) {
		t.Fatalf("default namespace path stat err = %v, want not exist", statErr)
	}
}

func TestServerVolumePathFindsExistingNamespaceForOldPV(t *testing.T) {
	pool := t.TempDir()
	existing := filepath.Join(pool, "games", "pvc-123")
	if err := os.MkdirAll(existing, 0o755); err != nil {
		t.Fatal(err)
	}
	s := &Server{poolName: "default", poolPath: pool}
	path, err := s.volumePath("pvc-123", nil)
	if err != nil {
		t.Fatalf("volumePath returned error: %v", err)
	}
	if path != existing {
		t.Fatalf("path = %q, want %q", path, existing)
	}
}

func TestApplyVolumeMountGroup(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "data")
	if err := os.WriteFile(file, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	gid := os.Getgid()
	if err := applyVolumeMountGroup(dir, strconv.Itoa(gid)); err != nil {
		t.Fatalf("applyVolumeMountGroup returned error: %v", err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("stat metadata = %T, want *syscall.Stat_t", info.Sys())
	}
	if stat.Gid != uint32(gid) {
		t.Fatalf("dir gid = %d, want %d", stat.Gid, gid)
	}
	if info.Mode().Perm()&0o070 == 0 {
		t.Fatalf("dir mode = %o, want group permissions", info.Mode().Perm())
	}
	fileInfo, err := os.Stat(file)
	if err != nil {
		t.Fatal(err)
	}
	fileStat, ok := fileInfo.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("file stat metadata = %T, want *syscall.Stat_t", fileInfo.Sys())
	}
	if fileStat.Gid != uint32(gid) {
		t.Fatalf("file gid = %d, want %d", fileStat.Gid, gid)
	}
	if fileInfo.Mode().Perm()&0o060 == 0 {
		t.Fatalf("file mode = %o, want group read/write permissions", fileInfo.Mode().Perm())
	}
}
