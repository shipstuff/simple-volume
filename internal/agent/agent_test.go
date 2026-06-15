package agent

import (
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

func TestResolveVolumePathStaysInsidePool(t *testing.T) {
	path, err := ResolveVolumePath(VolumePath{
		Pool:      Pool{Name: "default", Path: "/mnt/simple-volume"},
		Namespace: "../games",
		Name:      "demo",
	})
	if err != nil {
		t.Fatalf("ResolveVolumePath returned error: %v", err)
	}
	if path != "/mnt/simple-volume/games/demo" {
		t.Fatalf("path = %q", path)
	}
}

func TestBuildSyncCommandRsync(t *testing.T) {
	cmd, args, err := BuildSyncCommand(SyncRequest{
		Method:     "rsync",
		SourceURL:  "http://agent/source/",
		TargetPath: "/data/target",
		Excludes:   []string{"cache/"},
		Delete:     true,
	})
	if err != nil {
		t.Fatalf("BuildSyncCommand returned error: %v", err)
	}
	if cmd != "rsync" {
		t.Fatalf("cmd = %q", cmd)
	}
	wantLast := "/data/target"
	if args[len(args)-1] != wantLast {
		t.Fatalf("last arg = %q, want %q; args=%#v", args[len(args)-1], wantLast, args)
	}
}

func TestTokenAuthorizer(t *testing.T) {
	req, _ := http.NewRequest(http.MethodGet, "/sync", nil)
	req.Header.Set("Authorization", "Bearer secret")
	if !((TokenAuthorizer{Token: "secret"}).Authorize(req)) {
		t.Fatal("expected token to authorize")
	}
}

func TestEnsurePoolInitializesEmptyDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := EnsurePool(Pool{Name: "default", Path: dir}, false); err != nil {
		t.Fatalf("EnsurePool returned error: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, PoolMarkerFile)); err != nil {
		t.Fatalf("marker was not created: %v", err)
	}
}

func TestEnsurePoolRejectsNonEmptyUninitializedDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "existing"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	err := EnsurePool(Pool{Name: "default", Path: dir}, false)
	if !errors.Is(err, ErrNonEmptyPool) {
		t.Fatalf("err = %v, want ErrNonEmptyPool", err)
	}
}

func TestEnsurePoolAllowsNonEmptyOverride(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "existing"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := EnsurePool(Pool{Name: "default", Path: dir}, true); err != nil {
		t.Fatalf("EnsurePool returned error: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, PoolMarkerFile)); err != nil {
		t.Fatalf("marker was not created: %v", err)
	}
}
