package agent

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWatchVolumeEmitsDebouncedBatch(t *testing.T) {
	dir := t.TempDir()
	pool := Pool{Name: "default", Path: dir}
	if err := EnsurePool(pool, false); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	batches := make(chan EventBatch, 1)
	errCh := make(chan error, 1)
	go func() {
		errCh <- WatchVolume(ctx, WatchConfig{
			Pool:         pool,
			Namespace:    "default",
			Volume:       "demo",
			IncludePaths: []string{"save/**"},
			Debounce:     50 * time.Millisecond,
		}, func(_ context.Context, batch EventBatch) error {
			batches <- batch
			cancel()
			return nil
		})
	}()

	time.Sleep(100 * time.Millisecond)
	target := filepath.Join(dir, "default", "demo", "save", "world.txt")
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}

	select {
	case batch := <-batches:
		if batch.Volume != "demo" || len(batch.Events) == 0 {
			t.Fatalf("batch = %#v", batch)
		}
	case err := <-errCh:
		t.Fatalf("watch returned before batch: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for batch")
	}
}

func TestWatchVolumeEmitsForDeepIncludeCreatedAfterStart(t *testing.T) {
	dir := t.TempDir()
	pool := Pool{Name: "default", Path: dir}
	if err := EnsurePool(pool, false); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	batches := make(chan EventBatch, 1)
	errCh := make(chan error, 1)
	go func() {
		errCh <- WatchVolume(ctx, WatchConfig{
			Pool:      pool,
			Namespace: "games",
			Volume:    "windrose-canary",
			IncludePaths: []string{
				"steam-root/windrose/WindowsServer/R5/Saved/**",
			},
			Debounce: 50 * time.Millisecond,
		}, func(_ context.Context, batch EventBatch) error {
			batches <- batch
			cancel()
			return nil
		})
	}()

	time.Sleep(100 * time.Millisecond)
	target := filepath.Join(dir, "games", "windrose-canary", "steam-root", "windrose", "WindowsServer", "R5", "Saved", "marker.txt")
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}

	select {
	case batch := <-batches:
		if batch.Volume != "windrose-canary" || len(batch.Events) == 0 {
			t.Fatalf("batch = %#v", batch)
		}
	case err := <-errCh:
		t.Fatalf("watch returned before batch: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for batch")
	}
}
