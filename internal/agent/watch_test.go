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

func TestWatchedSnapshotDiffDetectsCreateUpdateDelete(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "default", "demo")
	saveDir := filepath.Join(root, "save")
	if err := os.MkdirAll(saveDir, 0o755); err != nil {
		t.Fatal(err)
	}
	oldFile := filepath.Join(saveDir, "old.txt")
	changedFile := filepath.Join(saveDir, "changed.txt")
	if err := os.WriteFile(oldFile, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(changedFile, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	filter := PathFilter{IncludePaths: []string{"save/**"}}
	before, err := snapshotWatchedFiles(root, filter)
	if err != nil {
		t.Fatal(err)
	}

	if err := os.Remove(oldFile); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(changedFile, []byte("new content"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(saveDir, "new.txt"), []byte("new"), 0o644); err != nil {
		t.Fatal(err)
	}
	after, err := snapshotWatchedFiles(root, filter)
	if err != nil {
		t.Fatal(err)
	}

	events := diffWatchedSnapshots(before, after)
	want := []FileEvent{
		{Path: "save/changed.txt", Op: EventOpUpsert},
		{Path: "save/new.txt", Op: EventOpUpsert},
		{Path: "save/old.txt", Op: EventOpDelete},
	}
	if len(events) != len(want) {
		t.Fatalf("events = %#v, want %#v", events, want)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("events[%d] = %#v, want %#v; all=%#v", i, events[i], want[i], events)
		}
	}
}
