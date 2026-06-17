package agent

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type recordingBatchSender struct {
	batches chan EventBatch
}

func (s recordingBatchSender) SendBatch(_ context.Context, _ TargetRef, batch EventBatch) error {
	s.batches <- batch
	return nil
}

func TestWatchManagerSendsWatchedBatches(t *testing.T) {
	dir := t.TempDir()
	pool := Pool{Name: "default", Path: dir}
	if err := EnsurePool(pool, false); err != nil {
		t.Fatal(err)
	}
	sender := recordingBatchSender{batches: make(chan EventBatch, 1)}
	manager := NewWatchManager(pool, sender)
	uid := int64(10000)
	status, err := manager.Start(context.Background(), WatchStartRequest{
		Namespace:    "default",
		Volume:       "demo",
		Source:       SourceRef{WebDAVURL: "http://source:8081"},
		Targets:      []TargetRef{{URL: "http://target:8080", Token: "secret"}},
		IncludePaths: []string{"save/**"},
		Ownership:    OwnershipPolicy{UID: &uid},
		Debounce:     "50ms",
	})
	if err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if !status.Running {
		t.Fatalf("status = %#v", status)
	}
	if status.Ownership.UID == nil || *status.Ownership.UID != 10000 {
		t.Fatalf("status ownership = %#v", status.Ownership)
	}
	defer manager.Stop("default", "demo")

	time.Sleep(100 * time.Millisecond)
	target := filepath.Join(dir, "default", "demo", "save", "world.txt")
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}

	select {
	case batch := <-sender.batches:
		if batch.Source.WebDAVURL != "http://source:8081" {
			t.Fatalf("source = %#v", batch.Source)
		}
		if batch.Namespace != "default" || batch.Volume != "demo" || len(batch.Events) == 0 {
			t.Fatalf("batch = %#v", batch)
		}
		if batch.Ownership.UID == nil || *batch.Ownership.UID != 10000 {
			t.Fatalf("batch ownership = %#v", batch.Ownership)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for sent batch")
	}

	deadline := time.After(3 * time.Second)
	for {
		watchStatus, ok := manager.Status("default", "demo")
		if !ok {
			t.Fatal("watch status not found")
		}
		if watchStatus.DeliveredBatchCount == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("DeliveredBatchCount = %d, want 1", watchStatus.DeliveredBatchCount)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestWatchManagerRejectsInvalidStart(t *testing.T) {
	manager := NewWatchManager(Pool{Name: "default", Path: t.TempDir()}, nil)
	_, err := manager.Start(context.Background(), WatchStartRequest{
		Volume: "demo",
		Source: SourceRef{WebDAVURL: "http://source:8081"},
	})
	if err == nil {
		t.Fatal("Start returned nil error without targets")
	}
}
