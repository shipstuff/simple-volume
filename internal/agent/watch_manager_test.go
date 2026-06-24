package agent

import (
	"context"
	"fmt"
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

type selectiveBatchSender struct {
	fail map[string]bool
}

func (s selectiveBatchSender) SendBatch(_ context.Context, target TargetRef, _ EventBatch) error {
	if s.fail[target.URL] {
		return fmt.Errorf("send failed")
	}
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
		Namespace:       "default",
		Volume:          "demo",
		Source:          SourceRef{WebDAVURL: "http://source:8081"},
		Targets:         []TargetRef{{URL: "http://target:8080", Token: "secret"}},
		IncludePaths:    []string{"save/**"},
		Ownership:       OwnershipPolicy{UID: &uid},
		Debounce:        "50ms",
		ConsistencyMode: ConsistencyModeLive,
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

func TestWatchManagerRequiresConfirmedReplicasBeforeSuccessfulDelivery(t *testing.T) {
	dir := t.TempDir()
	pool := Pool{Name: "default", Path: dir}
	if err := EnsurePool(pool, false); err != nil {
		t.Fatal(err)
	}
	manager := NewWatchManager(pool, selectiveBatchSender{fail: map[string]bool{"http://target-b:8080": true}})
	confirmed := 2
	status, err := manager.Start(context.Background(), WatchStartRequest{
		Namespace:         "default",
		Volume:            "demo",
		Source:            SourceRef{WebDAVURL: "http://source:8081"},
		Targets:           []TargetRef{{URL: "http://target-a:8080"}, {URL: "http://target-b:8080"}},
		IncludePaths:      []string{"save/**"},
		Debounce:          "50ms",
		ConsistencyMode:   ConsistencyModeLive,
		ConfirmedReplicas: &confirmed,
	})
	if err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	if status.ConfirmedReplicas != 2 {
		t.Fatalf("ConfirmedReplicas = %d, want 2", status.ConfirmedReplicas)
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

	deadline := time.After(3 * time.Second)
	for {
		watchStatus, ok := manager.Status("default", "demo")
		if !ok {
			t.Fatal("watch status not found")
		}
		if watchStatus.LastDeliveryError != "" {
			if watchStatus.DeliveredBatchCount != 0 {
				t.Fatalf("DeliveredBatchCount = %d, want 0 for unconfirmed generation", watchStatus.DeliveredBatchCount)
			}
			return
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for unconfirmed delivery status")
		case <-time.After(10 * time.Millisecond):
		}
	}
}
