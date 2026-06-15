package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/shipstuff/simple-volume/internal/agent"
)

func TestDesiredReplicationsDiscoversActiveAndReplicaAgents(t *testing.T) {
	storageClass := "simple-volume"
	client := fake.NewSimpleClientset(
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "data",
				Namespace: "default",
				Annotations: map[string]string{
					AnnotationReplicationEnabled: "true",
					AnnotationIncludePaths:       "writes.log, saves/**",
					AnnotationExcludePaths:       "downloads/**",
					AnnotationDebounce:           "2s",
					AnnotationFullSyncOnStart:    "true",
					AnnotationFullSyncSchedule:   "0 4 * * *",
				},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: &storageClass,
				VolumeName:       "pvc-123",
			},
		},
		workloadPod("writer", "default", "kapolei-pacific-1", "data"),
		agentPod("agent-kap", "kapolei-pacific-1", "10.0.0.10"),
		agentPod("agent-sf", "sf-west-1", "10.0.0.11"),
		agentPod("agent-fresno", "fresno-west-1", "10.0.0.12"),
	)
	controller := NewReplicationController(client, ReplicationControllerConfig{
		Namespace:        "simple-volume-system",
		StorageClassName: storageClass,
	})
	desired, err := controller.DesiredReplications(context.Background(), "secret")
	if err != nil {
		t.Fatalf("DesiredReplications returned error: %v", err)
	}
	if len(desired) != 1 {
		t.Fatalf("desired = %#v", desired)
	}
	got := desired[0]
	if got.Namespace != "default" || got.ClaimName != "data" || got.Volume != "pvc-123" {
		t.Fatalf("desired identity = %#v", got)
	}
	if got.ActiveNode != "kapolei-pacific-1" || got.SourceURL != "http://10.0.0.10:8081" {
		t.Fatalf("active/source = %#v", got)
	}
	if len(got.Targets) != 2 {
		t.Fatalf("targets = %#v", got.Targets)
	}
	if got.Targets[0].Token != "secret" || got.Targets[1].Token != "secret" {
		t.Fatalf("target tokens = %#v", got.Targets)
	}
	if got.IncludePaths[0] != "writes.log" || got.IncludePaths[1] != "saves/**" {
		t.Fatalf("include paths = %#v", got.IncludePaths)
	}
	if !got.FullSync || got.FullSchedule != "0 4 * * *" || got.Debounce != "2s" {
		t.Fatalf("sync policy = %#v", got)
	}
}

func TestReconcileOneStartsWatchAndRunsStartupFullSyncOnce(t *testing.T) {
	var watchStarts int
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/replication/watch/start" {
			t.Fatalf("source path = %s", r.URL.Path)
		}
		watchStarts++
		var req agent.WatchStartRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode watch request: %v", err)
		}
		if req.Volume != "pvc-123" || len(req.Targets) != 2 {
			t.Fatalf("watch request = %#v", req)
		}
		_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	}))
	defer source.Close()

	var fullSyncs int
	target := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/replication/full-sync" {
				t.Fatalf("target path = %s", r.URL.Path)
			}
			fullSyncs++
			_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
		}))
	}
	targetA := target()
	defer targetA.Close()
	targetB := target()
	defer targetB.Close()

	controller := NewReplicationController(nil, ReplicationControllerConfig{})
	desired := DesiredReplication{
		Namespace:    "default",
		ClaimName:    "data",
		Volume:       "pvc-123",
		ActiveNode:   "kapolei-pacific-1",
		SourceURL:    source.URL,
		Targets:      []agent.TargetRef{{URL: targetA.URL, Token: "secret"}, {URL: targetB.URL, Token: "secret"}},
		IncludePaths: []string{"writes.log"},
		Debounce:     "2s",
		FullSync:     true,
	}

	if err := controller.reconcileOne(context.Background(), desired, "secret", time.Date(2026, 6, 15, 3, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("reconcileOne returned error: %v", err)
	}
	if err := controller.reconcileOne(context.Background(), desired, "secret", time.Date(2026, 6, 15, 3, 1, 0, 0, time.UTC)); err != nil {
		t.Fatalf("second reconcileOne returned error: %v", err)
	}
	if watchStarts != 1 {
		t.Fatalf("watchStarts = %d, want 1", watchStarts)
	}
	if fullSyncs != 2 {
		t.Fatalf("fullSyncs = %d, want 2", fullSyncs)
	}
}

func TestShouldRunScheduledFullSync(t *testing.T) {
	if !shouldRunScheduledFullSync("0 4 * * *", time.Date(2026, 6, 15, 4, 0, 30, 0, time.UTC)) {
		t.Fatal("expected schedule to match")
	}
	if shouldRunScheduledFullSync("0 4 * * *", time.Date(2026, 6, 15, 4, 1, 0, 0, time.UTC)) {
		t.Fatal("expected schedule not to match")
	}
	if shouldRunScheduledFullSync("*/5 * * * *", time.Date(2026, 6, 15, 4, 0, 0, 0, time.UTC)) {
		t.Fatal("unsupported schedule should not match")
	}
}

func workloadPod(name, namespace, node, claim string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: corev1.PodSpec{
			NodeName: node,
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim},
				},
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func agentPod(name, node, ip string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "simple-volume-system",
			Labels: map[string]string{
				"app.kubernetes.io/component": "node",
			},
		},
		Spec: corev1.PodSpec{NodeName: node},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: ip,
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: corev1.ConditionTrue,
			}},
		},
	}
}
