package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/shipstuff/simple-volume/internal/agent"
)

func TestNewReplicationControllerDefaultsHTTPTimeoutToFullSyncWindow(t *testing.T) {
	controller := NewReplicationController(fake.NewSimpleClientset(), ReplicationControllerConfig{})
	if controller.cfg.HTTPTimeout != time.Hour {
		t.Fatalf("HTTPTimeout = %v, want %v", controller.cfg.HTTPTimeout, time.Hour)
	}
	if controller.http.Timeout != time.Hour {
		t.Fatalf("http client timeout = %v, want %v", controller.http.Timeout, time.Hour)
	}
}

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
					AnnotationPruneExcluded:      "true",
					AnnotationRequiredPaths:      "saves/world",
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
	if got.Targets[0].Ref.Token != "secret" || got.Targets[1].Ref.Token != "secret" {
		t.Fatalf("target tokens = %#v", got.Targets)
	}
	if got.IncludePaths[0] != "writes.log" || got.IncludePaths[1] != "saves/**" {
		t.Fatalf("include paths = %#v", got.IncludePaths)
	}
	if len(got.RequiredPaths) != 1 || got.RequiredPaths[0] != "saves/world" {
		t.Fatalf("required paths = %#v", got.RequiredPaths)
	}
	if !got.PruneExcluded {
		t.Fatalf("prune excluded = false")
	}
	if !got.FullSync || got.FullSchedule != "0 4 * * *" || got.Debounce != "2s" {
		t.Fatalf("sync policy = %#v", got)
	}
	if got.ConsistencyMode != agent.ConsistencyModeShadow || got.ConfirmedReplicas != 1 {
		t.Fatalf("consistency policy = %#v", got)
	}
}

func TestDesiredReplicationsFallsBackToPVCActiveNode(t *testing.T) {
	storageClass := "simple-volume"
	client := fake.NewSimpleClientset(
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "data",
				Namespace: "default",
				Annotations: map[string]string{
					AnnotationReplicationEnabled: "true",
					AnnotationActiveNode:         "sf-west-1",
				},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: &storageClass,
				VolumeName:       "pvc-123",
			},
		},
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
	if got := desired[0].ActiveNode; got != "sf-west-1" {
		t.Fatalf("ActiveNode = %q, want sf-west-1", got)
	}
	if got := desired[0].SourceURL; got != "http://10.0.0.11:8081" {
		t.Fatalf("SourceURL = %q, want sf source", got)
	}
	if len(desired[0].Targets) != 1 || desired[0].Targets[0].Node != "fresno-west-1" {
		t.Fatalf("targets = %#v", desired[0].Targets)
	}
}

func TestDesiredReplicationsInfersOwnershipFromActivePodSecurityContext(t *testing.T) {
	storageClass := "simple-volume"
	runAsUser := int64(10000)
	runAsGroup := int64(10000)
	fileMode := "0664"
	writer := workloadPod("writer", "default", "kapolei-pacific-1", "data")
	writer.Spec.SecurityContext = &corev1.PodSecurityContext{
		RunAsUser:  &runAsUser,
		RunAsGroup: &runAsGroup,
	}
	client := fake.NewSimpleClientset(
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "data",
				Namespace: "default",
				Annotations: map[string]string{
					AnnotationReplicationEnabled:  "true",
					AnnotationReplicationFileMode: fileMode,
				},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: &storageClass,
				VolumeName:       "pvc-123",
			},
		},
		writer,
		agentPod("agent-kap", "kapolei-pacific-1", "10.0.0.10"),
		agentPod("agent-sf", "sf-west-1", "10.0.0.11"),
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
	ownership := desired[0].Ownership
	if ownership.UID == nil || *ownership.UID != 10000 {
		t.Fatalf("ownership uid = %#v, want 10000", ownership.UID)
	}
	if ownership.GID == nil || *ownership.GID != 10000 {
		t.Fatalf("ownership gid = %#v, want 10000", ownership.GID)
	}
	if ownership.FileMode == nil || *ownership.FileMode != 0o664 {
		t.Fatalf("ownership file mode = %#v, want 0664", ownership.FileMode)
	}
}

func TestReconcileOneStartsWatchAndRunsStartupFullSyncOnce(t *testing.T) {
	var watchStarts atomic.Int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/replication/shadow/prepare":
			var req agent.ShadowPrepareRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode shadow request: %v", err)
			}
			if req.Volume != "pvc-123" || len(req.RequiredPaths) != 1 || req.RequiredPaths[0] != "saves/world" {
				t.Fatalf("shadow request = %#v", req)
			}
			if !req.PruneExcluded {
				t.Fatalf("shadow prune excluded = false")
			}
			_ = json.NewEncoder(w).Encode(agent.ShadowPrepareResponse{
				Volume:         req.Volume,
				SourceBasePath: ".simple-volume-shadows/default/pvc-123/current/data",
				Generation:     "g1",
				OK:             true,
			})
		case "/replication/watch/start":
			watchStarts.Add(1)
			var req agent.WatchStartRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode watch request: %v", err)
			}
			if req.Volume != "pvc-123" || len(req.Targets) != 2 {
				t.Fatalf("watch request = %#v", req)
			}
			if req.Ownership.UID == nil || *req.Ownership.UID != 10000 {
				t.Fatalf("watch ownership = %#v", req.Ownership)
			}
			if req.ConsistencyMode != agent.ConsistencyModeShadow || req.ConfirmedReplicas == nil || *req.ConfirmedReplicas != 1 {
				t.Fatalf("watch consistency = %#v confirmed=%#v", req.ConsistencyMode, req.ConfirmedReplicas)
			}
			_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
		case "/replication/watch/status":
			_ = json.NewEncoder(w).Encode(map[string]bool{"running": true})
		default:
			t.Fatalf("source path = %s", r.URL.Path)
		}
	}))
	defer source.Close()

	var fullSyncs atomic.Int32
	var stopCalls atomic.Int32
	target := func() *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/replication/full-sync":
			case "/replication/watch/stop":
				stopCalls.Add(1)
				http.Error(w, "watch not found", http.StatusNotFound)
				return
			default:
				t.Fatalf("target path = %s", r.URL.Path)
			}
			var req agent.FullSyncRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode full-sync request: %v", err)
			}
			if req.Ownership.UID == nil || *req.Ownership.UID != 10000 {
				t.Fatalf("full-sync ownership = %#v", req.Ownership)
			}
			if len(req.RequiredPaths) != 1 || req.RequiredPaths[0] != "saves/world" {
				t.Fatalf("full-sync required paths = %#v", req.RequiredPaths)
			}
			if req.SourceBasePath != ".simple-volume-shadows/default/pvc-123/current/data" {
				t.Fatalf("full-sync source base = %q", req.SourceBasePath)
			}
			if !req.PruneExcluded {
				t.Fatalf("full-sync prune excluded = false")
			}
			fullSyncs.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
		}))
	}
	targetA := target()
	defer targetA.Close()
	targetB := target()
	defer targetB.Close()

	controller := NewReplicationController(nil, ReplicationControllerConfig{})
	desired := DesiredReplication{
		Namespace:  "default",
		ClaimName:  "data",
		Volume:     "pvc-123",
		ActiveNode: "kapolei-pacific-1",
		SourceURL:  source.URL,
		Targets: []DesiredTarget{
			{Node: "sf-west-1", Ref: agent.TargetRef{URL: targetA.URL, Token: "secret"}},
			{Node: "fresno-west-1", Ref: agent.TargetRef{URL: targetB.URL, Token: "secret"}},
		},
		IncludePaths:  []string{"writes.log"},
		RequiredPaths: []string{"saves/world"},
		PruneExcluded: true,
		Ownership: agent.OwnershipPolicy{
			UID: int64Ptr(10000),
			GID: int64Ptr(10000),
		},
		Debounce:          "2s",
		FullSync:          true,
		ConsistencyMode:   agent.ConsistencyModeShadow,
		ConfirmedReplicas: 1,
	}

	if err := controller.reconcileOne(context.Background(), desired, "secret", time.Date(2026, 6, 15, 3, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("reconcileOne returned error: %v", err)
	}
	if got := watchStarts.Load(); got != 0 {
		t.Fatalf("watchStarts after initial reconcile = %d, want 0 before full sync completes", got)
	}
	waitFor(t, time.Second, func() bool { return fullSyncs.Load() == 2 })
	waitFor(t, time.Second, func() bool { return watchStarts.Load() == 1 })
	freshness := controller.failover.freshnessForPVC(&corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
		Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: "pvc-123"},
	})
	if len(freshness) != 2 {
		t.Fatalf("freshness = %#v, want 2 replicas", freshness)
	}
	reconcileStart := time.Date(2026, 6, 15, 3, 0, 0, 0, time.UTC)
	for node, replica := range freshness {
		if !replica.Healthy {
			t.Fatalf("replica %s healthy = false", node)
		}
		if !replica.LastSuccessfulSync.After(reconcileStart) {
			t.Fatalf("replica %s freshness = %v, want after %v", node, replica.LastSuccessfulSync, reconcileStart)
		}
	}
	if err := controller.reconcileOne(context.Background(), desired, "secret", time.Date(2026, 6, 15, 3, 1, 0, 0, time.UTC)); err != nil {
		t.Fatalf("second reconcileOne returned error: %v", err)
	}
	if got := watchStarts.Load(); got != 1 {
		t.Fatalf("watchStarts = %d, want 1", got)
	}
	if got := fullSyncs.Load(); got != 2 {
		t.Fatalf("fullSyncs = %d, want 2", got)
	}
	if got := stopCalls.Load(); got != 2 {
		t.Fatalf("stopCalls = %d, want one stale watch stop per target", got)
	}
}

func TestReconcileOneRunsStartupFullSyncAgainAfterActiveNodeChanges(t *testing.T) {
	sourceA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/replication/watch/start":
			_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
		case "/replication/watch/status":
			_ = json.NewEncoder(w).Encode(map[string]bool{"running": true})
		default:
			t.Fatalf("sourceA path = %s", r.URL.Path)
		}
	}))
	defer sourceA.Close()
	sourceB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/replication/watch/start":
			_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
		case "/replication/watch/status":
			_ = json.NewEncoder(w).Encode(map[string]bool{"running": true})
		default:
			t.Fatalf("sourceB path = %s", r.URL.Path)
		}
	}))
	defer sourceB.Close()

	var fullSyncs atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/replication/full-sync":
			fullSyncs.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
		case "/replication/watch/stop":
			http.Error(w, "watch not found", http.StatusNotFound)
		default:
			t.Fatalf("target path = %s", r.URL.Path)
		}
	}))
	defer target.Close()

	controller := NewReplicationController(nil, ReplicationControllerConfig{})
	desired := DesiredReplication{
		Namespace:  "default",
		ClaimName:  "data",
		Volume:     "pvc-123",
		ActiveNode: "sf-west-1",
		SourceURL:  sourceA.URL,
		Targets: []DesiredTarget{
			{Node: "fresno-west-1", Ref: agent.TargetRef{URL: target.URL, Token: "secret"}},
		},
		FullSync: true,
	}

	if err := controller.reconcileOne(context.Background(), desired, "secret", time.Date(2026, 6, 15, 3, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("first reconcileOne returned error: %v", err)
	}
	waitFor(t, time.Second, func() bool { return fullSyncs.Load() == 1 })

	desired.ActiveNode = "fresno-west-1"
	desired.SourceURL = sourceB.URL
	desired.Targets = []DesiredTarget{
		{Node: "sf-west-1", Ref: agent.TargetRef{URL: target.URL, Token: "secret"}},
	}
	if err := controller.reconcileOne(context.Background(), desired, "secret", time.Date(2026, 6, 15, 3, 1, 0, 0, time.UTC)); err != nil {
		t.Fatalf("second reconcileOne returned error: %v", err)
	}
	waitFor(t, time.Second, func() bool { return fullSyncs.Load() == 2 })
}

func TestReconcileRunsFailoverBeforeReplicationTokenLookup(t *testing.T) {
	storageClass := "simple-volume"
	client := fake.NewSimpleClientset(
		&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: "pvc-123"},
		},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "data",
				Namespace: "default",
				Annotations: map[string]string{
					AnnotationReplicationEnabled: "true",
					AnnotationFailoverEnabled:    "true",
					AnnotationSelectedNode:       "sf-west-1",
				},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: &storageClass,
				VolumeName:       "pvc-123",
			},
		},
		workloadPod("writer", "default", "sf-west-1", "data"),
		readyNode("sf-west-1", true),
	)
	controller := NewReplicationController(client, ReplicationControllerConfig{
		Namespace:        "simple-volume-system",
		StorageClassName: storageClass,
	})

	if err := controller.Reconcile(context.Background()); err == nil {
		t.Fatal("Reconcile returned nil error, want missing token error")
	}
	pvc, err := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "data", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pvc: %v", err)
	}
	if got := pvc.Annotations[AnnotationActiveNode]; got != "sf-west-1" {
		t.Fatalf("pvc active node annotation = %q, want sf-west-1", got)
	}
	if _, ok := pvc.Annotations[AnnotationSelectedNode]; ok {
		t.Fatalf("selected-node annotation should be removed: %#v", pvc.Annotations)
	}
	node, err := client.CoreV1().Nodes().Get(context.Background(), "sf-west-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get node: %v", err)
	}
	if got := node.Labels[RoleLabel("default", "data")]; got != "active" {
		t.Fatalf("active node label = %q, want active", got)
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

func waitFor(t *testing.T, timeout time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !ok() {
		t.Fatalf("condition not met within %v", timeout)
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
