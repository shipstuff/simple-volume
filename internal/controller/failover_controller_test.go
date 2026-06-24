package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func TestFailoverControllerPromotesStorageBindingAndDeletesStalePod(t *testing.T) {
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
					AnnotationReplicationEnabled:   "true",
					AnnotationFailoverEnabled:      "true",
					AnnotationFailoverGracePeriod:  "1s",
					AnnotationFailoverMaxStaleness: "30s",
					AnnotationIncludePaths:         "writes.log",
					AnnotationFullSyncOnStart:      "true",
					AnnotationFullSyncSchedule:     "0 4 * * *",
					AnnotationDebounce:             "2s",
					AnnotationExcludePaths:         "downloads/**",
					AnnotationSelectedNode:         "kapolei-pacific-1",
				},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: &storageClass,
				VolumeName:       "pvc-123",
			},
		},
		workloadPod("writer-old", "default", "kapolei-pacific-1", "data"),
		readyNode("sf-west-1", true),
		readyNode("fresno-west-1", true),
		readyNode("kapolei-pacific-1", false),
		agentPod("agent-sf", "sf-west-1", "10.0.0.11"),
		agentPod("agent-fresno", "fresno-west-1", "10.0.0.12"),
	)
	controller := NewFailoverController(client, ReplicationControllerConfig{
		Namespace:          "simple-volume-system",
		StorageClassName:   storageClass,
		AgentLabelSelector: "app.kubernetes.io/component=node",
	})
	now := time.Date(2026, 6, 15, 5, 0, 0, 0, time.UTC)
	controller.RecordReplicaFreshness("default", "pvc-123", "sf-west-1", now.Add(-5*time.Second), true)
	controller.RecordReplicaFreshness("default", "pvc-123", "fresno-west-1", now.Add(-2*time.Second), true)
	if err := controller.Reconcile(context.Background(), now); err != nil {
		t.Fatalf("first Reconcile returned error: %v", err)
	}
	if err := controller.Reconcile(context.Background(), now.Add(2*time.Second)); err != nil {
		t.Fatalf("second Reconcile returned error: %v", err)
	}
	pv, err := client.CoreV1().PersistentVolumes().Get(context.Background(), "pvc-123", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pv: %v", err)
	}
	if pv.Spec.NodeAffinity != nil {
		t.Fatalf("pv nodeAffinity = %#v, want unchanged nil affinity", pv.Spec.NodeAffinity)
	}
	if got := pv.Annotations[AnnotationActiveNode]; got != "fresno-west-1" {
		t.Fatalf("pv active node annotation = %q, want fresno-west-1", got)
	}
	if got := pv.Annotations[AnnotationPreviousActiveNode]; got != "kapolei-pacific-1" {
		t.Fatalf("pv previous active node annotation = %q, want kapolei-pacific-1", got)
	}
	pvc, err := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "data", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pvc: %v", err)
	}
	if got := pvc.Annotations[AnnotationActiveNode]; got != "fresno-west-1" {
		t.Fatalf("pvc active node annotation = %q, want fresno-west-1", got)
	}
	if _, ok := pvc.Annotations[AnnotationSelectedNode]; ok {
		t.Fatalf("selected-node annotation should be removed: %#v", pvc.Annotations)
	}
	targetNode, err := client.CoreV1().Nodes().Get(context.Background(), "fresno-west-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get target node: %v", err)
	}
	if got := targetNode.Labels[RoleLabel("default", "data")]; got != "active" {
		t.Fatalf("target node active label = %q, want active", got)
	}
	oldNode, err := client.CoreV1().Nodes().Get(context.Background(), "kapolei-pacific-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get old node: %v", err)
	}
	if _, ok := oldNode.Labels[RoleLabel("default", "data")]; ok {
		t.Fatalf("old node still has active label: %#v", oldNode.Labels)
	}
	_, err = client.CoreV1().Pods("default").Get(context.Background(), "writer-old", metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("old pod error = %v, want not found", err)
	}
}

func TestFailoverControllerTwoLegFailoverDrill(t *testing.T) {
	storageClass := "simple-volume"
	client := fake.NewSimpleClientset(
		&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "pvc-123",
				Annotations: map[string]string{AnnotationActiveNode: "fresno-west-1"},
			},
		},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "data",
				Namespace: "default",
				Annotations: map[string]string{
					AnnotationReplicationEnabled:   "true",
					AnnotationFailoverEnabled:      "true",
					AnnotationFailoverGracePeriod:  "1s",
					AnnotationFailoverMaxStaleness: "30s",
					AnnotationActiveNode:           "fresno-west-1",
				},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: &storageClass,
				VolumeName:       "pvc-123",
			},
		},
		workloadPod("writer-fresno", "default", "fresno-west-1", "data"),
		readyNode("fresno-west-1", true),
		readyNode("sf-west-1", true),
		agentPod("agent-fresno", "fresno-west-1", "10.0.0.12"),
		agentPod("agent-sf", "sf-west-1", "10.0.0.11"),
	)
	controller := NewFailoverController(client, ReplicationControllerConfig{
		Namespace:          "simple-volume-system",
		StorageClassName:   storageClass,
		AgentLabelSelector: "app.kubernetes.io/component=node",
	})
	now := time.Date(2026, 6, 24, 2, 0, 0, 0, time.UTC)
	controller.RecordReplicaFreshness("default", "pvc-123", "sf-west-1", now.Add(-5*time.Second), true)

	setNodeUnschedulable(t, client, "fresno-west-1", true)
	if err := controller.Reconcile(context.Background(), now); err != nil {
		t.Fatalf("first leg initial Reconcile returned error: %v", err)
	}
	if err := controller.Reconcile(context.Background(), now.Add(2*time.Second)); err != nil {
		t.Fatalf("first leg promotion Reconcile returned error: %v", err)
	}
	assertPVCActiveNode(t, client, "default", "data", "sf-west-1")
	assertPVActiveNode(t, client, "pvc-123", "sf-west-1")
	assertNodeRole(t, client, "default", "data", "sf-west-1")
	assertPodDeleted(t, client, "default", "writer-fresno")

	setNodeUnschedulable(t, client, "fresno-west-1", false)
	if _, err := client.CoreV1().Pods("default").Create(context.Background(), workloadPod("writer-sf", "default", "sf-west-1", "data"), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create sf replacement pod: %v", err)
	}
	controller.RecordReplicaFreshness("default", "pvc-123", "fresno-west-1", now.Add(3*time.Second), true)
	controller.MarkRestored("default", "pvc-123", "fresno-west-1")
	setNodeUnschedulable(t, client, "sf-west-1", true)
	if err := controller.Reconcile(context.Background(), now.Add(4*time.Second)); err != nil {
		t.Fatalf("second leg initial Reconcile returned error: %v", err)
	}
	if err := controller.Reconcile(context.Background(), now.Add(6*time.Second)); err != nil {
		t.Fatalf("second leg promotion Reconcile returned error: %v", err)
	}
	assertPVCActiveNode(t, client, "default", "data", "fresno-west-1")
	assertPVActiveNode(t, client, "pvc-123", "fresno-west-1")
	assertNodeRole(t, client, "default", "data", "fresno-west-1")
	assertPodDeleted(t, client, "default", "writer-sf")

	for _, action := range client.Actions() {
		if action.GetResource().Resource == "nodes" && action.GetVerb() == "update" {
			t.Fatalf("failover drill should patch node labels, not update nodes: %#v", action)
		}
	}
}

func TestFailoverControllerLabelsInitialActiveNodeForUnboundPVC(t *testing.T) {
	storageClass := "simple-volume"
	client := fake.NewSimpleClientset(
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "data",
				Namespace: "default",
				Annotations: map[string]string{
					AnnotationReplicationEnabled:   "true",
					AnnotationFailoverEnabled:      "true",
					AnnotationFailoverNodePriority: "fresno-west-1,sf-west-1",
				},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: &storageClass,
			},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "writer", Namespace: "default"},
			Spec: corev1.PodSpec{
				NodeSelector: map[string]string{RoleLabel("default", "data"): "active"},
				Volumes: []corev1.Volume{{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"},
					},
				}},
			},
			Status: corev1.PodStatus{Phase: corev1.PodPending},
		},
		readyNode("fresno-west-1", true),
		readyNode("sf-west-1", true),
		agentPod("agent-fresno", "fresno-west-1", "10.0.0.12"),
		agentPod("agent-sf", "sf-west-1", "10.0.0.11"),
	)
	controller := NewFailoverController(client, ReplicationControllerConfig{
		Namespace:          "simple-volume-system",
		StorageClassName:   storageClass,
		AgentLabelSelector: "app.kubernetes.io/component=node",
	})

	if err := controller.Reconcile(context.Background(), time.Date(2026, 6, 18, 6, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("Reconcile returned error: %v", err)
	}

	pvc, err := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "data", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pvc: %v", err)
	}
	if got := pvc.Annotations[AnnotationActiveNode]; got != "fresno-west-1" {
		t.Fatalf("pvc active node annotation = %q, want fresno-west-1", got)
	}
	node, err := client.CoreV1().Nodes().Get(context.Background(), "fresno-west-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get node: %v", err)
	}
	if got := node.Labels[RoleLabel("default", "data")]; got != "active" {
		t.Fatalf("node active label = %q, want active", got)
	}
}

func TestFailoverControllerAdoptsSelectedNodeForInitialActiveNode(t *testing.T) {
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
					AnnotationSelectedNode:       "fresno-west-1",
				},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: &storageClass,
				VolumeName:       "pvc-123",
			},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "writer", Namespace: "default"},
			Spec: corev1.PodSpec{
				Volumes: []corev1.Volume{{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"},
					},
				}},
			},
			Status: corev1.PodStatus{Phase: corev1.PodPending},
		},
		readyNode("fresno-west-1", true),
		agentPod("agent-fresno", "fresno-west-1", "10.0.0.12"),
	)
	controller := NewFailoverController(client, ReplicationControllerConfig{
		Namespace:          "simple-volume-system",
		StorageClassName:   storageClass,
		AgentLabelSelector: "app.kubernetes.io/component=node",
	})

	if err := controller.Reconcile(context.Background(), time.Date(2026, 6, 18, 6, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("Reconcile returned error: %v", err)
	}

	pvc, err := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "data", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pvc: %v", err)
	}
	if got := pvc.Annotations[AnnotationActiveNode]; got != "fresno-west-1" {
		t.Fatalf("pvc active node annotation = %q, want fresno-west-1", got)
	}
	if _, ok := pvc.Annotations[AnnotationSelectedNode]; ok {
		t.Fatalf("selected-node annotation should be removed: %#v", pvc.Annotations)
	}
	pv, err := client.CoreV1().PersistentVolumes().Get(context.Background(), "pvc-123", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pv: %v", err)
	}
	if got := pv.Annotations[AnnotationActiveNode]; got != "fresno-west-1" {
		t.Fatalf("pv active node annotation = %q, want fresno-west-1", got)
	}
	node, err := client.CoreV1().Nodes().Get(context.Background(), "fresno-west-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get node: %v", err)
	}
	if got := node.Labels[RoleLabel("default", "data")]; got != "active" {
		t.Fatalf("node active label = %q, want active", got)
	}
}

func TestFailoverControllerRecordsHealthyActiveNode(t *testing.T) {
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
		readyNode("fresno-west-1", true),
		agentPod("agent-sf", "sf-west-1", "10.0.0.11"),
		agentPod("agent-fresno", "fresno-west-1", "10.0.0.12"),
	)
	controller := NewFailoverController(client, ReplicationControllerConfig{
		Namespace:          "simple-volume-system",
		StorageClassName:   storageClass,
		AgentLabelSelector: "app.kubernetes.io/component=node",
	})
	now := time.Date(2026, 6, 15, 5, 0, 0, 0, time.UTC)
	controller.RecordReplicaFreshness("default", "pvc-123", "fresno-west-1", now.Add(-5*time.Second), true)
	if err := controller.Reconcile(context.Background(), now); err != nil {
		t.Fatalf("Reconcile returned error: %v", err)
	}

	pv, err := client.CoreV1().PersistentVolumes().Get(context.Background(), "pvc-123", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pv: %v", err)
	}
	if got := pv.Annotations[AnnotationActiveNode]; got != "sf-west-1" {
		t.Fatalf("pv active node annotation = %q, want sf-west-1", got)
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
	activeNode, err := client.CoreV1().Nodes().Get(context.Background(), "sf-west-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get active node: %v", err)
	}
	if got := activeNode.Labels[RoleLabel("default", "data")]; got != "active" {
		t.Fatalf("active node label = %q, want active", got)
	}
	if _, err := client.CoreV1().Pods("default").Get(context.Background(), "writer", metav1.GetOptions{}); err != nil {
		t.Fatalf("healthy pod should remain: %v", err)
	}
}

func TestFailoverControllerDoesNotPromoteScaledToZeroWorkload(t *testing.T) {
	storageClass := "simple-volume"
	roleLabel := RoleLabel("default", "data")
	client := fake.NewSimpleClientset(
		&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "pvc-123",
				Annotations: map[string]string{AnnotationActiveNode: "fresno-west-1"},
			},
		},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "data",
				Namespace: "default",
				Annotations: map[string]string{
					AnnotationReplicationEnabled:   "true",
					AnnotationFailoverEnabled:      "true",
					AnnotationFailoverGracePeriod:  "1s",
					AnnotationFailoverMaxStaleness: "30s",
					AnnotationActiveNode:           "fresno-west-1",
				},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: &storageClass,
				VolumeName:       "pvc-123",
			},
		},
		statefulSetUsingClaim("writer", "default", "data", 0),
		readyNode("sf-west-1", true),
		readyNode("fresno-west-1", true),
		agentPod("agent-sf", "sf-west-1", "10.0.0.11"),
		agentPod("agent-fresno", "fresno-west-1", "10.0.0.12"),
	)
	controller := NewFailoverController(client, ReplicationControllerConfig{
		Namespace:          "simple-volume-system",
		StorageClassName:   storageClass,
		AgentLabelSelector: "app.kubernetes.io/component=node",
	})
	now := time.Date(2026, 6, 15, 5, 0, 0, 0, time.UTC)
	controller.RecordReplicaFreshness("default", "pvc-123", "sf-west-1", now.Add(-5*time.Second), true)
	if err := controller.Reconcile(context.Background(), now); err != nil {
		t.Fatalf("first Reconcile returned error: %v", err)
	}
	if err := controller.Reconcile(context.Background(), now.Add(2*time.Second)); err != nil {
		t.Fatalf("second Reconcile returned error: %v", err)
	}
	pvc, err := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "data", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pvc: %v", err)
	}
	if got := pvc.Annotations[AnnotationActiveNode]; got != "fresno-west-1" {
		t.Fatalf("pvc active node annotation = %q, want fresno-west-1", got)
	}
	fresno, err := client.CoreV1().Nodes().Get(context.Background(), "fresno-west-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get fresno node: %v", err)
	}
	if got := fresno.Labels[roleLabel]; got != "active" {
		t.Fatalf("fresno active label = %q, want active", got)
	}
	sf, err := client.CoreV1().Nodes().Get(context.Background(), "sf-west-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get sf node: %v", err)
	}
	if _, ok := sf.Labels[roleLabel]; ok {
		t.Fatalf("scaled-to-zero workload promoted sf-west-1: %#v", sf.Labels)
	}
}

func TestFailoverControllerPromotesDesiredWorkloadWithMissingPod(t *testing.T) {
	storageClass := "simple-volume"
	client := fake.NewSimpleClientset(
		&corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "pvc-123",
				Annotations: map[string]string{AnnotationActiveNode: "kapolei-pacific-1"},
			},
		},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "data",
				Namespace: "default",
				Annotations: map[string]string{
					AnnotationReplicationEnabled:   "true",
					AnnotationFailoverEnabled:      "true",
					AnnotationFailoverGracePeriod:  "1s",
					AnnotationFailoverMaxStaleness: "30s",
					AnnotationActiveNode:           "kapolei-pacific-1",
				},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: &storageClass,
				VolumeName:       "pvc-123",
			},
		},
		statefulSetUsingClaim("writer", "default", "data", 1),
		readyNode("sf-west-1", true),
		readyNode("kapolei-pacific-1", false),
		agentPod("agent-sf", "sf-west-1", "10.0.0.11"),
	)
	controller := NewFailoverController(client, ReplicationControllerConfig{
		Namespace:          "simple-volume-system",
		StorageClassName:   storageClass,
		AgentLabelSelector: "app.kubernetes.io/component=node",
	})
	now := time.Date(2026, 6, 15, 5, 0, 0, 0, time.UTC)
	controller.RecordReplicaFreshness("default", "pvc-123", "sf-west-1", now.Add(-5*time.Second), true)
	if err := controller.Reconcile(context.Background(), now); err != nil {
		t.Fatalf("first Reconcile returned error: %v", err)
	}
	if err := controller.Reconcile(context.Background(), now.Add(2*time.Second)); err != nil {
		t.Fatalf("second Reconcile returned error: %v", err)
	}
	pvc, err := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "data", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pvc: %v", err)
	}
	if got := pvc.Annotations[AnnotationActiveNode]; got != "sf-west-1" {
		t.Fatalf("pvc active node annotation = %q, want sf-west-1", got)
	}
}

func TestFailoverControllerBlocksStaleReplicas(t *testing.T) {
	storageClass := "simple-volume"
	client := fake.NewSimpleClientset(
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "data",
				Namespace: "default",
				Annotations: map[string]string{
					AnnotationReplicationEnabled:   "true",
					AnnotationFailoverEnabled:      "true",
					AnnotationFailoverGracePeriod:  "1s",
					AnnotationFailoverMaxStaleness: "30s",
				},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: &storageClass,
				VolumeName:       "pvc-123",
			},
		},
		workloadPod("writer-old", "default", "kapolei-pacific-1", "data"),
		readyNode("sf-west-1", true),
		readyNode("kapolei-pacific-1", false),
		agentPod("agent-sf", "sf-west-1", "10.0.0.11"),
	)
	controller := NewFailoverController(client, ReplicationControllerConfig{
		Namespace:          "simple-volume-system",
		StorageClassName:   storageClass,
		AgentLabelSelector: "app.kubernetes.io/component=node",
	})
	now := time.Date(2026, 6, 15, 5, 0, 0, 0, time.UTC)
	controller.RecordReplicaFreshness("default", "pvc-123", "sf-west-1", now.Add(-2*time.Minute), true)
	if err := controller.Reconcile(context.Background(), now); err != nil {
		t.Fatalf("first Reconcile returned error: %v", err)
	}
	if err := controller.Reconcile(context.Background(), now.Add(2*time.Second)); err != nil {
		t.Fatalf("second Reconcile returned error: %v", err)
	}
	pvc, err := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "data", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pvc: %v", err)
	}
	if got := pvc.Annotations[AnnotationActiveNode]; got != "" {
		t.Fatalf("pvc active node annotation = %q, want no promotion", got)
	}
	if _, err := client.CoreV1().Pods("default").Get(context.Background(), "writer-old", metav1.GetOptions{}); err != nil {
		t.Fatalf("stale pod should remain when replicas are stale: %v", err)
	}
}

func TestFailoverControllerDoesNotDeleteUnboundPendingPod(t *testing.T) {
	storageClass := "simple-volume"
	client := fake.NewSimpleClientset(
		&corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pvc-123"}},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "data",
				Namespace: "default",
				Annotations: map[string]string{
					AnnotationReplicationEnabled:   "true",
					AnnotationFailoverEnabled:      "true",
					AnnotationFailoverGracePeriod:  "1s",
					AnnotationFailoverMaxStaleness: "30s",
					AnnotationActiveNode:           "kapolei-pacific-1",
				},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: &storageClass,
				VolumeName:       "pvc-123",
			},
		},
		func() *corev1.Pod {
			pod := workloadPod("writer-pending", "default", "", "data")
			pod.Status.Phase = corev1.PodPending
			return pod
		}(),
		readyNode("sf-west-1", true),
		readyNode("kapolei-pacific-1", false),
		agentPod("agent-sf", "sf-west-1", "10.0.0.11"),
	)
	controller := NewFailoverController(client, ReplicationControllerConfig{
		Namespace:          "simple-volume-system",
		StorageClassName:   storageClass,
		AgentLabelSelector: "app.kubernetes.io/component=node",
	})
	now := time.Date(2026, 6, 15, 5, 0, 0, 0, time.UTC)
	controller.RecordReplicaFreshness("default", "pvc-123", "sf-west-1", now.Add(-5*time.Second), true)
	if err := controller.Reconcile(context.Background(), now); err != nil {
		t.Fatalf("first Reconcile returned error: %v", err)
	}
	if err := controller.Reconcile(context.Background(), now.Add(2*time.Second)); err != nil {
		t.Fatalf("second Reconcile returned error: %v", err)
	}
	pvc, err := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "data", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pvc: %v", err)
	}
	if got := pvc.Annotations[AnnotationActiveNode]; got != "sf-west-1" {
		t.Fatalf("pvc active node annotation = %q, want sf-west-1", got)
	}
	if _, err := client.CoreV1().Pods("default").Get(context.Background(), "writer-pending", metav1.GetOptions{}); err != nil {
		t.Fatalf("pending pod should remain for scheduler placement: %v", err)
	}
}

func TestFailoverControllerHonorsNodePriorityOverFreshness(t *testing.T) {
	storageClass := "simple-volume"
	client := fake.NewSimpleClientset(
		&corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pvc-123"}},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "data",
				Namespace: "default",
				Annotations: map[string]string{
					AnnotationReplicationEnabled:   "true",
					AnnotationFailoverEnabled:      "true",
					AnnotationFailoverGracePeriod:  "1s",
					AnnotationFailoverMaxStaleness: "30s",
					AnnotationFailoverNodePriority: "sf-west-1,fresno-west-1",
				},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: &storageClass,
				VolumeName:       "pvc-123",
			},
		},
		workloadPod("writer-old", "default", "kapolei-pacific-1", "data"),
		readyNode("sf-west-1", true),
		readyNode("fresno-west-1", true),
		readyNode("kapolei-pacific-1", false),
		agentPod("agent-sf", "sf-west-1", "10.0.0.11"),
		agentPod("agent-fresno", "fresno-west-1", "10.0.0.12"),
	)
	controller := NewFailoverController(client, ReplicationControllerConfig{
		Namespace:          "simple-volume-system",
		StorageClassName:   storageClass,
		AgentLabelSelector: "app.kubernetes.io/component=node",
	})
	now := time.Date(2026, 6, 15, 5, 0, 0, 0, time.UTC)
	controller.RecordReplicaFreshness("default", "pvc-123", "sf-west-1", now.Add(-5*time.Second), true)
	controller.RecordReplicaFreshness("default", "pvc-123", "fresno-west-1", now.Add(-2*time.Second), true)
	if err := controller.Reconcile(context.Background(), now); err != nil {
		t.Fatalf("first Reconcile returned error: %v", err)
	}
	if err := controller.Reconcile(context.Background(), now.Add(2*time.Second)); err != nil {
		t.Fatalf("second Reconcile returned error: %v", err)
	}
	pvc, err := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "data", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pvc: %v", err)
	}
	if got := pvc.Annotations[AnnotationActiveNode]; got != "sf-west-1" {
		t.Fatalf("pvc active node annotation = %q, want sf-west-1", got)
	}
}

func TestFailoverControllerSkipsPriorityNodeWithoutResourceFit(t *testing.T) {
	storageClass := "simple-volume"
	client := fake.NewSimpleClientset(
		&corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pvc-123"}},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "data",
				Namespace: "default",
				Annotations: map[string]string{
					AnnotationReplicationEnabled:   "true",
					AnnotationFailoverEnabled:      "true",
					AnnotationFailoverGracePeriod:  "1s",
					AnnotationFailoverMaxStaleness: "30s",
					AnnotationFailoverNodePriority: "sf-west-1,fresno-west-1",
				},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: &storageClass,
				VolumeName:       "pvc-123",
			},
		},
		workloadPodWithRequests("writer-old", "default", "kapolei-pacific-1", "data", "750m", "2Gi"),
		readyNodeWithAllocatable("sf-west-1", true, "1000m", "1Gi"),
		readyNodeWithAllocatable("fresno-west-1", true, "4000m", "8Gi"),
		readyNode("kapolei-pacific-1", false),
		agentPod("agent-sf", "sf-west-1", "10.0.0.11"),
		agentPod("agent-fresno", "fresno-west-1", "10.0.0.12"),
	)
	controller := NewFailoverController(client, ReplicationControllerConfig{
		Namespace:          "simple-volume-system",
		StorageClassName:   storageClass,
		AgentLabelSelector: "app.kubernetes.io/component=node",
	})
	now := time.Date(2026, 6, 15, 5, 0, 0, 0, time.UTC)
	controller.RecordReplicaFreshness("default", "pvc-123", "sf-west-1", now.Add(-5*time.Second), true)
	controller.RecordReplicaFreshness("default", "pvc-123", "fresno-west-1", now.Add(-10*time.Second), true)
	if err := controller.Reconcile(context.Background(), now); err != nil {
		t.Fatalf("first Reconcile returned error: %v", err)
	}
	if err := controller.Reconcile(context.Background(), now.Add(2*time.Second)); err != nil {
		t.Fatalf("second Reconcile returned error: %v", err)
	}
	pvc, err := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "data", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pvc: %v", err)
	}
	if got := pvc.Annotations[AnnotationActiveNode]; got != "fresno-west-1" {
		t.Fatalf("pvc active node annotation = %q, want fresno-west-1", got)
	}
}

func TestFailoverControllerMaintainsFreshCandidateLabels(t *testing.T) {
	storageClass := "simple-volume"
	candidateLabel := CandidateLabel("default", "data")
	client := fake.NewSimpleClientset(
		&corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pvc-123"}},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "data",
				Namespace: "default",
				Annotations: map[string]string{
					AnnotationReplicationEnabled:   "true",
					AnnotationFailoverEnabled:      "true",
					AnnotationFailoverMaxStaleness: "30s",
				},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: &storageClass,
				VolumeName:       "pvc-123",
			},
		},
		workloadPod("writer", "default", "sf-west-1", "data"),
		readyNode("sf-west-1", true),
		readyNode("fresno-west-1", true),
		nodeWithLabels("kapolei-pacific-1", false, map[string]string{candidateLabel: "true"}),
		agentPod("agent-sf", "sf-west-1", "10.0.0.11"),
		agentPod("agent-fresno", "fresno-west-1", "10.0.0.12"),
		agentPod("agent-kapolei", "kapolei-pacific-1", "10.0.0.13"),
	)
	controller := NewFailoverController(client, ReplicationControllerConfig{
		Namespace:          "simple-volume-system",
		StorageClassName:   storageClass,
		AgentLabelSelector: "app.kubernetes.io/component=node",
	})
	now := time.Date(2026, 6, 15, 5, 0, 0, 0, time.UTC)
	controller.RecordReplicaFreshness("default", "pvc-123", "fresno-west-1", now.Add(-5*time.Second), true)
	controller.RecordReplicaFreshness("default", "pvc-123", "kapolei-pacific-1", now.Add(-5*time.Second), true)
	if err := controller.Reconcile(context.Background(), now); err != nil {
		t.Fatalf("Reconcile returned error: %v", err)
	}
	sawNodePatch := false
	for _, action := range client.Actions() {
		if action.GetResource().Resource != "nodes" {
			continue
		}
		if action.GetVerb() == "update" {
			t.Fatalf("node labels should be patched, not updated: %#v", action)
		}
		if action.GetVerb() == "patch" {
			sawNodePatch = true
		}
	}
	if !sawNodePatch {
		t.Fatalf("expected at least one node label patch, actions=%#v", client.Actions())
	}
	active, err := client.CoreV1().Nodes().Get(context.Background(), "sf-west-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get active node: %v", err)
	}
	if got := active.Labels[RoleLabel("default", "data")]; got != "active" {
		t.Fatalf("active node role label = %q, want active", got)
	}
	candidate, err := client.CoreV1().Nodes().Get(context.Background(), "fresno-west-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get candidate node: %v", err)
	}
	if got := candidate.Labels[candidateLabel]; got != "true" {
		t.Fatalf("candidate label = %q, want true", got)
	}
	offline, err := client.CoreV1().Nodes().Get(context.Background(), "kapolei-pacific-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get offline node: %v", err)
	}
	if _, ok := offline.Labels[candidateLabel]; ok {
		t.Fatalf("offline node still has candidate label: %#v", offline.Labels)
	}
}

func TestFailoverControllerExcludesDemotedNodeFromCandidates(t *testing.T) {
	storageClass := "simple-volume"
	candidateLabel := CandidateLabel("default", "data")
	client := fake.NewSimpleClientset(
		&corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pvc-123"}},
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "data",
				Namespace: "default",
				Annotations: map[string]string{
					AnnotationReplicationEnabled:   "true",
					AnnotationFailoverEnabled:      "true",
					AnnotationFailoverGracePeriod:  "1s",
					AnnotationFailoverMaxStaleness: "30s",
					AnnotationFailoverNodePriority: "kapolei-pacific-1,fresno-west-1",
				},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: &storageClass,
				VolumeName:       "pvc-123",
			},
		},
		workloadPod("writer-old", "default", "kapolei-pacific-1", "data"),
		readyNode("sf-west-1", true),
		readyNode("fresno-west-1", true),
		readyNode("kapolei-pacific-1", false),
		agentPod("agent-sf", "sf-west-1", "10.0.0.11"),
		agentPod("agent-fresno", "fresno-west-1", "10.0.0.12"),
		agentPod("agent-kapolei", "kapolei-pacific-1", "10.0.0.13"),
	)
	controller := NewFailoverController(client, ReplicationControllerConfig{
		Namespace:          "simple-volume-system",
		StorageClassName:   storageClass,
		AgentLabelSelector: "app.kubernetes.io/component=node",
	})
	now := time.Date(2026, 6, 15, 5, 0, 0, 0, time.UTC)
	controller.RecordReplicaFreshness("default", "pvc-123", "fresno-west-1", now.Add(-5*time.Second), true)
	controller.RecordReplicaFreshness("default", "pvc-123", "kapolei-pacific-1", now.Add(-5*time.Second), true)
	if err := controller.Reconcile(context.Background(), now); err != nil {
		t.Fatalf("first Reconcile returned error: %v", err)
	}
	if err := controller.Reconcile(context.Background(), now.Add(2*time.Second)); err != nil {
		t.Fatalf("second Reconcile returned error: %v", err)
	}
	promoted, err := client.CoreV1().PersistentVolumeClaims("default").Get(context.Background(), "data", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pvc: %v", err)
	}
	if got := promoted.Annotations[AnnotationActiveNode]; got != "fresno-west-1" {
		t.Fatalf("pvc active node annotation = %q, want fresno-west-1", got)
	}
	kapolei, err := client.CoreV1().Nodes().Get(context.Background(), "kapolei-pacific-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get kapolei node: %v", err)
	}
	kapolei.Status.Conditions[0].Status = corev1.ConditionTrue
	if _, err := client.CoreV1().Nodes().Update(context.Background(), kapolei, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update kapolei node: %v", err)
	}
	controller.RecordReplicaFreshness("default", "pvc-123", "kapolei-pacific-1", now.Add(3*time.Second), true)
	if err := controller.Reconcile(context.Background(), now.Add(4*time.Second)); err != nil {
		t.Fatalf("third Reconcile returned error: %v", err)
	}
	kapolei, err = client.CoreV1().Nodes().Get(context.Background(), "kapolei-pacific-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get kapolei node after reconcile: %v", err)
	}
	if _, ok := kapolei.Labels[candidateLabel]; ok {
		t.Fatalf("demoted node should not have candidate label before restore: %#v", kapolei.Labels)
	}
}

func TestReadyNodeSetExcludesUnschedulableNodes(t *testing.T) {
	node := readyNode("kapolei-pacific-1", true)
	node.Spec.Unschedulable = true
	nodes := readyNodeSet([]corev1.Node{*node, *readyNode("sf-west-1", true)})
	if nodes["kapolei-pacific-1"] {
		t.Fatal("unschedulable ready node should be excluded")
	}
	if !nodes["sf-west-1"] {
		t.Fatal("schedulable ready node should be included")
	}
}

func setNodeUnschedulable(t *testing.T, client *fake.Clientset, name string, unschedulable bool) {
	t.Helper()
	patch := []byte(fmt.Sprintf(`{"spec":{"unschedulable":%t}}`, unschedulable))
	if _, err := client.CoreV1().Nodes().Patch(context.Background(), name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
		t.Fatalf("patch node %s unschedulable=%t: %v", name, unschedulable, err)
	}
}

func assertPVCActiveNode(t *testing.T, client *fake.Clientset, namespace, claim, want string) {
	t.Helper()
	pvc, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(context.Background(), claim, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pvc %s/%s: %v", namespace, claim, err)
	}
	if got := pvc.Annotations[AnnotationActiveNode]; got != want {
		t.Fatalf("pvc %s/%s active node = %q, want %q", namespace, claim, got, want)
	}
}

func assertPVActiveNode(t *testing.T, client *fake.Clientset, pvName, want string) {
	t.Helper()
	pv, err := client.CoreV1().PersistentVolumes().Get(context.Background(), pvName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pv %s: %v", pvName, err)
	}
	if got := pv.Annotations[AnnotationActiveNode]; got != want {
		t.Fatalf("pv %s active node = %q, want %q", pvName, got, want)
	}
}

func assertNodeRole(t *testing.T, client *fake.Clientset, namespace, claim, activeNode string) {
	t.Helper()
	roleLabel := RoleLabel(namespace, claim)
	nodes, err := client.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list nodes: %v", err)
	}
	for _, node := range nodes.Items {
		got, has := node.Labels[roleLabel]
		if node.Name == activeNode {
			if got != "active" {
				t.Fatalf("node %s role label = %q, want active", node.Name, got)
			}
			continue
		}
		if has {
			t.Fatalf("inactive node %s still has role label %s=%q", node.Name, roleLabel, got)
		}
	}
}

func assertPodDeleted(t *testing.T, client *fake.Clientset, namespace, podName string) {
	t.Helper()
	_, err := client.CoreV1().Pods(namespace).Get(context.Background(), podName, metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("pod %s/%s error = %v, want not found", namespace, podName, err)
	}
}

func readyNode(name string, ready bool) *corev1.Node {
	return nodeWithLabels(name, ready, nil)
}

func nodeWithLabels(name string, ready bool, labels map[string]string) *corev1.Node {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{
				Type:   corev1.NodeReady,
				Status: status,
			}},
		},
	}
}

func readyNodeWithAllocatable(name string, ready bool, cpu, memory string) *corev1.Node {
	node := readyNode(name, ready)
	node.Status.Allocatable = corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse(cpu),
		corev1.ResourceMemory: resource.MustParse(memory),
	}
	return node
}

func workloadPodWithRequests(name, namespace, node, claim, cpu, memory string) *corev1.Pod {
	pod := workloadPod(name, namespace, node, claim)
	pod.Spec.Containers = []corev1.Container{{
		Name:  "writer",
		Image: "busybox",
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(cpu),
				corev1.ResourceMemory: resource.MustParse(memory),
			},
		},
	}}
	return pod
}

func statefulSetUsingClaim(name, namespace, claim string, replicas int32) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "writer", Image: "busybox"}},
					Volumes: []corev1.Volume{{
						Name: "data",
						VolumeSource: corev1.VolumeSource{
							PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim},
						},
					}},
				},
			},
		},
	}
}
