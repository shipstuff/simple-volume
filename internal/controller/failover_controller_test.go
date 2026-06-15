package controller

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
					AnnotationReplicationEnabled:        "true",
					AnnotationFailoverEnabled:           "true",
					AnnotationFailoverWorkloadKind:      "Deployment",
					AnnotationFailoverWorkloadName:      "writer",
					AnnotationFailoverGracePeriod:       "1s",
					AnnotationFailoverMaxStaleness:      "30s",
					AnnotationIncludePaths:              "writes.log",
					AnnotationFullSyncOnStart:           "true",
					AnnotationFullSyncSchedule:          "0 4 * * *",
					AnnotationDebounce:                  "2s",
					AnnotationExcludePaths:              "downloads/**",
					AnnotationFailoverWorkloadNamespace: "default",
					AnnotationSelectedNode:              "kapolei-pacific-1",
				},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: &storageClass,
				VolumeName:       "pvc-123",
			},
		},
		deployment("writer", "default", "kapolei-pacific-1"),
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
	updated, err := client.AppsV1().Deployments("default").Get(context.Background(), "writer", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if got := updated.Spec.Template.Spec.NodeSelector[RoleLabel("default", "data")]; got != "active" {
		t.Fatalf("nodeSelector = %#v, want active volume label", updated.Spec.Template.Spec.NodeSelector)
	}
	if _, ok := updated.Spec.Template.Spec.NodeSelector[corev1.LabelHostname]; ok {
		t.Fatalf("nodeSelector still has hard hostname pin: %#v", updated.Spec.Template.Spec.NodeSelector)
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
					AnnotationFailoverWorkloadName: "writer",
					AnnotationFailoverGracePeriod:  "1s",
					AnnotationFailoverMaxStaleness: "30s",
				},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: &storageClass,
				VolumeName:       "pvc-123",
			},
		},
		deployment("writer", "default", "kapolei-pacific-1"),
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
	updated, err := client.AppsV1().Deployments("default").Get(context.Background(), "writer", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	if got := updated.Spec.Template.Spec.NodeSelector["kubernetes.io/hostname"]; got != "kapolei-pacific-1" {
		t.Fatalf("nodeSelector = %q, want original node", got)
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
					AnnotationFailoverWorkloadName: "writer",
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
		deployment("writer", "default", "kapolei-pacific-1"),
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
					AnnotationFailoverWorkloadName: "writer",
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
		deployment("writer", "default", "kapolei-pacific-1"),
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
		&corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "data",
				Namespace: "default",
				Annotations: map[string]string{
					AnnotationReplicationEnabled:   "true",
					AnnotationFailoverEnabled:      "true",
					AnnotationFailoverWorkloadName: "writer",
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
					AnnotationFailoverWorkloadName: "writer",
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
		deployment("writer", "default", "kapolei-pacific-1"),
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

func deployment(name, namespace, node string) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					NodeSelector: map[string]string{"kubernetes.io/hostname": node},
				},
			},
		},
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
