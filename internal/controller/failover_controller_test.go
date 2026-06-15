package controller

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestFailoverControllerPatchesDeploymentAndDeletesStalePod(t *testing.T) {
	storageClass := "simple-volume"
	client := fake.NewSimpleClientset(
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
					AnnotationIncludePaths:              "writes.log",
					AnnotationFullSyncOnStart:           "true",
					AnnotationFullSyncSchedule:          "0 4 * * *",
					AnnotationDebounce:                  "2s",
					AnnotationExcludePaths:              "downloads/**",
					AnnotationFailoverWorkloadNamespace: "default",
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
	if got := updated.Spec.Template.Spec.NodeSelector["kubernetes.io/hostname"]; got != "fresno-west-1" {
		t.Fatalf("nodeSelector = %q, want fresno-west-1", got)
	}
	_, err = client.CoreV1().Pods("default").Get(context.Background(), "writer-old", metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("old pod error = %v, want not found", err)
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
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{
				Type:   corev1.NodeReady,
				Status: status,
			}},
		},
	}
}
