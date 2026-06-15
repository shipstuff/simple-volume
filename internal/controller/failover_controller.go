package controller

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	AnnotationFailoverEnabled           = LabelPrefix + "/failover-enabled"
	AnnotationFailoverWorkloadKind      = LabelPrefix + "/failover-workload-kind"
	AnnotationFailoverWorkloadName      = LabelPrefix + "/failover-workload-name"
	AnnotationFailoverWorkloadNamespace = LabelPrefix + "/failover-workload-namespace"
	AnnotationFailoverGracePeriod       = LabelPrefix + "/failover-grace-period"
	AnnotationFailoverMaxStaleness      = LabelPrefix + "/failover-max-staleness"
)

type FailoverController struct {
	client    kubernetes.Interface
	cfg       ReplicationControllerConfig
	mu        sync.Mutex
	seen      map[string]time.Time
	freshness map[string]map[string]ReplicaFreshness
	demoted   map[string]map[string]bool
}

type ReplicaFreshness struct {
	LastSuccessfulSync time.Time
	Healthy            bool
}

type FailoverDecision struct {
	Promote    bool
	TargetNode string
	Reason     string
}

func NewFailoverController(client kubernetes.Interface, cfg ReplicationControllerConfig) *FailoverController {
	return &FailoverController{
		client:    client,
		cfg:       cfg,
		seen:      make(map[string]time.Time),
		freshness: make(map[string]map[string]ReplicaFreshness),
		demoted:   make(map[string]map[string]bool),
	}
}

func (c *FailoverController) RecordReplicaFreshness(namespace, volume, node string, lastSuccessfulSync time.Time, healthy bool) {
	if volume == "" || node == "" || lastSuccessfulSync.IsZero() {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := volumeKey(namespace, volume)
	if c.freshness[key] == nil {
		c.freshness[key] = make(map[string]ReplicaFreshness)
	}
	c.freshness[key][node] = ReplicaFreshness{LastSuccessfulSync: lastSuccessfulSync.UTC(), Healthy: healthy}
}

func (c *FailoverController) ShouldBackupBeforeRestore(namespace, volume, node string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.demoted[volumeKey(namespace, volume)][node]
}

func (c *FailoverController) MarkRestored(namespace, volume, node string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	nodes := c.demoted[volumeKey(namespace, volume)]
	if nodes == nil {
		return
	}
	delete(nodes, node)
	if len(nodes) == 0 {
		delete(c.demoted, volumeKey(namespace, volume))
	}
}

func (c *FailoverController) Reconcile(ctx context.Context, now time.Time) error {
	pvcs, err := c.client.CoreV1().PersistentVolumeClaims("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	agentPods, err := c.client.CoreV1().Pods(c.cfg.Namespace).List(ctx, metav1.ListOptions{LabelSelector: c.cfg.AgentLabelSelector})
	if err != nil {
		return err
	}
	nodes, err := c.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	readyNodes := readyNodeSet(nodes.Items)
	agents := readyAgentPods(agentPods.Items)
	for _, pvc := range pvcs.Items {
		if !failoverEnabled(&pvc, c.cfg.StorageClassName) {
			continue
		}
		if err := c.reconcilePVC(ctx, &pvc, agents, readyNodes, now); err != nil {
			log.Printf("reconcile failover %s/%s: %v", pvc.Namespace, pvc.Name, err)
		}
	}
	return nil
}

func (c *FailoverController) reconcilePVC(ctx context.Context, pvc *corev1.PersistentVolumeClaim, agents map[string]corev1.Pod, readyNodes map[string]bool, now time.Time) error {
	pods, err := c.client.CoreV1().Pods(pvc.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	usingClaim := podsUsingClaim(pods.Items, pvc.Name)
	if hasHealthyClaimPod(usingClaim, readyNodes) {
		delete(c.seen, pvcKey(pvc))
		return nil
	}
	firstSeen, ok := c.seen[pvcKey(pvc)]
	if !ok {
		c.seen[pvcKey(pvc)] = now
		return nil
	}
	grace := failoverGracePeriod(pvc)
	if now.Sub(firstSeen) < grace {
		return nil
	}
	decision := SelectFailoverTarget(usingClaim, agents, readyNodes, c.freshnessForPVC(pvc), failoverMaxStaleness(pvc), now)
	if !decision.Promote {
		return fmt.Errorf("failover blocked: %s", decision.Reason)
	}
	if err := c.patchDeploymentNodeSelector(ctx, pvc, decision.TargetNode); err != nil {
		return err
	}
	for _, pod := range usingClaim {
		if pod.Spec.NodeName == decision.TargetNode {
			continue
		}
		c.recordDemoted(pvc, pod.Spec.NodeName)
		if err := c.client.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	log.Printf("promoted pvc %s/%s to node %s", pvc.Namespace, pvc.Name, decision.TargetNode)
	delete(c.seen, pvcKey(pvc))
	return nil
}

func SelectFailoverTarget(pods []corev1.Pod, agents map[string]corev1.Pod, readyNodes map[string]bool, freshness map[string]ReplicaFreshness, maxStaleness time.Duration, now time.Time) FailoverDecision {
	blocked := make(map[string]bool)
	for _, pod := range pods {
		if pod.Spec.NodeName != "" {
			blocked[pod.Spec.NodeName] = true
		}
	}
	type candidate struct {
		node      string
		freshness ReplicaFreshness
	}
	candidates := make([]candidate, 0, len(agents))
	for node := range agents {
		if !readyNodes[node] || blocked[node] {
			continue
		}
		replica := freshness[node]
		if !replica.Healthy || replica.LastSuccessfulSync.IsZero() {
			continue
		}
		if maxStaleness > 0 && now.Sub(replica.LastSuccessfulSync) > maxStaleness {
			continue
		}
		candidates = append(candidates, candidate{node: node, freshness: replica})
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].freshness.LastSuccessfulSync.Equal(candidates[j].freshness.LastSuccessfulSync) {
			return candidates[i].node < candidates[j].node
		}
		return candidates[i].freshness.LastSuccessfulSync.After(candidates[j].freshness.LastSuccessfulSync)
	})
	if len(candidates) == 0 {
		return FailoverDecision{Reason: "NoReadyReplicaNode"}
	}
	return FailoverDecision{Promote: true, TargetNode: candidates[0].node, Reason: "FreshReplicaNode"}
}

func (c *FailoverController) patchDeploymentNodeSelector(ctx context.Context, pvc *corev1.PersistentVolumeClaim, targetNode string) error {
	kind := strings.TrimSpace(pvc.Annotations[AnnotationFailoverWorkloadKind])
	if kind == "" {
		kind = "Deployment"
	}
	if kind != "Deployment" {
		return fmt.Errorf("unsupported failover workload kind %q", kind)
	}
	name := strings.TrimSpace(pvc.Annotations[AnnotationFailoverWorkloadName])
	if name == "" {
		return fmt.Errorf("failover workload name annotation is required")
	}
	namespace := strings.TrimSpace(pvc.Annotations[AnnotationFailoverWorkloadNamespace])
	if namespace == "" {
		namespace = pvc.Namespace
	}
	deployment, err := c.client.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	updated := deployment.DeepCopy()
	if updated.Spec.Template.Spec.NodeSelector == nil {
		updated.Spec.Template.Spec.NodeSelector = make(map[string]string)
	}
	updated.Spec.Template.Spec.NodeSelector["kubernetes.io/hostname"] = targetNode
	if deploymentNodeSelectorEqual(deployment, updated) {
		return nil
	}
	_, err = c.client.AppsV1().Deployments(namespace).Update(ctx, updated, metav1.UpdateOptions{})
	return err
}

func failoverEnabled(pvc *corev1.PersistentVolumeClaim, storageClass string) bool {
	if !replicationEnabled(pvc, storageClass) {
		return false
	}
	return truthy(pvc.Annotations[AnnotationFailoverEnabled])
}

func failoverGracePeriod(pvc *corev1.PersistentVolumeClaim) time.Duration {
	value := strings.TrimSpace(pvc.Annotations[AnnotationFailoverGracePeriod])
	if value == "" {
		return time.Minute
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration < 0 {
		return time.Minute
	}
	return duration
}

func failoverMaxStaleness(pvc *corev1.PersistentVolumeClaim) time.Duration {
	value := strings.TrimSpace(pvc.Annotations[AnnotationFailoverMaxStaleness])
	if value == "" {
		return 2 * time.Minute
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration < 0 {
		return 2 * time.Minute
	}
	return duration
}

func (c *FailoverController) freshnessForPVC(pvc *corev1.PersistentVolumeClaim) map[string]ReplicaFreshness {
	c.mu.Lock()
	defer c.mu.Unlock()
	source := c.freshness[volumeKey(pvc.Namespace, pvc.Spec.VolumeName)]
	out := make(map[string]ReplicaFreshness, len(source))
	for node, freshness := range source {
		out[node] = freshness
	}
	return out
}

func (c *FailoverController) recordDemoted(pvc *corev1.PersistentVolumeClaim, node string) {
	if node == "" || pvc.Spec.VolumeName == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	key := volumeKey(pvc.Namespace, pvc.Spec.VolumeName)
	if c.demoted[key] == nil {
		c.demoted[key] = make(map[string]bool)
	}
	c.demoted[key][node] = true
}

func volumeKey(namespace, volume string) string {
	return namespace + "/" + volume
}

func readyNodeSet(nodes []corev1.Node) map[string]bool {
	out := make(map[string]bool)
	for _, node := range nodes {
		if node.Spec.Unschedulable {
			continue
		}
		for _, condition := range node.Status.Conditions {
			if condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue {
				out[node.Name] = true
				break
			}
		}
	}
	return out
}

func podsUsingClaim(pods []corev1.Pod, claim string) []corev1.Pod {
	out := make([]corev1.Pod, 0)
	for _, pod := range pods {
		if podUsesClaim(pod, claim) {
			out = append(out, pod)
		}
	}
	return out
}

func hasHealthyClaimPod(pods []corev1.Pod, readyNodes map[string]bool) bool {
	for _, pod := range pods {
		if pod.Status.Phase == corev1.PodRunning && pod.Spec.NodeName != "" && readyNodes[pod.Spec.NodeName] {
			return true
		}
	}
	return false
}

func pvcKey(pvc *corev1.PersistentVolumeClaim) string {
	return pvc.Namespace + "/" + pvc.Name
}

func deploymentNodeSelectorEqual(before, after *appsv1.Deployment) bool {
	var beforeNode, afterNode string
	if before.Spec.Template.Spec.NodeSelector != nil {
		beforeNode = before.Spec.Template.Spec.NodeSelector["kubernetes.io/hostname"]
	}
	if after.Spec.Template.Spec.NodeSelector != nil {
		afterNode = after.Spec.Template.Spec.NodeSelector["kubernetes.io/hostname"]
	}
	return beforeNode == afterNode
}
