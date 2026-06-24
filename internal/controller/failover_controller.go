package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

const (
	AnnotationFailoverEnabled      = LabelPrefix + "/failover-enabled"
	AnnotationFailoverGracePeriod  = LabelPrefix + "/failover-grace-period"
	AnnotationFailoverMaxStaleness = LabelPrefix + "/failover-max-staleness"
	AnnotationFailoverNodePriority = LabelPrefix + "/failover-node-priority"
	AnnotationActiveNode           = LabelPrefix + "/active-node"
	AnnotationPreviousActiveNode   = LabelPrefix + "/previous-active-node"
	AnnotationSelectedNode         = "volume.kubernetes.io/selected-node"
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
	allPods, err := c.client.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	available := nodeAvailableResources(nodes.Items, allPods.Items)
	for _, pvc := range pvcs.Items {
		if !failoverEnabled(&pvc, c.cfg.StorageClassName) {
			continue
		}
		if err := c.reconcilePVC(ctx, &pvc, agents, readyNodes, available, now); err != nil {
			log.Printf("reconcile failover %s/%s: %v", pvc.Namespace, pvc.Name, err)
		}
	}
	return nil
}

func (c *FailoverController) reconcilePVC(ctx context.Context, pvc *corev1.PersistentVolumeClaim, agents map[string]corev1.Pod, readyNodes map[string]bool, available map[string]corev1.ResourceList, now time.Time) error {
	pods, err := c.client.CoreV1().Pods(pvc.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	usingClaim := podsUsingClaim(pods.Items, pvc.Name)
	freshness := c.freshnessForPVC(pvc)
	demoted := c.demotedForPVC(pvc)
	requests := failoverWorkloadRequests(usingClaim)
	candidates := failoverCandidates(usingClaim, agents, readyNodes, freshness, demoted, failoverMaxStaleness(pvc), now, requests, available)
	hasDemand, err := c.claimHasDesiredConsumer(ctx, pvc, usingClaim)
	if err != nil {
		return err
	}
	activeNode := healthyClaimPodNode(usingClaim, readyNodes)
	if activeNode == "" {
		activeNode = strings.TrimSpace(pvc.Annotations[AnnotationActiveNode])
	}
	if activeNode == "" {
		activeNode = strings.TrimSpace(pvc.Annotations[AnnotationSelectedNode])
	}
	if activeNode == "" && hasDemand {
		activeNode = initialActiveNode(agents, readyNodes, failoverNodePriority(pvc), requests, available)
	}
	if activeNode != "" {
		if err := c.recordActiveNode(ctx, pvc, activeNode, ""); err != nil {
			return err
		}
	}
	if err := c.reconcileNodeLabels(ctx, pvc, activeNode, candidates); err != nil {
		return err
	}
	if !hasDemand {
		delete(c.seen, pvcKey(pvc))
		return nil
	}
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
	decision := SelectFailoverTargetFromCandidates(candidates, failoverNodePriority(pvc))
	if !decision.Promote {
		return fmt.Errorf("failover blocked: %s", decision.Reason)
	}
	previousActive := activeNodeFromPods(usingClaim, decision.TargetNode)
	if previousActive == "" && activeNode != "" && activeNode != decision.TargetNode {
		previousActive = activeNode
	}
	if err := c.promoteStorageState(ctx, pvc, decision.TargetNode, previousActive); err != nil {
		return err
	}
	if err := c.promoteNodeLabel(ctx, pvc, decision.TargetNode); err != nil {
		return err
	}
	for _, pod := range usingClaim {
		if pod.Spec.NodeName == "" || pod.Spec.NodeName == decision.TargetNode {
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

func initialActiveNode(agents map[string]corev1.Pod, readyNodes map[string]bool, priority []string, requests corev1.ResourceList, available map[string]corev1.ResourceList) string {
	for _, node := range priority {
		if _, ok := agents[node]; ok && readyNodes[node] && nodeFitsRequests(node, requests, available) {
			return node
		}
	}
	nodes := make([]string, 0, len(agents))
	for node := range agents {
		if readyNodes[node] && nodeFitsRequests(node, requests, available) {
			nodes = append(nodes, node)
		}
	}
	sort.Strings(nodes)
	if len(nodes) == 0 {
		return ""
	}
	return nodes[0]
}

func (c *FailoverController) claimHasDesiredConsumer(ctx context.Context, pvc *corev1.PersistentVolumeClaim, usingClaim []corev1.Pod) (bool, error) {
	if len(usingClaim) > 0 {
		return true, nil
	}
	deployments, err := c.client.AppsV1().Deployments(pvc.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, err
	}
	for _, deployment := range deployments.Items {
		if desiredReplicas(deployment.Spec.Replicas, 1) > 0 && podTemplateUsesClaim(deployment.Spec.Template, pvc.Name) {
			return true, nil
		}
	}
	statefulSets, err := c.client.AppsV1().StatefulSets(pvc.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, err
	}
	for _, statefulSet := range statefulSets.Items {
		if desiredReplicas(statefulSet.Spec.Replicas, 1) > 0 &&
			(podTemplateUsesClaim(statefulSet.Spec.Template, pvc.Name) || statefulSetVolumeClaimMatches(statefulSet, pvc.Name)) {
			return true, nil
		}
	}
	replicaSets, err := c.client.AppsV1().ReplicaSets(pvc.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, err
	}
	for _, replicaSet := range replicaSets.Items {
		if desiredReplicas(replicaSet.Spec.Replicas, 1) > 0 && podTemplateUsesClaim(replicaSet.Spec.Template, pvc.Name) {
			return true, nil
		}
	}
	daemonSets, err := c.client.AppsV1().DaemonSets(pvc.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return false, err
	}
	for _, daemonSet := range daemonSets.Items {
		if podTemplateUsesClaim(daemonSet.Spec.Template, pvc.Name) {
			return true, nil
		}
	}
	return false, nil
}

func SelectFailoverTarget(pods []corev1.Pod, agents map[string]corev1.Pod, readyNodes map[string]bool, freshness map[string]ReplicaFreshness, maxStaleness time.Duration, now time.Time) FailoverDecision {
	return SelectFailoverTargetFromCandidates(failoverCandidates(pods, agents, readyNodes, freshness, nil, maxStaleness, now, corev1.ResourceList{}, nil), nil)
}

func SelectFailoverTargetFromCandidates(candidates []FailoverCandidate, priority []string) FailoverDecision {
	byNode := make(map[string]FailoverCandidate, len(candidates))
	for _, candidate := range candidates {
		byNode[candidate.Node] = candidate
	}
	for _, node := range priority {
		if _, ok := byNode[node]; ok {
			return FailoverDecision{Promote: true, TargetNode: node, Reason: "PreferredFreshReplicaNode"}
		}
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].Freshness.LastSuccessfulSync.Equal(candidates[j].Freshness.LastSuccessfulSync) {
			return candidates[i].Node < candidates[j].Node
		}
		return candidates[i].Freshness.LastSuccessfulSync.After(candidates[j].Freshness.LastSuccessfulSync)
	})
	if len(candidates) == 0 {
		return FailoverDecision{Reason: "NoReadyReplicaNode"}
	}
	return FailoverDecision{Promote: true, TargetNode: candidates[0].Node, Reason: "FreshReplicaNode"}
}

type FailoverCandidate struct {
	Node      string
	Freshness ReplicaFreshness
}

func failoverCandidates(pods []corev1.Pod, agents map[string]corev1.Pod, readyNodes map[string]bool, freshness map[string]ReplicaFreshness, demoted map[string]bool, maxStaleness time.Duration, now time.Time, requests corev1.ResourceList, available map[string]corev1.ResourceList) []FailoverCandidate {
	blocked := make(map[string]bool)
	for _, pod := range pods {
		if pod.Spec.NodeName != "" {
			blocked[pod.Spec.NodeName] = true
		}
	}
	candidates := make([]FailoverCandidate, 0, len(agents))
	for node := range agents {
		if !readyNodes[node] || blocked[node] || demoted[node] {
			continue
		}
		replica := freshness[node]
		if !replica.Healthy || replica.LastSuccessfulSync.IsZero() {
			continue
		}
		if maxStaleness > 0 && now.Sub(replica.LastSuccessfulSync) > maxStaleness {
			continue
		}
		if !nodeFitsRequests(node, requests, available) {
			continue
		}
		candidates = append(candidates, FailoverCandidate{Node: node, Freshness: replica})
	}
	return candidates
}

func (c *FailoverController) recordActiveNode(ctx context.Context, pvc *corev1.PersistentVolumeClaim, targetNode, previousActive string) error {
	if pvc.Spec.VolumeName == "" {
		return c.promotePVCState(ctx, pvc, targetNode, previousActive)
	}
	return c.promoteStorageState(ctx, pvc, targetNode, previousActive)
}

func (c *FailoverController) promoteStorageState(ctx context.Context, pvc *corev1.PersistentVolumeClaim, targetNode, previousActive string) error {
	if pvc.Spec.VolumeName == "" {
		return fmt.Errorf("pvc %s/%s has no bound volume", pvc.Namespace, pvc.Name)
	}
	pv, err := c.client.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	updatedPV := pv.DeepCopy()
	if updatedPV.Annotations == nil {
		updatedPV.Annotations = make(map[string]string)
	}
	updatedPV.Annotations[AnnotationActiveNode] = targetNode
	if previousActive != "" {
		updatedPV.Annotations[AnnotationPreviousActiveNode] = previousActive
	}
	if !reflect.DeepEqual(pv.Annotations, updatedPV.Annotations) {
		if _, err := c.client.CoreV1().PersistentVolumes().Update(ctx, updatedPV, metav1.UpdateOptions{}); err != nil {
			return err
		}
	}

	return c.promotePVCState(ctx, pvc, targetNode, previousActive)
}

func (c *FailoverController) promotePVCState(ctx context.Context, pvc *corev1.PersistentVolumeClaim, targetNode, previousActive string) error {
	latestPVC, err := c.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	updatedPVC := latestPVC.DeepCopy()
	if updatedPVC.Annotations == nil {
		updatedPVC.Annotations = make(map[string]string)
	}
	updatedPVC.Annotations[AnnotationActiveNode] = targetNode
	if previousActive != "" {
		updatedPVC.Annotations[AnnotationPreviousActiveNode] = previousActive
	}
	delete(updatedPVC.Annotations, AnnotationSelectedNode)
	if !reflect.DeepEqual(latestPVC.Annotations, updatedPVC.Annotations) {
		if _, err := c.client.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(ctx, updatedPVC, metav1.UpdateOptions{}); err != nil {
			return err
		}
	}
	return nil
}

func (c *FailoverController) promoteNodeLabel(ctx context.Context, pvc *corev1.PersistentVolumeClaim, targetNode string) error {
	return c.reconcileNodeLabels(ctx, pvc, targetNode, nil)
}

func (c *FailoverController) reconcileNodeLabels(ctx context.Context, pvc *corev1.PersistentVolumeClaim, activeNode string, candidates []FailoverCandidate) error {
	nodes, err := c.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	roleLabel := RoleLabel(pvc.Namespace, pvc.Name)
	candidateLabel := CandidateLabel(pvc.Namespace, pvc.Name)
	candidateNodes := make(map[string]bool, len(candidates))
	for _, candidate := range candidates {
		candidateNodes[candidate.Node] = true
	}
	for _, node := range nodes.Items {
		patchLabels := make(map[string]any)
		if node.Name == activeNode && activeNode != "" {
			if node.Labels[roleLabel] != "active" {
				patchLabels[roleLabel] = "active"
			}
		} else {
			if _, ok := node.Labels[roleLabel]; ok {
				patchLabels[roleLabel] = nil
			}
		}
		if candidateNodes[node.Name] {
			if node.Labels[candidateLabel] != "true" {
				patchLabels[candidateLabel] = "true"
			}
		} else {
			if _, ok := node.Labels[candidateLabel]; ok {
				patchLabels[candidateLabel] = nil
			}
		}
		if len(patchLabels) == 0 {
			continue
		}
		patch, err := json.Marshal(map[string]any{
			"metadata": map[string]any{
				"labels": patchLabels,
			},
		})
		if err != nil {
			return err
		}
		if _, err := c.client.CoreV1().Nodes().Patch(ctx, node.Name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
			return err
		}
	}
	return nil
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

func failoverNodePriority(pvc *corev1.PersistentVolumeClaim) []string {
	return csvAnnotation(pvc.Annotations[AnnotationFailoverNodePriority])
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

func (c *FailoverController) demotedForPVC(pvc *corev1.PersistentVolumeClaim) map[string]bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	source := c.demoted[volumeKey(pvc.Namespace, pvc.Spec.VolumeName)]
	out := make(map[string]bool, len(source))
	for node, demoted := range source {
		out[node] = demoted
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

func activeNodeFromPods(pods []corev1.Pod, promotedNode string) string {
	for _, pod := range pods {
		if pod.Spec.NodeName != "" && pod.Spec.NodeName != promotedNode {
			return pod.Spec.NodeName
		}
	}
	return ""
}

func healthyClaimPodNode(pods []corev1.Pod, readyNodes map[string]bool) string {
	for _, pod := range pods {
		if pod.Status.Phase == corev1.PodRunning && pod.Spec.NodeName != "" && readyNodes[pod.Spec.NodeName] {
			return pod.Spec.NodeName
		}
	}
	return ""
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

func podTemplateUsesClaim(template corev1.PodTemplateSpec, claim string) bool {
	return podSpecUsesClaim(template.Spec, claim)
}

func podSpecUsesClaim(spec corev1.PodSpec, claim string) bool {
	for _, volume := range spec.Volumes {
		if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == claim {
			return true
		}
	}
	return false
}

func statefulSetVolumeClaimMatches(statefulSet appsv1.StatefulSet, claim string) bool {
	for _, template := range statefulSet.Spec.VolumeClaimTemplates {
		if template.Name == claim {
			return true
		}
		prefix := template.Name + "-" + statefulSet.Name + "-"
		if strings.HasPrefix(claim, prefix) {
			return true
		}
	}
	return false
}

func desiredReplicas(replicas *int32, defaultValue int32) int32 {
	if replicas == nil {
		return defaultValue
	}
	return *replicas
}

func hasHealthyClaimPod(pods []corev1.Pod, readyNodes map[string]bool) bool {
	return healthyClaimPodNode(pods, readyNodes) != ""
}

func pvcKey(pvc *corev1.PersistentVolumeClaim) string {
	return pvc.Namespace + "/" + pvc.Name
}

func failoverWorkloadRequests(pods []corev1.Pod) corev1.ResourceList {
	out := corev1.ResourceList{}
	for _, pod := range pods {
		addResourceList(out, podRequest(pod))
	}
	return out
}

func nodeAvailableResources(nodes []corev1.Node, pods []corev1.Pod) map[string]corev1.ResourceList {
	out := make(map[string]corev1.ResourceList, len(nodes))
	for _, node := range nodes {
		available := corev1.ResourceList{}
		for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
			if quantity, ok := node.Status.Allocatable[name]; ok {
				available[name] = quantity.DeepCopy()
			}
		}
		out[node.Name] = available
	}
	for _, pod := range pods {
		if pod.Spec.NodeName == "" || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		available, ok := out[pod.Spec.NodeName]
		if !ok {
			continue
		}
		subtractResourceList(available, podRequest(pod))
	}
	return out
}

func podRequest(pod corev1.Pod) corev1.ResourceList {
	out := corev1.ResourceList{}
	for _, container := range pod.Spec.Containers {
		addResourceList(out, container.Resources.Requests)
	}
	for _, container := range pod.Spec.InitContainers {
		maxResourceList(out, container.Resources.Requests)
	}
	return out
}

func nodeFitsRequests(node string, requests corev1.ResourceList, available map[string]corev1.ResourceList) bool {
	if len(requests) == 0 || len(available) == 0 {
		return true
	}
	nodeAvailable, ok := available[node]
	if !ok || len(nodeAvailable) == 0 {
		return true
	}
	for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		requested, ok := requests[name]
		if !ok || requested.IsZero() {
			continue
		}
		free, ok := nodeAvailable[name]
		if !ok || free.IsZero() {
			continue
		}
		if free.Cmp(requested) < 0 {
			return false
		}
	}
	return true
}

func addResourceList(target corev1.ResourceList, source corev1.ResourceList) {
	for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		quantity, ok := source[name]
		if !ok || quantity.IsZero() {
			continue
		}
		current := target[name]
		current.Add(quantity)
		target[name] = current
	}
}

func subtractResourceList(target corev1.ResourceList, source corev1.ResourceList) {
	for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		quantity, ok := source[name]
		if !ok || quantity.IsZero() {
			continue
		}
		current, ok := target[name]
		if !ok {
			continue
		}
		current.Sub(quantity)
		target[name] = current
	}
}

func maxResourceList(target corev1.ResourceList, source corev1.ResourceList) {
	for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		quantity, ok := source[name]
		if !ok || quantity.IsZero() {
			continue
		}
		current := target[name]
		if current.Cmp(quantity) < 0 {
			target[name] = quantity.DeepCopy()
		}
	}
}
