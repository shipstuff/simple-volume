package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/shipstuff/simple-volume/internal/agent"
)

const (
	AnnotationReplicationEnabled  = LabelPrefix + "/replication-enabled"
	AnnotationIncludePaths        = LabelPrefix + "/replication-include-paths"
	AnnotationExcludePaths        = LabelPrefix + "/replication-exclude-paths"
	AnnotationPruneExcluded       = LabelPrefix + "/replication-prune-excluded"
	AnnotationDebounce            = LabelPrefix + "/replication-debounce"
	AnnotationFullSyncOnStart     = LabelPrefix + "/replication-full-sync-on-start"
	AnnotationFullSyncSchedule    = LabelPrefix + "/replication-full-sync-schedule"
	AnnotationRequiredPaths       = LabelPrefix + "/replication-required-paths"
	AnnotationConsistencyMode     = LabelPrefix + "/replication-consistency-mode"
	AnnotationConfirmedReplicas   = LabelPrefix + "/replication-confirmed-replicas"
	AnnotationReplicationOwnerUID = LabelPrefix + "/replication-owner-uid"
	AnnotationReplicationOwnerGID = LabelPrefix + "/replication-owner-gid"
	AnnotationReplicationFileMode = LabelPrefix + "/replication-file-mode"
	AnnotationReplicationDirMode  = LabelPrefix + "/replication-dir-mode"
)

type ReplicationControllerConfig struct {
	Namespace          string
	StorageClassName   string
	TokenSecretName    string
	TokenSecretKey     string
	ReconcileInterval  time.Duration
	FailoverTimeout    time.Duration
	HTTPTimeout        time.Duration
	AgentLabelSelector string
}

type ReplicationController struct {
	client   kubernetes.Interface
	cfg      ReplicationControllerConfig
	http     *http.Client
	failover *FailoverController

	mu              sync.Mutex
	startedWatches  map[string]string
	completedSyncs  map[string]bool
	runningSyncs    map[string]bool
	lastScheduledOn map[string]string
	lastActiveNode  map[string]string
	activeEpoch     map[string]int64
}

type DesiredTarget struct {
	Node string
	Ref  agent.TargetRef
}

type DesiredReplication struct {
	Namespace         string
	ClaimName         string
	Volume            string
	ActiveNode        string
	SourceURL         string
	Targets           []DesiredTarget
	IncludePaths      []string
	ExcludePaths      []string
	PruneExcluded     bool
	RequiredPaths     []string
	Ownership         agent.OwnershipPolicy
	Debounce          string
	FullSync          bool
	FullSchedule      string
	ConsistencyMode   string
	ConfirmedReplicas int
}

func NewReplicationController(client kubernetes.Interface, cfg ReplicationControllerConfig) *ReplicationController {
	if cfg.Namespace == "" {
		cfg.Namespace = "simple-volume-system"
	}
	if cfg.StorageClassName == "" {
		cfg.StorageClassName = "simple-volume"
	}
	if cfg.TokenSecretName == "" {
		cfg.TokenSecretName = "simple-volume-sync-token"
	}
	if cfg.TokenSecretKey == "" {
		cfg.TokenSecretKey = "token"
	}
	if cfg.ReconcileInterval <= 0 {
		cfg.ReconcileInterval = 30 * time.Second
	}
	if cfg.FailoverTimeout <= 0 {
		cfg.FailoverTimeout = 10 * time.Second
	}
	if cfg.HTTPTimeout <= 0 {
		cfg.HTTPTimeout = time.Hour
	}
	if cfg.AgentLabelSelector == "" {
		cfg.AgentLabelSelector = "app.kubernetes.io/component=node"
	}
	controller := &ReplicationController{
		client:          client,
		cfg:             cfg,
		http:            &http.Client{Timeout: cfg.HTTPTimeout},
		startedWatches:  make(map[string]string),
		completedSyncs:  make(map[string]bool),
		runningSyncs:    make(map[string]bool),
		lastScheduledOn: make(map[string]string),
		lastActiveNode:  make(map[string]string),
		activeEpoch:     make(map[string]int64),
	}
	controller.failover = NewFailoverController(client, cfg)
	return controller
}

func RunReplicationController(ctx context.Context, cfg ReplicationControllerConfig) error {
	restConfig, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("load in-cluster config: %w", err)
	}
	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("create kubernetes client: %w", err)
	}
	return NewReplicationController(client, cfg).Run(ctx)
}

func (c *ReplicationController) Run(ctx context.Context) error {
	if err := c.Reconcile(ctx); err != nil {
		log.Printf("replication reconcile failed: %v", err)
	}
	ticker := time.NewTicker(c.cfg.ReconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := c.Reconcile(ctx); err != nil {
				log.Printf("replication reconcile failed: %v", err)
			}
		}
	}
}

func (c *ReplicationController) Reconcile(ctx context.Context) error {
	now := time.Now()
	if c.failover != nil {
		failoverCtx, cancel := context.WithTimeout(ctx, c.cfg.FailoverTimeout)
		err := c.failover.Reconcile(failoverCtx, now)
		cancel()
		if err != nil {
			log.Printf("failover reconcile failed: %v", err)
		}
	}
	token, err := c.syncToken(ctx)
	if err != nil {
		return err
	}
	desired, err := c.DesiredReplications(ctx, token)
	if err != nil {
		return err
	}
	if len(desired) > 0 {
		log.Printf("replication reconcile desired=%d", len(desired))
	}
	for _, item := range desired {
		if err := c.reconcileOne(ctx, item, token, now); err != nil {
			log.Printf("reconcile replication %s/%s: %v", item.Namespace, item.ClaimName, err)
		}
	}
	return nil
}

func (c *ReplicationController) DesiredReplications(ctx context.Context, token string) ([]DesiredReplication, error) {
	pvcs, err := c.client.CoreV1().PersistentVolumeClaims("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	agentPods, err := c.client.CoreV1().Pods(c.cfg.Namespace).List(ctx, metav1.ListOptions{LabelSelector: c.cfg.AgentLabelSelector})
	if err != nil {
		return nil, err
	}
	agents := readyAgentPods(agentPods.Items)
	out := make([]DesiredReplication, 0)
	for _, pvc := range pvcs.Items {
		if !replicationEnabled(&pvc, c.cfg.StorageClassName) {
			continue
		}
		if pvc.Spec.VolumeName == "" {
			continue
		}
		activeNode := strings.TrimSpace(pvc.Annotations[AnnotationActiveNode])
		activePod, ok, err := c.activePodForPVC(ctx, pvc.Namespace, pvc.Name)
		if err != nil {
			return nil, err
		}
		ownership := agent.OwnershipPolicy{}
		if ok {
			activeNode = activePod.Spec.NodeName
			ownership = inferOwnershipFromPod(activePod, pvc.Name)
		}
		annotationOwnership, err := ownershipPolicyFromAnnotations(pvc.Annotations)
		if err != nil {
			return nil, fmt.Errorf("pvc %s/%s ownership annotations: %w", pvc.Namespace, pvc.Name, err)
		}
		ownership = mergeOwnershipPolicy(ownership, annotationOwnership)
		if activeNode == "" {
			log.Printf("replication pvc %s/%s has no running active pod or active-node annotation", pvc.Namespace, pvc.Name)
			continue
		}
		source, ok := agents[activeNode]
		if !ok {
			log.Printf("replication pvc %s/%s active node %s has no ready agent", pvc.Namespace, pvc.Name, activeNode)
			continue
		}
		targets := make([]DesiredTarget, 0, len(agents)-1)
		for node, pod := range agents {
			if node == activeNode {
				continue
			}
			targets = append(targets, DesiredTarget{
				Node: node,
				Ref: agent.TargetRef{
					URL:   agentHTTPURL(pod.Status.PodIP),
					Token: token,
				},
			})
		}
		sort.Slice(targets, func(i, j int) bool { return targets[i].Node < targets[j].Node })
		if len(targets) == 0 {
			log.Printf("replication pvc %s/%s has no replica targets", pvc.Namespace, pvc.Name)
			continue
		}
		out = append(out, DesiredReplication{
			Namespace:         pvc.Namespace,
			ClaimName:         pvc.Name,
			Volume:            pvc.Spec.VolumeName,
			ActiveNode:        activeNode,
			SourceURL:         agentWebDAVURL(source.Status.PodIP),
			Targets:           targets,
			IncludePaths:      csvAnnotation(pvc.Annotations[AnnotationIncludePaths]),
			ExcludePaths:      csvAnnotation(pvc.Annotations[AnnotationExcludePaths]),
			PruneExcluded:     truthy(pvc.Annotations[AnnotationPruneExcluded]),
			RequiredPaths:     csvAnnotation(pvc.Annotations[AnnotationRequiredPaths]),
			Ownership:         ownership,
			Debounce:          strings.TrimSpace(pvc.Annotations[AnnotationDebounce]),
			FullSync:          truthy(pvc.Annotations[AnnotationFullSyncOnStart]),
			FullSchedule:      strings.TrimSpace(pvc.Annotations[AnnotationFullSyncSchedule]),
			ConsistencyMode:   replicationConsistencyMode(pvc.Annotations),
			ConfirmedReplicas: confirmedReplicas(pvc.Annotations),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace == out[j].Namespace {
			return out[i].ClaimName < out[j].ClaimName
		}
		return out[i].Namespace < out[j].Namespace
	})
	return out, nil
}

func (c *ReplicationController) reconcileOne(ctx context.Context, desired DesiredReplication, token string, now time.Time) error {
	key := desired.Namespace + "/" + desired.Volume
	epoch := c.noteActiveNode(key, desired.ActiveNode)
	signature := fmt.Sprintf("epoch=%d:%s", epoch, desired.signature())
	if desired.FullSync {
		syncKey := key + ":" + signature
		c.mu.Lock()
		done := c.completedSyncs[syncKey]
		c.mu.Unlock()
		if !done {
			if c.tryStartFullSync(syncKey) {
				c.stopReplicaWatches(ctx, desired, token)
				log.Printf("starting startup full sync namespace=%s claim=%s volume=%s activeNode=%s targets=%d consistency=%s confirmedReplicas=%d",
					desired.Namespace, desired.ClaimName, desired.Volume, desired.ActiveNode, len(desired.Targets), desired.ConsistencyMode, desired.ConfirmedReplicas)
				go c.runFullSync(ctx, syncKey, desired, token, now, func() {
					if !c.isCurrentActiveEpoch(key, desired.ActiveNode, epoch) {
						log.Printf("discarded startup full sync for stale active epoch namespace=%s claim=%s volume=%s activeNode=%s",
							desired.Namespace, desired.ClaimName, desired.Volume, desired.ActiveNode)
						return
					}
					c.mu.Lock()
					c.completedSyncs[syncKey] = true
					c.mu.Unlock()
					log.Printf("completed startup full sync namespace=%s claim=%s volume=%s targets=%d",
						desired.Namespace, desired.ClaimName, desired.Volume, len(desired.Targets))
					if err := c.startWatchAndRemember(ctx, desired, token, signature); err != nil {
						log.Printf("start replication watch after full sync namespace=%s claim=%s volume=%s: %v",
							desired.Namespace, desired.ClaimName, desired.Volume, err)
					}
				})
			}
			return nil
		}
	}

	if err := c.ensureWatchStarted(ctx, desired, token, signature); err != nil {
		return err
	}

	if shouldRunScheduledFullSync(desired.FullSchedule, now) {
		scheduleKey := key + ":" + desired.FullSchedule
		today := now.Format("2006-01-02")
		c.mu.Lock()
		last := c.lastScheduledOn[scheduleKey]
		c.mu.Unlock()
		syncKey := scheduleKey + ":" + today
		if last != today && c.tryStartFullSync(syncKey) {
			log.Printf("starting scheduled full sync namespace=%s claim=%s volume=%s activeNode=%s schedule=%q targets=%d consistency=%s confirmedReplicas=%d",
				desired.Namespace, desired.ClaimName, desired.Volume, desired.ActiveNode, desired.FullSchedule, len(desired.Targets), desired.ConsistencyMode, desired.ConfirmedReplicas)
			go c.runFullSync(ctx, syncKey, desired, token, now, func() {
				c.mu.Lock()
				c.lastScheduledOn[scheduleKey] = today
				c.mu.Unlock()
				log.Printf("completed scheduled full sync namespace=%s claim=%s volume=%s schedule=%q targets=%d",
					desired.Namespace, desired.ClaimName, desired.Volume, desired.FullSchedule, len(desired.Targets))
			})
		}
	}
	return nil
}

func (c *ReplicationController) noteActiveNode(key, activeNode string) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	previous := c.lastActiveNode[key]
	if previous != "" && previous != activeNode {
		c.activeEpoch[key]++
		delete(c.startedWatches, key)
		prefix := key + ":"
		for syncKey := range c.completedSyncs {
			if strings.HasPrefix(syncKey, prefix) {
				delete(c.completedSyncs, syncKey)
			}
		}
	}
	c.lastActiveNode[key] = activeNode
	return c.activeEpoch[key]
}

func (c *ReplicationController) isCurrentActiveEpoch(key, activeNode string, epoch int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastActiveNode[key] == activeNode && c.activeEpoch[key] == epoch
}

func (c *ReplicationController) ensureWatchStarted(ctx context.Context, desired DesiredReplication, token, signature string) error {
	key := desired.Namespace + "/" + desired.Volume
	c.mu.Lock()
	alreadyStarted := c.startedWatches[key] == signature
	c.mu.Unlock()
	if alreadyStarted {
		status, err := c.watchStatus(ctx, desired, token)
		if err != nil {
			log.Printf("check replication watch namespace=%s claim=%s volume=%s: %v", desired.Namespace, desired.ClaimName, desired.Volume, err)
			alreadyStarted = false
		} else if !status.Running {
			alreadyStarted = false
		} else {
			c.recordWatchFreshness(desired, status)
		}
	}
	if !alreadyStarted {
		c.stopReplicaWatches(ctx, desired, token)
		return c.startWatchAndRemember(ctx, desired, token, signature)
	}
	return nil
}

func (c *ReplicationController) startWatchAndRemember(ctx context.Context, desired DesiredReplication, token, signature string) error {
	if err := c.startWatch(ctx, desired, token); err != nil {
		return err
	}
	key := desired.Namespace + "/" + desired.Volume
	c.mu.Lock()
	c.startedWatches[key] = signature
	c.mu.Unlock()
	log.Printf("started replication watch namespace=%s claim=%s volume=%s activeNode=%s targets=%d",
		desired.Namespace, desired.ClaimName, desired.Volume, desired.ActiveNode, len(desired.Targets))
	return nil
}

func (c *ReplicationController) stopReplicaWatches(ctx context.Context, desired DesiredReplication, token string) {
	for _, target := range desired.Targets {
		if err := c.stopWatch(ctx, target.Ref.URL, token, desired.Namespace, desired.Volume); err != nil {
			log.Printf("stop stale replica watch namespace=%s claim=%s volume=%s node=%s: %v",
				desired.Namespace, desired.ClaimName, desired.Volume, target.Node, err)
		}
	}
}

func (c *ReplicationController) tryStartFullSync(syncKey string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.runningSyncs[syncKey] {
		return false
	}
	c.runningSyncs[syncKey] = true
	return true
}

func (c *ReplicationController) runFullSync(ctx context.Context, syncKey string, desired DesiredReplication, token string, now time.Time, onSuccess func()) {
	defer func() {
		c.mu.Lock()
		delete(c.runningSyncs, syncKey)
		c.mu.Unlock()
	}()
	if err := c.fullSyncTargets(ctx, desired, token, now); err != nil {
		log.Printf("reconcile replication %s/%s: %v", desired.Namespace, desired.ClaimName, err)
		return
	}
	onSuccess()
}

func (c *ReplicationController) startWatch(ctx context.Context, desired DesiredReplication, token string) error {
	req := agent.WatchStartRequest{
		Namespace:         desired.Namespace,
		Volume:            desired.Volume,
		Source:            agent.SourceRef{WebDAVURL: desired.SourceURL},
		Targets:           targetRefs(desired.Targets),
		IncludePaths:      desired.IncludePaths,
		ExcludePaths:      desired.ExcludePaths,
		Ownership:         desired.Ownership,
		Debounce:          desired.Debounce,
		ConsistencyMode:   desired.ConsistencyMode,
		ConfirmedReplicas: intPtr(desired.ConfirmedReplicas),
	}
	return c.postJSON(ctx, strings.TrimRight(agentHTTPURLFromWebDAV(desired.SourceURL), "/")+"/replication/watch/start", token, req)
}

func (c *ReplicationController) stopWatch(ctx context.Context, agentURL, token, namespace, volume string) error {
	reqBody := agent.WatchStopRequest{
		Namespace: namespace,
		Volume:    volume,
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(agentURL, "/")+"/replication/watch/stop", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		responseBody, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("%s returned %s: %s", req.URL.String(), resp.Status, strings.TrimSpace(string(responseBody)))
	}
	return nil
}

func (c *ReplicationController) watchStatus(ctx context.Context, desired DesiredReplication, token string) (agent.WatchStatus, error) {
	statusURL := strings.TrimRight(agentHTTPURLFromWebDAV(desired.SourceURL), "/") +
		"/replication/watch/status?namespace=" + url.QueryEscape(desired.Namespace) + "&volume=" + url.QueryEscape(desired.Volume)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, statusURL, nil)
	if err != nil {
		return agent.WatchStatus{}, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := c.http.Do(req)
	if err != nil {
		return agent.WatchStatus{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return agent.WatchStatus{}, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		responseBody, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return agent.WatchStatus{}, fmt.Errorf("%s returned %s: %s", statusURL, resp.Status, strings.TrimSpace(string(responseBody)))
	}
	var status agent.WatchStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return agent.WatchStatus{}, err
	}
	return status, nil
}

func (c *ReplicationController) fullSyncTargets(ctx context.Context, desired DesiredReplication, token string, _ time.Time) error {
	var errs []string
	sourceBasePath := ""
	if desired.ConsistencyMode == agent.ConsistencyModeShadow {
		resp, err := c.prepareShadow(ctx, desired, token)
		if err != nil {
			return err
		}
		sourceBasePath = resp.SourceBasePath
	}
	successes := 0
	for _, target := range desired.Targets {
		backupExisting := c.failover.ShouldBackupBeforeRestore(desired.Namespace, desired.Volume, target.Node)
		req := agent.FullSyncRequest{
			Namespace:      desired.Namespace,
			Volume:         desired.Volume,
			Source:         agent.SourceRef{WebDAVURL: desired.SourceURL},
			SourceBasePath: sourceBasePath,
			IncludePaths:   desired.IncludePaths,
			ExcludePaths:   desired.ExcludePaths,
			PruneExcluded:  desired.PruneExcluded,
			RequiredPaths:  desired.RequiredPaths,
			Ownership:      desired.Ownership,
			BackupExisting: backupExisting,
		}
		if err := c.postJSON(ctx, strings.TrimRight(target.Ref.URL, "/")+"/replication/full-sync", token, req); err != nil {
			errs = append(errs, err.Error())
			continue
		}
		successes++
		c.failover.RecordReplicaFreshness(desired.Namespace, desired.Volume, target.Node, time.Now(), true)
		if backupExisting {
			c.failover.MarkRestored(desired.Namespace, desired.Volume, target.Node)
		}
	}
	required := confirmationThreshold(desired.ConfirmedReplicas, len(desired.Targets))
	if successes < required {
		return fmt.Errorf("full sync failed: %s", strings.Join(errs, "; "))
	}
	if len(errs) > 0 {
		log.Printf("full sync namespace=%s claim=%s volume=%s confirmed %d/%d replicas; ignored errors: %s",
			desired.Namespace, desired.ClaimName, desired.Volume, successes, required, strings.Join(errs, "; "))
	}
	return nil
}

func (c *ReplicationController) prepareShadow(ctx context.Context, desired DesiredReplication, token string) (agent.ShadowPrepareResponse, error) {
	req := agent.ShadowPrepareRequest{
		Namespace:     desired.Namespace,
		Volume:        desired.Volume,
		IncludePaths:  desired.IncludePaths,
		ExcludePaths:  desired.ExcludePaths,
		PruneExcluded: desired.PruneExcluded,
		RequiredPaths: desired.RequiredPaths,
	}
	var out agent.ShadowPrepareResponse
	err := c.postJSONDecode(ctx, strings.TrimRight(agentHTTPURLFromWebDAV(desired.SourceURL), "/")+"/replication/shadow/prepare", token, req, &out)
	if err != nil {
		return agent.ShadowPrepareResponse{}, err
	}
	if strings.TrimSpace(out.SourceBasePath) == "" {
		return agent.ShadowPrepareResponse{}, fmt.Errorf("shadow prepare returned empty sourceBasePath")
	}
	return out, nil
}

func (c *ReplicationController) postJSON(ctx context.Context, url, token string, payload any) error {
	return c.postJSONDecode(ctx, url, token, payload, nil)
}

func (c *ReplicationController) postJSONDecode(ctx context.Context, url, token string, payload any, out any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		responseBody, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("%s returned %s: %s", url, resp.Status, strings.TrimSpace(string(responseBody)))
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return err
		}
	}
	return nil
}

func (c *ReplicationController) syncToken(ctx context.Context) (string, error) {
	secret, err := c.client.CoreV1().Secrets(c.cfg.Namespace).Get(ctx, c.cfg.TokenSecretName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return "", fmt.Errorf("sync token secret %s/%s not found", c.cfg.Namespace, c.cfg.TokenSecretName)
	}
	if err != nil {
		return "", err
	}
	token := strings.TrimSpace(string(secret.Data[c.cfg.TokenSecretKey]))
	if token == "" {
		return "", fmt.Errorf("sync token secret %s/%s key %s is empty", c.cfg.Namespace, c.cfg.TokenSecretName, c.cfg.TokenSecretKey)
	}
	return token, nil
}

func (c *ReplicationController) activePodForPVC(ctx context.Context, namespace, claim string) (corev1.Pod, bool, error) {
	pods, err := c.client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return corev1.Pod{}, false, err
	}
	candidates := make([]corev1.Pod, 0)
	for _, pod := range pods.Items {
		if pod.Status.Phase != corev1.PodRunning || pod.Spec.NodeName == "" {
			continue
		}
		if podUsesClaim(pod, claim) {
			candidates = append(candidates, pod)
		}
	}
	if len(candidates) == 0 {
		return corev1.Pod{}, false, nil
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].CreationTimestamp.Before(&candidates[j].CreationTimestamp)
	})
	return candidates[0], true, nil
}

func readyAgentPods(pods []corev1.Pod) map[string]corev1.Pod {
	out := make(map[string]corev1.Pod)
	for _, pod := range pods {
		if pod.Status.Phase != corev1.PodRunning || pod.Spec.NodeName == "" || pod.Status.PodIP == "" || !podReady(pod) {
			continue
		}
		out[pod.Spec.NodeName] = pod
	}
	return out
}

func replicationEnabled(pvc *corev1.PersistentVolumeClaim, storageClass string) bool {
	if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName != storageClass {
		return false
	}
	return truthy(pvc.Annotations[AnnotationReplicationEnabled])
}

func podUsesClaim(pod corev1.Pod, claim string) bool {
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == claim {
			return true
		}
	}
	return false
}

func podReady(pod corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func csvAnnotation(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func truthy(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "t", "true", "y", "yes", "on":
		return true
	default:
		return false
	}
}

func replicationConsistencyMode(annotations map[string]string) string {
	if annotations == nil {
		return agent.ConsistencyModeShadow
	}
	switch strings.ToLower(strings.TrimSpace(annotations[AnnotationConsistencyMode])) {
	case "", agent.ConsistencyModeShadow:
		return agent.ConsistencyModeShadow
	case agent.ConsistencyModeLive:
		return agent.ConsistencyModeLive
	default:
		return agent.ConsistencyModeShadow
	}
}

func confirmedReplicas(annotations map[string]string) int {
	if annotations == nil {
		return 1
	}
	value := strings.TrimSpace(annotations[AnnotationConfirmedReplicas])
	if value == "" {
		return 1
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return 1
	}
	return parsed
}

func confirmationThreshold(value, targets int) int {
	if value <= 0 {
		return 0
	}
	if targets > 0 && value > targets {
		return targets
	}
	return value
}

func agentHTTPURL(ip string) string {
	return "http://" + ip + ":8080"
}

func agentWebDAVURL(ip string) string {
	return "http://" + ip + ":8081"
}

func agentHTTPURLFromWebDAV(webdavURL string) string {
	return strings.Replace(webdavURL, ":8081", ":8080", 1)
}

func (d DesiredReplication) signature() string {
	var b strings.Builder
	b.WriteString(d.SourceURL)
	b.WriteString("|")
	for _, target := range d.Targets {
		b.WriteString(target.Node)
		b.WriteString("=")
		b.WriteString(target.Ref.URL)
		b.WriteString(":")
		b.WriteString(target.Ref.Token)
		b.WriteString(",")
	}
	b.WriteString("|")
	b.WriteString(strings.Join(d.IncludePaths, ","))
	b.WriteString("|")
	b.WriteString(strings.Join(d.ExcludePaths, ","))
	b.WriteString("|")
	b.WriteString(strconv.FormatBool(d.PruneExcluded))
	b.WriteString("|")
	b.WriteString(ownershipSignature(d.Ownership))
	b.WriteString("|")
	b.WriteString(d.Debounce)
	b.WriteString("|")
	b.WriteString(d.ConsistencyMode)
	b.WriteString("|")
	b.WriteString(strconv.Itoa(d.ConfirmedReplicas))
	return b.String()
}

func targetRefs(targets []DesiredTarget) []agent.TargetRef {
	refs := make([]agent.TargetRef, 0, len(targets))
	for _, target := range targets {
		refs = append(refs, target.Ref)
	}
	return refs
}

func (c *ReplicationController) recordWatchFreshness(desired DesiredReplication, status agent.WatchStatus) {
	if c.failover == nil {
		return
	}
	nodesByURL := make(map[string]string, len(desired.Targets))
	for _, target := range desired.Targets {
		nodesByURL[target.Ref.URL] = target.Node
	}
	for _, targetStatus := range status.TargetStatuses {
		node := nodesByURL[targetStatus.URL]
		if node == "" || targetStatus.LastSuccessfulSync == nil {
			continue
		}
		c.failover.RecordReplicaFreshness(
			desired.Namespace,
			desired.Volume,
			node,
			*targetStatus.LastSuccessfulSync,
			targetStatus.LastError == "",
		)
	}
}

func shouldRunScheduledFullSync(schedule string, now time.Time) bool {
	fields := strings.Fields(schedule)
	if len(fields) != 5 {
		return false
	}
	minute, ok := parseSingleCronNumber(fields[0], 0, 59)
	if !ok {
		return false
	}
	hour, ok := parseSingleCronNumber(fields[1], 0, 23)
	if !ok {
		return false
	}
	return now.Minute() == minute && now.Hour() == hour
}

func parseSingleCronNumber(field string, min, max int) (int, bool) {
	value, err := strconv.Atoi(field)
	if err != nil {
		return 0, false
	}
	if value < min || value > max {
		return 0, false
	}
	return value, true
}

func inferOwnershipFromPod(pod corev1.Pod, claim string) agent.OwnershipPolicy {
	var policy agent.OwnershipPolicy
	claimVolumes := claimVolumeNames(pod, claim)
	if len(claimVolumes) == 0 {
		return policy
	}
	if pod.Spec.SecurityContext != nil {
		if pod.Spec.SecurityContext.RunAsUser != nil {
			policy.UID = int64Ptr(*pod.Spec.SecurityContext.RunAsUser)
		}
		if pod.Spec.SecurityContext.RunAsGroup != nil {
			policy.GID = int64Ptr(*pod.Spec.SecurityContext.RunAsGroup)
		} else if pod.Spec.SecurityContext.FSGroup != nil {
			policy.GID = int64Ptr(*pod.Spec.SecurityContext.FSGroup)
		}
	}
	if policy.UID != nil && policy.GID != nil {
		return policy
	}
	containers := append([]corev1.Container{}, pod.Spec.InitContainers...)
	containers = append(containers, pod.Spec.Containers...)
	for _, container := range containers {
		if !containerMountsAnyVolume(container, claimVolumes) || container.SecurityContext == nil {
			continue
		}
		if policy.UID == nil && container.SecurityContext.RunAsUser != nil {
			policy.UID = int64Ptr(*container.SecurityContext.RunAsUser)
		}
		if policy.GID == nil && container.SecurityContext.RunAsGroup != nil {
			policy.GID = int64Ptr(*container.SecurityContext.RunAsGroup)
		}
		if policy.UID != nil && policy.GID != nil {
			break
		}
	}
	return policy
}

func claimVolumeNames(pod corev1.Pod, claim string) map[string]bool {
	out := make(map[string]bool)
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == claim {
			out[volume.Name] = true
		}
	}
	return out
}

func containerMountsAnyVolume(container corev1.Container, volumeNames map[string]bool) bool {
	for _, mount := range container.VolumeMounts {
		if volumeNames[mount.Name] {
			return true
		}
	}
	return false
}

func ownershipPolicyFromAnnotations(annotations map[string]string) (agent.OwnershipPolicy, error) {
	var policy agent.OwnershipPolicy
	uid, err := parseOptionalInt64Annotation(annotations, AnnotationReplicationOwnerUID)
	if err != nil {
		return policy, err
	}
	gid, err := parseOptionalInt64Annotation(annotations, AnnotationReplicationOwnerGID)
	if err != nil {
		return policy, err
	}
	fileMode, err := parseOptionalModeAnnotation(annotations, AnnotationReplicationFileMode)
	if err != nil {
		return policy, err
	}
	dirMode, err := parseOptionalModeAnnotation(annotations, AnnotationReplicationDirMode)
	if err != nil {
		return policy, err
	}
	policy.UID = uid
	policy.GID = gid
	policy.FileMode = fileMode
	policy.DirMode = dirMode
	return policy, nil
}

func parseOptionalInt64Annotation(annotations map[string]string, key string) (*int64, error) {
	value := strings.TrimSpace(annotations[key])
	if value == "" {
		return nil, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 0 {
		return nil, fmt.Errorf("%s must be a non-negative integer", key)
	}
	return int64Ptr(parsed), nil
}

func parseOptionalModeAnnotation(annotations map[string]string, key string) (*uint32, error) {
	value := strings.TrimSpace(annotations[key])
	if value == "" {
		return nil, nil
	}
	parsed, err := strconv.ParseUint(value, 8, 32)
	if err != nil || parsed > 0o7777 {
		return nil, fmt.Errorf("%s must be an octal mode such as 0664", key)
	}
	mode := uint32(parsed)
	return &mode, nil
}

func mergeOwnershipPolicy(base, override agent.OwnershipPolicy) agent.OwnershipPolicy {
	if override.UID != nil {
		base.UID = override.UID
	}
	if override.GID != nil {
		base.GID = override.GID
	}
	if override.FileMode != nil {
		base.FileMode = override.FileMode
	}
	if override.DirMode != nil {
		base.DirMode = override.DirMode
	}
	return base
}

func ownershipSignature(policy agent.OwnershipPolicy) string {
	parts := []string{
		"uid=" + optionalInt64String(policy.UID),
		"gid=" + optionalInt64String(policy.GID),
		"fileMode=" + optionalModeString(policy.FileMode),
		"dirMode=" + optionalModeString(policy.DirMode),
	}
	return strings.Join(parts, ",")
}

func optionalInt64String(value *int64) string {
	if value == nil {
		return ""
	}
	return strconv.FormatInt(*value, 10)
}

func optionalModeString(value *uint32) string {
	if value == nil {
		return ""
	}
	return strconv.FormatUint(uint64(*value), 8)
}

func int64Ptr(value int64) *int64 {
	return &value
}

func intPtr(value int) *int {
	return &value
}
