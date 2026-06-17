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
	AnnotationReplicationEnabled = LabelPrefix + "/replication-enabled"
	AnnotationIncludePaths       = LabelPrefix + "/replication-include-paths"
	AnnotationExcludePaths       = LabelPrefix + "/replication-exclude-paths"
	AnnotationDebounce           = LabelPrefix + "/replication-debounce"
	AnnotationFullSyncOnStart    = LabelPrefix + "/replication-full-sync-on-start"
	AnnotationFullSyncSchedule   = LabelPrefix + "/replication-full-sync-schedule"
)

type ReplicationControllerConfig struct {
	Namespace          string
	StorageClassName   string
	TokenSecretName    string
	TokenSecretKey     string
	ReconcileInterval  time.Duration
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
}

type DesiredTarget struct {
	Node string
	Ref  agent.TargetRef
}

type DesiredReplication struct {
	Namespace    string
	ClaimName    string
	Volume       string
	ActiveNode   string
	SourceURL    string
	Targets      []DesiredTarget
	IncludePaths []string
	ExcludePaths []string
	Debounce     string
	FullSync     bool
	FullSchedule string
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
		if err := c.failover.Reconcile(ctx, now); err != nil {
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
		activePod, ok, err := c.activePodForPVC(ctx, pvc.Namespace, pvc.Name)
		if err != nil {
			return nil, err
		}
		if !ok {
			log.Printf("replication pvc %s/%s has no running active pod", pvc.Namespace, pvc.Name)
			continue
		}
		source, ok := agents[activePod.Spec.NodeName]
		if !ok {
			log.Printf("replication pvc %s/%s active node %s has no ready agent", pvc.Namespace, pvc.Name, activePod.Spec.NodeName)
			continue
		}
		targets := make([]DesiredTarget, 0, len(agents)-1)
		for node, pod := range agents {
			if node == activePod.Spec.NodeName {
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
			Namespace:    pvc.Namespace,
			ClaimName:    pvc.Name,
			Volume:       pvc.Spec.VolumeName,
			ActiveNode:   activePod.Spec.NodeName,
			SourceURL:    agentWebDAVURL(source.Status.PodIP),
			Targets:      targets,
			IncludePaths: csvAnnotation(pvc.Annotations[AnnotationIncludePaths]),
			ExcludePaths: csvAnnotation(pvc.Annotations[AnnotationExcludePaths]),
			Debounce:     strings.TrimSpace(pvc.Annotations[AnnotationDebounce]),
			FullSync:     truthy(pvc.Annotations[AnnotationFullSyncOnStart]),
			FullSchedule: strings.TrimSpace(pvc.Annotations[AnnotationFullSyncSchedule]),
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
	signature := desired.signature()
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
		if err := c.startWatch(ctx, desired, token); err != nil {
			return err
		}
		c.mu.Lock()
		c.startedWatches[key] = signature
		c.mu.Unlock()
		log.Printf("started replication watch namespace=%s claim=%s volume=%s activeNode=%s targets=%d",
			desired.Namespace, desired.ClaimName, desired.Volume, desired.ActiveNode, len(desired.Targets))
	}

	if desired.FullSync {
		syncKey := key + ":" + signature
		c.mu.Lock()
		done := c.completedSyncs[syncKey]
		c.mu.Unlock()
		if !done && c.tryStartFullSync(syncKey) {
			go c.runFullSync(ctx, syncKey, desired, token, now, func() {
				c.mu.Lock()
				c.completedSyncs[syncKey] = true
				c.mu.Unlock()
				log.Printf("completed startup full sync namespace=%s claim=%s volume=%s targets=%d",
					desired.Namespace, desired.ClaimName, desired.Volume, len(desired.Targets))
			})
		}
	}

	if shouldRunScheduledFullSync(desired.FullSchedule, now) {
		scheduleKey := key + ":" + desired.FullSchedule
		today := now.Format("2006-01-02")
		c.mu.Lock()
		last := c.lastScheduledOn[scheduleKey]
		c.mu.Unlock()
		syncKey := scheduleKey + ":" + today
		if last != today && c.tryStartFullSync(syncKey) {
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
		Namespace:    desired.Namespace,
		Volume:       desired.Volume,
		Source:       agent.SourceRef{WebDAVURL: desired.SourceURL},
		Targets:      targetRefs(desired.Targets),
		IncludePaths: desired.IncludePaths,
		ExcludePaths: desired.ExcludePaths,
		Debounce:     desired.Debounce,
	}
	return c.postJSON(ctx, strings.TrimRight(agentHTTPURLFromWebDAV(desired.SourceURL), "/")+"/replication/watch/start", token, req)
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

func (c *ReplicationController) fullSyncTargets(ctx context.Context, desired DesiredReplication, token string, now time.Time) error {
	var errs []string
	for _, target := range desired.Targets {
		backupExisting := c.failover.ShouldBackupBeforeRestore(desired.Namespace, desired.Volume, target.Node)
		req := agent.FullSyncRequest{
			Namespace:      desired.Namespace,
			Volume:         desired.Volume,
			Source:         agent.SourceRef{WebDAVURL: desired.SourceURL},
			IncludePaths:   desired.IncludePaths,
			ExcludePaths:   desired.ExcludePaths,
			BackupExisting: backupExisting,
		}
		if err := c.postJSON(ctx, strings.TrimRight(target.Ref.URL, "/")+"/replication/full-sync", token, req); err != nil {
			errs = append(errs, err.Error())
			continue
		}
		c.failover.RecordReplicaFreshness(desired.Namespace, desired.Volume, target.Node, now, true)
		if backupExisting {
			c.failover.MarkRestored(desired.Namespace, desired.Volume, target.Node)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("full sync failed: %s", strings.Join(errs, "; "))
	}
	return nil
}

func (c *ReplicationController) postJSON(ctx context.Context, url, token string, payload any) error {
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
	b.WriteString(d.Debounce)
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
