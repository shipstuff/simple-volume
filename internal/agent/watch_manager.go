package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

type TargetRef struct {
	URL   string `json:"url"`
	Token string `json:"token,omitempty"`
}

type WatchStartRequest struct {
	Namespace    string      `json:"namespace,omitempty"`
	Volume       string      `json:"volume"`
	Source       SourceRef   `json:"source"`
	Targets      []TargetRef `json:"targets"`
	IncludePaths []string    `json:"includePaths,omitempty"`
	ExcludePaths []string    `json:"excludePaths,omitempty"`
	Debounce     string      `json:"debounce,omitempty"`
}

type WatchStopRequest struct {
	Namespace string `json:"namespace,omitempty"`
	Volume    string `json:"volume"`
}

type WatchStatus struct {
	Namespace            string         `json:"namespace,omitempty"`
	Volume               string         `json:"volume"`
	Source               SourceRef      `json:"source"`
	Targets              []TargetRef    `json:"targets"`
	TargetStatuses       []TargetStatus `json:"targetStatuses,omitempty"`
	IncludePaths         []string       `json:"includePaths,omitempty"`
	ExcludePaths         []string       `json:"excludePaths,omitempty"`
	Running              bool           `json:"running"`
	StartedAt            time.Time      `json:"startedAt"`
	StoppedAt            *time.Time     `json:"stoppedAt,omitempty"`
	LastBatchAt          *time.Time     `json:"lastBatchAt,omitempty"`
	LastBatchEventCount  int            `json:"lastBatchEventCount,omitempty"`
	LastBatchGeneration  string         `json:"lastBatchGeneration,omitempty"`
	LastDeliveryError    string         `json:"lastDeliveryError,omitempty"`
	LastWatchError       string         `json:"lastWatchError,omitempty"`
	DeliveredBatchCount  int64          `json:"deliveredBatchCount,omitempty"`
	DeliveredEventCount  int64          `json:"deliveredEventCount,omitempty"`
	DeliveryFailureCount int64          `json:"deliveryFailureCount,omitempty"`
}

type TargetStatus struct {
	URL                    string     `json:"url"`
	LastSuccessfulSync     *time.Time `json:"lastSuccessfulSync,omitempty"`
	LastObservedGeneration string     `json:"lastObservedGeneration,omitempty"`
	LastError              string     `json:"lastError,omitempty"`
	DeliveredBatchCount    int64      `json:"deliveredBatchCount,omitempty"`
	DeliveredEventCount    int64      `json:"deliveredEventCount,omitempty"`
	DeliveryFailureCount   int64      `json:"deliveryFailureCount,omitempty"`
}

type BatchSender interface {
	SendBatch(context.Context, TargetRef, EventBatch) error
}

type HTTPBatchSender struct {
	Client *http.Client
}

func (s HTTPBatchSender) SendBatch(ctx context.Context, target TargetRef, batch EventBatch) error {
	client := s.Client
	if client == nil {
		client = http.DefaultClient
	}
	if strings.TrimSpace(target.URL) == "" {
		return fmt.Errorf("target url is required")
	}
	body, err := json.Marshal(batch)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(target.URL, "/")+"/replication/sync-batch", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if target.Token != "" {
		req.Header.Set("Authorization", "Bearer "+target.Token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		responseBody, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("target %s returned %s: %s", target.URL, resp.Status, strings.TrimSpace(string(responseBody)))
	}
	return nil
}

type WatchManager struct {
	pool   Pool
	sender BatchSender

	mu      sync.Mutex
	watches map[string]*managedWatch
}

type managedWatch struct {
	cancel context.CancelFunc
	status WatchStatus
}

func NewWatchManager(pool Pool, sender BatchSender) *WatchManager {
	if sender == nil {
		sender = HTTPBatchSender{Client: &http.Client{Timeout: 10 * time.Minute}}
	}
	return &WatchManager{
		pool:    pool,
		sender:  sender,
		watches: make(map[string]*managedWatch),
	}
}

func (m *WatchManager) Start(ctx context.Context, req WatchStartRequest) (WatchStatus, error) {
	if req.Volume == "" {
		return WatchStatus{}, fmt.Errorf("volume is required")
	}
	if req.Source.WebDAVURL == "" {
		return WatchStatus{}, fmt.Errorf("source.webdavUrl is required")
	}
	if len(req.Targets) == 0 {
		return WatchStatus{}, fmt.Errorf("at least one target is required")
	}
	for i, target := range req.Targets {
		if strings.TrimSpace(target.URL) == "" {
			return WatchStatus{}, fmt.Errorf("targets[%d].url is required", i)
		}
	}
	debounce, err := parseOptionalDuration(req.Debounce, 5*time.Second)
	if err != nil {
		return WatchStatus{}, fmt.Errorf("debounce: %w", err)
	}

	key := watchKey(req.Namespace, req.Volume)
	watchCtx, cancel := context.WithCancel(context.Background())
	status := WatchStatus{
		Namespace:      req.Namespace,
		Volume:         req.Volume,
		Source:         req.Source,
		Targets:        append([]TargetRef(nil), req.Targets...),
		TargetStatuses: initialTargetStatuses(req.Targets),
		IncludePaths:   append([]string(nil), req.IncludePaths...),
		ExcludePaths:   append([]string(nil), req.ExcludePaths...),
		Running:        true,
		StartedAt:      time.Now().UTC(),
	}

	m.mu.Lock()
	if existing := m.watches[key]; existing != nil {
		existing.cancel()
	}
	watch := &managedWatch{cancel: cancel, status: status}
	m.watches[key] = watch
	m.mu.Unlock()

	go m.run(watchCtx, key, watch, WatchConfig{
		Pool:         m.pool,
		Namespace:    req.Namespace,
		Volume:       req.Volume,
		IncludePaths: req.IncludePaths,
		ExcludePaths: req.ExcludePaths,
		Debounce:     debounce,
	}, req.Source, append([]TargetRef(nil), req.Targets...))

	return status, nil
}

func (m *WatchManager) Stop(namespace, volume string) (WatchStatus, bool) {
	key := watchKey(namespace, volume)
	m.mu.Lock()
	defer m.mu.Unlock()
	watch, ok := m.watches[key]
	if !ok {
		return WatchStatus{}, false
	}
	watch.cancel()
	now := time.Now().UTC()
	watch.status.Running = false
	watch.status.StoppedAt = &now
	return watch.status, true
}

func (m *WatchManager) Statuses() []WatchStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	statuses := make([]WatchStatus, 0, len(m.watches))
	for _, watch := range m.watches {
		statuses = append(statuses, cloneWatchStatus(watch.status))
	}
	return statuses
}

func (m *WatchManager) Status(namespace, volume string) (WatchStatus, bool) {
	key := watchKey(namespace, volume)
	m.mu.Lock()
	defer m.mu.Unlock()
	watch, ok := m.watches[key]
	if !ok {
		return WatchStatus{}, false
	}
	return cloneWatchStatus(watch.status), true
}

func (m *WatchManager) StartHandler(auth TokenAuthorizer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !auth.Authorize(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var req WatchStartRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		status, err := m.Start(r.Context(), req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(status)
	}
}

func (m *WatchManager) StopHandler(auth TokenAuthorizer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !auth.Authorize(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var req WatchStopRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		status, ok := m.Stop(req.Namespace, req.Volume)
		if !ok {
			http.Error(w, "watch not found", http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(status)
	}
}

func (m *WatchManager) StatusHandler(auth TokenAuthorizer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !auth.Authorize(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		namespace := r.URL.Query().Get("namespace")
		volume := r.URL.Query().Get("volume")
		if volume != "" {
			status, ok := m.Status(namespace, volume)
			if !ok {
				http.Error(w, "watch not found", http.StatusNotFound)
				return
			}
			_ = json.NewEncoder(w).Encode(status)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"watches": m.Statuses()})
	}
}

func (m *WatchManager) run(ctx context.Context, key string, watch *managedWatch, cfg WatchConfig, source SourceRef, targets []TargetRef) {
	err := WatchVolume(ctx, cfg, func(ctx context.Context, batch EventBatch) error {
		batch.Source = source
		for _, target := range targets {
			if err := m.sender.SendBatch(ctx, target, batch); err != nil {
				m.recordDeliveryError(key, watch, target, err)
				continue
			}
			m.recordDeliverySuccess(key, watch, target, batch)
		}
		return nil
	})
	m.recordStopped(key, watch, err)
}

func (m *WatchManager) recordDeliverySuccess(key string, watch *managedWatch, target TargetRef, batch EventBatch) {
	now := time.Now().UTC()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.watches[key] != watch {
		return
	}
	watch.status.LastBatchAt = &now
	watch.status.LastBatchEventCount = len(batch.Events)
	watch.status.LastBatchGeneration = batch.Generation
	watch.status.DeliveredBatchCount++
	watch.status.DeliveredEventCount += int64(len(batch.Events))
	watch.status.LastDeliveryError = ""
	targetStatus := ensureTargetStatus(&watch.status, target.URL)
	targetStatus.LastSuccessfulSync = &now
	targetStatus.LastObservedGeneration = batch.Generation
	targetStatus.DeliveredBatchCount++
	targetStatus.DeliveredEventCount += int64(len(batch.Events))
	targetStatus.LastError = ""
}

func (m *WatchManager) recordDeliveryError(key string, watch *managedWatch, target TargetRef, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.watches[key] != watch {
		return
	}
	watch.status.DeliveryFailureCount++
	watch.status.LastDeliveryError = err.Error()
	targetStatus := ensureTargetStatus(&watch.status, target.URL)
	targetStatus.DeliveryFailureCount++
	targetStatus.LastError = err.Error()
}

func (m *WatchManager) recordStopped(key string, watch *managedWatch, err error) {
	now := time.Now().UTC()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.watches[key] != watch {
		return
	}
	watch.status.Running = false
	watch.status.StoppedAt = &now
	if err != nil && err != context.Canceled {
		watch.status.LastWatchError = err.Error()
	}
}

func watchKey(namespace, volume string) string {
	return namespace + "/" + volume
}

func parseOptionalDuration(value string, fallback time.Duration) (time.Duration, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, err
	}
	return duration, nil
}

func cloneWatchStatus(status WatchStatus) WatchStatus {
	status.Targets = append([]TargetRef(nil), status.Targets...)
	status.TargetStatuses = append([]TargetStatus(nil), status.TargetStatuses...)
	status.IncludePaths = append([]string(nil), status.IncludePaths...)
	status.ExcludePaths = append([]string(nil), status.ExcludePaths...)
	return status
}

func initialTargetStatuses(targets []TargetRef) []TargetStatus {
	statuses := make([]TargetStatus, 0, len(targets))
	for _, target := range targets {
		statuses = append(statuses, TargetStatus{URL: target.URL})
	}
	return statuses
}

func ensureTargetStatus(status *WatchStatus, targetURL string) *TargetStatus {
	for i := range status.TargetStatuses {
		if status.TargetStatuses[i].URL == targetURL {
			return &status.TargetStatuses[i]
		}
	}
	status.TargetStatuses = append(status.TargetStatuses, TargetStatus{URL: targetURL})
	return &status.TargetStatuses[len(status.TargetStatuses)-1]
}
