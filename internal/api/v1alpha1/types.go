package v1alpha1

import "time"

const (
	Group   = "storage.shipstuff.io"
	Version = "v1alpha1"

	DriverName = "simple-volume.shipstuff.io"
)

type VolumePhase string

const (
	VolumePhasePending          VolumePhase = "Pending"
	VolumePhaseReady            VolumePhase = "Ready"
	VolumePhasePromotionPending VolumePhase = "PromotionPending"
	VolumePhasePromotionBlocked VolumePhase = "PromotionBlocked"
	VolumePhaseDegraded         VolumePhase = "Degraded"
)

type ReplicaRole string

const (
	ReplicaRoleActive  ReplicaRole = "active"
	ReplicaRoleReplica ReplicaRole = "replica"
	ReplicaRoleStale   ReplicaRole = "stale"
)

type SimpleVolumeSpec struct {
	StoragePool string          `json:"storagePool,omitempty"`
	SizeBytes   int64           `json:"sizeBytes,omitempty"`
	Replica     ReplicaSpec     `json:"replica,omitempty"`
	Sync        SyncSpec        `json:"sync,omitempty"`
	Promotion   PromotionSpec   `json:"promotion,omitempty"`
	Access      AccessSpec      `json:"access,omitempty"`
	WorkloadRef *WorkloadRef    `json:"workloadRef,omitempty"`
	Constraints *NodeConstraint `json:"constraints,omitempty"`
}

type ReplicaSpec struct {
	Method       SyncMethod `json:"method,omitempty"`
	Excludes     []string   `json:"excludes,omitempty"`
	ReplicaCount int        `json:"replicaCount,omitempty"`
}

type SyncMethod string

const (
	SyncMethodRsync  SyncMethod = "rsync"
	SyncMethodRclone SyncMethod = "rclone"
)

type SyncMode string

const (
	SyncModeWatch SyncMode = "watch"
	SyncModeCron  SyncMode = "cron"
)

type SyncSpec struct {
	Mode               SyncMode      `json:"mode,omitempty"`
	IncludePaths       []string      `json:"includePaths,omitempty"`
	ExcludePaths       []string      `json:"excludePaths,omitempty"`
	Debounce           time.Duration `json:"debounce,omitempty"`
	FullResyncSchedule string        `json:"fullResyncSchedule,omitempty"`
}

type PromotionSpec struct {
	Automatic                   bool          `json:"automatic,omitempty"`
	MaxStaleness                time.Duration `json:"maxStaleness,omitempty"`
	NotReadyGracePeriod         time.Duration `json:"notReadyGracePeriod,omitempty"`
	ActiveAgentHeartbeatTimeout time.Duration `json:"activeAgentHeartbeatTimeout,omitempty"`
	PromotionDeadline           time.Duration `json:"promotionDeadline,omitempty"`
}

type AccessSpec struct {
	TokenSecretName string `json:"tokenSecretName,omitempty"`
	TokenSecretKey  string `json:"tokenSecretKey,omitempty"`
}

type WorkloadRef struct {
	APIVersion string `json:"apiVersion,omitempty"`
	Kind       string `json:"kind,omitempty"`
	Name       string `json:"name,omitempty"`
}

type NodeConstraint struct {
	NodeNames     []string          `json:"nodeNames,omitempty"`
	NodeSelector  map[string]string `json:"nodeSelector,omitempty"`
	StoragePool   string            `json:"storagePool,omitempty"`
	AllowDemoOnly bool              `json:"allowDemoOnly,omitempty"`
}

type SimpleVolumeStatus struct {
	Phase              VolumePhase     `json:"phase,omitempty"`
	ActiveNode         string          `json:"activeNode,omitempty"`
	ObservedGeneration string          `json:"observedGeneration,omitempty"`
	Replicas           []ReplicaStatus `json:"replicas,omitempty"`
	Conditions         []Condition     `json:"conditions,omitempty"`
}

type ReplicaStatus struct {
	Node                   string      `json:"node,omitempty"`
	Role                   ReplicaRole `json:"role,omitempty"`
	Healthy                bool        `json:"healthy,omitempty"`
	LastSuccessfulSync     *time.Time  `json:"lastSuccessfulSync,omitempty"`
	LastObservedGeneration string      `json:"lastObservedGeneration,omitempty"`
	BytesSynced            int64       `json:"bytesSynced,omitempty"`
	SyncDuration           string      `json:"syncDuration,omitempty"`
	Error                  string      `json:"error,omitempty"`
}

type Condition struct {
	Type               string    `json:"type,omitempty"`
	Status             string    `json:"status,omitempty"`
	Reason             string    `json:"reason,omitempty"`
	Message            string    `json:"message,omitempty"`
	LastTransitionTime time.Time `json:"lastTransitionTime,omitempty"`
}
