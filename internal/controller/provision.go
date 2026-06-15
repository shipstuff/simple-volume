package controller

import (
	"errors"
	"fmt"
	"sort"

	"github.com/shipstuff/simple-volume/internal/api/v1alpha1"
)

var ErrNoEligibleAgents = errors.New("no healthy agents are eligible for the requested pool")

type AgentStatus struct {
	Node       string
	Pool       string
	Healthy    bool
	FreeBytes  int64
	PoolLabels map[string]string
}

type ProvisionRequest struct {
	Namespace    string
	Name         string
	StoragePool  string
	SizeBytes    int64
	ReplicaCount int
}

type ProvisionDecision struct {
	VolumeHandle string
	ActiveNode   string
	ReplicaNodes []string
}

func SelectInitialPlacement(req ProvisionRequest, agents []AgentStatus) (ProvisionDecision, error) {
	var eligible []AgentStatus
	for _, agent := range agents {
		if !agent.Healthy || agent.Node == "" {
			continue
		}
		if req.StoragePool != "" && agent.Pool != req.StoragePool {
			continue
		}
		if req.SizeBytes > 0 && agent.FreeBytes > 0 && agent.FreeBytes < req.SizeBytes {
			continue
		}
		eligible = append(eligible, agent)
	}
	if len(eligible) == 0 {
		return ProvisionDecision{}, ErrNoEligibleAgents
	}
	sort.SliceStable(eligible, func(i, j int) bool {
		if eligible[i].FreeBytes == eligible[j].FreeBytes {
			return eligible[i].Node < eligible[j].Node
		}
		return eligible[i].FreeBytes > eligible[j].FreeBytes
	})

	replicaCount := req.ReplicaCount
	if replicaCount <= 0 || replicaCount > len(eligible) {
		replicaCount = len(eligible)
	}
	replicas := make([]string, 0, replicaCount)
	for _, agent := range eligible[:replicaCount] {
		replicas = append(replicas, agent.Node)
	}

	return ProvisionDecision{
		VolumeHandle: fmt.Sprintf("%s/%s", req.Namespace, req.Name),
		ActiveNode:   replicas[0],
		ReplicaNodes: replicas,
	}, nil
}

func NewVolumeSpecFromProvision(req ProvisionRequest) v1alpha1.SimpleVolumeSpec {
	return v1alpha1.SimpleVolumeSpec{
		StoragePool: req.StoragePool,
		SizeBytes:   req.SizeBytes,
		Replica: v1alpha1.ReplicaSpec{
			Method:       v1alpha1.SyncMethodRsync,
			ReplicaCount: req.ReplicaCount,
		},
	}
}
