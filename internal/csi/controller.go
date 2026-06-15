package csi

import (
	"fmt"

	"github.com/shipstuff/simple-volume/internal/controller"
)

type CreateVolumeRequest struct {
	Name          string
	Namespace     string
	StoragePool   string
	CapacityBytes int64
	ReplicaCount  int
}

type CreateVolumeResponse struct {
	VolumeHandle  string
	CapacityBytes int64
	ActiveNode    string
	ReplicaNodes  []string
}

func CreateVolume(req CreateVolumeRequest, agents []controller.AgentStatus) (CreateVolumeResponse, error) {
	if req.Name == "" || req.Namespace == "" {
		return CreateVolumeResponse{}, fmt.Errorf("namespace and name are required")
	}
	decision, err := controller.SelectInitialPlacement(controller.ProvisionRequest{
		Namespace:    req.Namespace,
		Name:         req.Name,
		StoragePool:  req.StoragePool,
		SizeBytes:    req.CapacityBytes,
		ReplicaCount: req.ReplicaCount,
	}, agents)
	if err != nil {
		return CreateVolumeResponse{}, err
	}
	return CreateVolumeResponse{
		VolumeHandle:  decision.VolumeHandle,
		CapacityBytes: req.CapacityBytes,
		ActiveNode:    decision.ActiveNode,
		ReplicaNodes:  decision.ReplicaNodes,
	}, nil
}
