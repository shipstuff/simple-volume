package csi

import (
	"testing"

	"github.com/shipstuff/simple-volume/internal/controller"
)

func TestCreateVolumeReturnsLogicalHandle(t *testing.T) {
	resp, err := CreateVolume(CreateVolumeRequest{
		Namespace:     "games",
		Name:          "demo",
		StoragePool:   "default",
		CapacityBytes: 1024,
		ReplicaCount:  1,
	}, []controller.AgentStatus{
		{Node: "sf-west-1", Pool: "default", Healthy: true, FreeBytes: 2048},
	})
	if err != nil {
		t.Fatalf("CreateVolume returned error: %v", err)
	}
	if resp.VolumeHandle != "games/demo" {
		t.Fatalf("handle = %q", resp.VolumeHandle)
	}
	if resp.ActiveNode != "sf-west-1" {
		t.Fatalf("active = %q", resp.ActiveNode)
	}
}
