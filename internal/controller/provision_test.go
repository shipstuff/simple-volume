package controller

import "testing"

func TestSelectInitialPlacementUsesHealthyPoolAgents(t *testing.T) {
	decision, err := SelectInitialPlacement(ProvisionRequest{
		Namespace:    "games",
		Name:         "demo",
		StoragePool:  "default",
		ReplicaCount: 2,
	}, []AgentStatus{
		{Node: "sf-west-1", Pool: "default", Healthy: true, FreeBytes: 100},
		{Node: "fresno-west-1", Pool: "default", Healthy: true, FreeBytes: 200},
		{Node: "kapolei", Pool: "archive", Healthy: true, FreeBytes: 300},
	})
	if err != nil {
		t.Fatalf("SelectInitialPlacement returned error: %v", err)
	}
	if decision.VolumeHandle != "games/demo" {
		t.Fatalf("handle = %q", decision.VolumeHandle)
	}
	if decision.ActiveNode != "fresno-west-1" {
		t.Fatalf("active = %q", decision.ActiveNode)
	}
	if len(decision.ReplicaNodes) != 2 {
		t.Fatalf("replicas = %#v", decision.ReplicaNodes)
	}
}
