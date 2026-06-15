package controller

import (
	"errors"
	"testing"
	"time"

	"github.com/shipstuff/simple-volume/internal/api/v1alpha1"
)

func TestSelectPromotionTargetChoosesFreshestReplica(t *testing.T) {
	now := time.Date(2026, 6, 14, 12, 0, 0, 0, time.UTC)
	unavailable := now.Add(-3 * time.Minute)
	older := now.Add(-10 * time.Minute)
	fresher := now.Add(-2 * time.Minute)

	decision, err := SelectPromotionTarget(PromotionInput{
		Spec: v1alpha1.SimpleVolumeSpec{Promotion: v1alpha1.PromotionSpec{
			Automatic:           true,
			NotReadyGracePeriod: time.Minute,
			MaxStaleness:        15 * time.Minute,
		}},
		Status: v1alpha1.SimpleVolumeStatus{
			ActiveNode: "sf-west-1",
			Replicas: []v1alpha1.ReplicaStatus{
				{Node: "sf-west-1", Role: v1alpha1.ReplicaRoleActive, Healthy: false},
				{Node: "fresno-west-1", Healthy: true, LastSuccessfulSync: &older},
				{Node: "kapolei-pacific-1", Healthy: true, LastSuccessfulSync: &fresher},
			},
		},
		Now:                    now,
		ActiveUnavailableSince: &unavailable,
	})
	if err != nil {
		t.Fatalf("SelectPromotionTarget returned error: %v", err)
	}
	if !decision.Promote || decision.TargetNode != "kapolei-pacific-1" {
		t.Fatalf("decision = %+v", decision)
	}
}

func TestSelectPromotionTargetBlocksStaleReplica(t *testing.T) {
	now := time.Date(2026, 6, 14, 12, 0, 0, 0, time.UTC)
	unavailable := now.Add(-3 * time.Minute)
	stale := now.Add(-20 * time.Minute)

	_, err := SelectPromotionTarget(PromotionInput{
		Spec: v1alpha1.SimpleVolumeSpec{Promotion: v1alpha1.PromotionSpec{
			Automatic:           true,
			NotReadyGracePeriod: time.Minute,
			MaxStaleness:        15 * time.Minute,
		}},
		Status: v1alpha1.SimpleVolumeStatus{
			ActiveNode: "sf-west-1",
			Replicas: []v1alpha1.ReplicaStatus{
				{Node: "fresno-west-1", Healthy: true, LastSuccessfulSync: &stale},
			},
		},
		Now:                    now,
		ActiveUnavailableSince: &unavailable,
	})
	if !errors.Is(err, ErrNoFreshReplica) {
		t.Fatalf("err = %v, want ErrNoFreshReplica", err)
	}
}
