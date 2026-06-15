package controller

import (
	"errors"
	"sort"
	"time"

	"github.com/shipstuff/simple-volume/internal/api/v1alpha1"
)

var (
	ErrAutoPromotionDisabled = errors.New("automatic promotion is disabled")
	ErrGracePeriodActive     = errors.New("not-ready grace period has not elapsed")
	ErrNoFreshReplica        = errors.New("no fresh healthy replica is available")
)

type PromotionInput struct {
	Spec                   v1alpha1.SimpleVolumeSpec
	Status                 v1alpha1.SimpleVolumeStatus
	Now                    time.Time
	ActiveUnavailableSince *time.Time
}

type PromotionDecision struct {
	Promote    bool
	TargetNode string
	Reason     string
}

func SelectPromotionTarget(input PromotionInput) (PromotionDecision, error) {
	if !input.Spec.Promotion.Automatic {
		return PromotionDecision{Reason: "AutomaticDisabled"}, ErrAutoPromotionDisabled
	}
	if input.ActiveUnavailableSince == nil {
		return PromotionDecision{Reason: "ActiveStillAvailable"}, ErrGracePeriodActive
	}
	grace := input.Spec.Promotion.NotReadyGracePeriod
	if grace > 0 && input.Now.Sub(*input.ActiveUnavailableSince) < grace {
		return PromotionDecision{Reason: "GracePeriodActive"}, ErrGracePeriodActive
	}

	maxStaleness := input.Spec.Promotion.MaxStaleness
	candidates := make([]v1alpha1.ReplicaStatus, 0, len(input.Status.Replicas))
	for _, replica := range input.Status.Replicas {
		if replica.Node == "" || replica.Node == input.Status.ActiveNode || !replica.Healthy || replica.LastSuccessfulSync == nil {
			continue
		}
		if maxStaleness > 0 && input.Now.Sub(*replica.LastSuccessfulSync) > maxStaleness {
			continue
		}
		candidates = append(candidates, replica)
	}
	if len(candidates) == 0 {
		return PromotionDecision{Reason: "NoFreshReplica"}, ErrNoFreshReplica
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].LastSuccessfulSync.After(*candidates[j].LastSuccessfulSync)
	})
	return PromotionDecision{Promote: true, TargetNode: candidates[0].Node, Reason: "FreshReplicaAvailable"}, nil
}
