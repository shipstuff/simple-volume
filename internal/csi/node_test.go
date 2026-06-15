package csi

import (
	"errors"
	"testing"
)

func TestNodePublisherAllowsAuthorizedNode(t *testing.T) {
	var gotSource, gotTarget string
	p := NodePublisher{
		NodeName: "sf-west-1",
		Authorize: StaticAuthorizer{
			"games/demo": {"sf-west-1": "/pool/games/demo"},
		},
		MakeTarget: func(target string) error { return nil },
		BindMount: func(source, target string) error {
			gotSource = source
			gotTarget = target
			return nil
		},
	}
	if err := p.Publish(PublishRequest{VolumeHandle: "games/demo", TargetPath: "/var/lib/kubelet/pods/x"}); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}
	if gotSource != "/pool/games/demo" || gotTarget != "/var/lib/kubelet/pods/x" {
		t.Fatalf("mount = %q -> %q", gotSource, gotTarget)
	}
}

func TestNodePublisherRejectsUnauthorizedNode(t *testing.T) {
	p := NodePublisher{
		NodeName: "fresno-west-1",
		Authorize: StaticAuthorizer{
			"games/demo": {"sf-west-1": "/pool/games/demo"},
		},
	}
	err := p.Publish(PublishRequest{VolumeHandle: "games/demo", TargetPath: "/target"})
	if !errors.Is(err, ErrNodeNotAuthorized) {
		t.Fatalf("err = %v, want ErrNodeNotAuthorized", err)
	}
}
