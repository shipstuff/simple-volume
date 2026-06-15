package csi

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

var (
	ErrNodeNotAuthorized = errors.New("node is not authorized to mount volume")
	ErrInvalidMountPath  = errors.New("invalid mount path")
)

type MountAuthorizer interface {
	Allowed(volumeHandle, nodeName string) (string, bool)
}

type StaticAuthorizer map[string]map[string]string

func (s StaticAuthorizer) Allowed(volumeHandle, nodeName string) (string, bool) {
	nodes := s[volumeHandle]
	if nodes == nil {
		return "", false
	}
	path, ok := nodes[nodeName]
	return path, ok
}

type NodePublisher struct {
	NodeName   string
	Authorize  MountAuthorizer
	BindMount  func(source, target string) error
	MakeTarget func(target string) error
}

type PublishRequest struct {
	VolumeHandle string
	TargetPath   string
	ReadOnly     bool
}

func (p NodePublisher) Publish(req PublishRequest) error {
	if req.VolumeHandle == "" {
		return fmt.Errorf("volume handle is required")
	}
	target := filepath.Clean(req.TargetPath)
	if target == "." || target == string(filepath.Separator) || target == "" {
		return ErrInvalidMountPath
	}
	source, ok := p.Authorize.Allowed(req.VolumeHandle, p.NodeName)
	if !ok {
		return ErrNodeNotAuthorized
	}
	if p.MakeTarget != nil {
		if err := p.MakeTarget(target); err != nil {
			return err
		}
	} else if err := os.MkdirAll(target, 0o755); err != nil {
		return err
	}
	if p.BindMount == nil {
		return nil
	}
	return p.BindMount(source, target)
}
