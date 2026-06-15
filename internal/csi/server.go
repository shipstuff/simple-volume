package csi

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/shipstuff/simple-volume/internal/agent"
	"github.com/shipstuff/simple-volume/internal/api/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type ServerConfig struct {
	Endpoint          string
	NodeName          string
	PoolName          string
	PoolPath          string
	ValidatePool      bool
	AllowNonEmptyPool bool
}

type Server struct {
	csipb.UnimplementedIdentityServer
	csipb.UnimplementedControllerServer
	csipb.UnimplementedNodeServer

	nodeName string
	poolName string
	poolPath string
}

func RunServer(ctx context.Context, cfg ServerConfig) error {
	if cfg.ValidatePool {
		if err := agent.EnsurePool(agent.Pool{Name: defaultString(cfg.PoolName, "default"), Path: defaultString(cfg.PoolPath, "/var/lib/simple-volume")}, cfg.AllowNonEmptyPool); err != nil {
			return err
		}
	}
	network, address, err := parseEndpoint(cfg.Endpoint)
	if err != nil {
		return err
	}
	if network == "unix" {
		_ = os.Remove(address)
		if err := os.MkdirAll(filepath.Dir(address), 0o755); err != nil {
			return err
		}
	}
	listener, err := net.Listen(network, address)
	if err != nil {
		return err
	}
	defer listener.Close()

	server := grpc.NewServer()
	impl := &Server{
		nodeName: cfg.NodeName,
		poolName: defaultString(cfg.PoolName, "default"),
		poolPath: defaultString(cfg.PoolPath, "/var/lib/simple-volume"),
	}
	csipb.RegisterIdentityServer(server, impl)
	csipb.RegisterControllerServer(server, impl)
	csipb.RegisterNodeServer(server, impl)

	go func() {
		<-ctx.Done()
		server.GracefulStop()
	}()
	return server.Serve(listener)
}

func (s *Server) GetPluginInfo(context.Context, *csipb.GetPluginInfoRequest) (*csipb.GetPluginInfoResponse, error) {
	return &csipb.GetPluginInfoResponse{
		Name:          v1alpha1.DriverName,
		VendorVersion: "0.1.0",
	}, nil
}

func (s *Server) Probe(context.Context, *csipb.ProbeRequest) (*csipb.ProbeResponse, error) {
	return &csipb.ProbeResponse{Ready: wrapperspb.Bool(true)}, nil
}

func (s *Server) GetPluginCapabilities(context.Context, *csipb.GetPluginCapabilitiesRequest) (*csipb.GetPluginCapabilitiesResponse, error) {
	return &csipb.GetPluginCapabilitiesResponse{
		Capabilities: []*csipb.PluginCapability{
			{
				Type: &csipb.PluginCapability_Service_{
					Service: &csipb.PluginCapability_Service{
						Type: csipb.PluginCapability_Service_CONTROLLER_SERVICE,
					},
				},
			},
		},
	}, nil
}

func (s *Server) ControllerGetCapabilities(context.Context, *csipb.ControllerGetCapabilitiesRequest) (*csipb.ControllerGetCapabilitiesResponse, error) {
	return &csipb.ControllerGetCapabilitiesResponse{
		Capabilities: []*csipb.ControllerServiceCapability{
			controllerCapability(csipb.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME),
		},
	}, nil
}

func (s *Server) CreateVolume(_ context.Context, req *csipb.CreateVolumeRequest) (*csipb.CreateVolumeResponse, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	capacity := int64(0)
	if req.GetCapacityRange() != nil {
		capacity = req.GetCapacityRange().GetRequiredBytes()
	}
	return &csipb.CreateVolumeResponse{
		Volume: &csipb.Volume{
			VolumeId:      req.GetName(),
			CapacityBytes: capacity,
			VolumeContext: map[string]string{
				"storagePool": s.poolName,
			},
		},
	}, nil
}

func (s *Server) DeleteVolume(context.Context, *csipb.DeleteVolumeRequest) (*csipb.DeleteVolumeResponse, error) {
	return &csipb.DeleteVolumeResponse{}, nil
}

func (s *Server) ValidateVolumeCapabilities(context.Context, *csipb.ValidateVolumeCapabilitiesRequest) (*csipb.ValidateVolumeCapabilitiesResponse, error) {
	return &csipb.ValidateVolumeCapabilitiesResponse{Confirmed: &csipb.ValidateVolumeCapabilitiesResponse_Confirmed{}}, nil
}

func (s *Server) NodeGetInfo(context.Context, *csipb.NodeGetInfoRequest) (*csipb.NodeGetInfoResponse, error) {
	if s.nodeName == "" {
		return nil, status.Error(codes.FailedPrecondition, "node name is required")
	}
	return &csipb.NodeGetInfoResponse{NodeId: s.nodeName}, nil
}

func (s *Server) NodeGetCapabilities(context.Context, *csipb.NodeGetCapabilitiesRequest) (*csipb.NodeGetCapabilitiesResponse, error) {
	return &csipb.NodeGetCapabilitiesResponse{
		Capabilities: []*csipb.NodeServiceCapability{
			nodeCapability(csipb.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME),
		},
	}, nil
}

func (s *Server) NodeStageVolume(context.Context, *csipb.NodeStageVolumeRequest) (*csipb.NodeStageVolumeResponse, error) {
	return &csipb.NodeStageVolumeResponse{}, nil
}

func (s *Server) NodeUnstageVolume(context.Context, *csipb.NodeUnstageVolumeRequest) (*csipb.NodeUnstageVolumeResponse, error) {
	return &csipb.NodeUnstageVolumeResponse{}, nil
}

func (s *Server) NodePublishVolume(_ context.Context, req *csipb.NodePublishVolumeRequest) (*csipb.NodePublishVolumeResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id is required")
	}
	if req.GetTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "target path is required")
	}
	source, err := s.volumePath(req.GetVolumeId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	publisher := NodePublisher{
		NodeName: s.nodeName,
		Authorize: StaticAuthorizer{
			req.GetVolumeId(): {s.nodeName: source},
		},
		BindMount: bindMount,
	}
	if err := publisher.Publish(PublishRequest{
		VolumeHandle: req.GetVolumeId(),
		TargetPath:   req.GetTargetPath(),
		ReadOnly:     req.GetReadonly(),
	}); err != nil {
		if err == ErrNodeNotAuthorized {
			return nil, status.Error(codes.PermissionDenied, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &csipb.NodePublishVolumeResponse{}, nil
}

func (s *Server) NodeUnpublishVolume(_ context.Context, req *csipb.NodeUnpublishVolumeRequest) (*csipb.NodeUnpublishVolumeResponse, error) {
	if req.GetTargetPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "target path is required")
	}
	if err := syscall.Unmount(req.GetTargetPath(), 0); err != nil && !os.IsNotExist(err) {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &csipb.NodeUnpublishVolumeResponse{}, nil
}

func (s *Server) volumePath(volumeID string) (string, error) {
	return agent.EnsureVolumePath(agent.VolumePath{
		Pool: agent.Pool{Name: s.poolName, Path: s.poolPath},
		Name: volumeID,
	}, 0o755)
}

func controllerCapability(capability csipb.ControllerServiceCapability_RPC_Type) *csipb.ControllerServiceCapability {
	return &csipb.ControllerServiceCapability{
		Type: &csipb.ControllerServiceCapability_Rpc{
			Rpc: &csipb.ControllerServiceCapability_RPC{Type: capability},
		},
	}
}

func nodeCapability(capability csipb.NodeServiceCapability_RPC_Type) *csipb.NodeServiceCapability {
	return &csipb.NodeServiceCapability{
		Type: &csipb.NodeServiceCapability_Rpc{
			Rpc: &csipb.NodeServiceCapability_RPC{Type: capability},
		},
	}
}

func bindMount(source, target string) error {
	if err := os.MkdirAll(source, 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(target, 0o755); err != nil {
		return err
	}
	return syscall.Mount(source, target, "", syscall.MS_BIND, "")
}

func parseEndpoint(endpoint string) (string, string, error) {
	if endpoint == "" {
		return "", "", fmt.Errorf("endpoint is required")
	}
	if strings.HasPrefix(endpoint, "unix://") {
		return "unix", strings.TrimPrefix(endpoint, "unix://"), nil
	}
	if strings.HasPrefix(endpoint, "tcp://") {
		return "tcp", strings.TrimPrefix(endpoint, "tcp://"), nil
	}
	return "", "", fmt.Errorf("unsupported endpoint %q", endpoint)
}

func defaultString(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}
