package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"time"

	"github.com/shipstuff/simple-volume/internal/agent"
	"github.com/shipstuff/simple-volume/internal/api/v1alpha1"
	simplecsi "github.com/shipstuff/simple-volume/internal/csi"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "controller":
		runController(os.Args[2:])
	case "csi-controller":
		runCSIController(os.Args[2:])
	case "agent":
		runAgent(os.Args[2:])
	case "csi-node":
		runCSINode(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func runController(args []string) {
	fs := flag.NewFlagSet("controller", flag.ExitOnError)
	addr := fs.String("http", ":8080", "health and metrics listen address")
	_ = fs.Parse(args)
	log.Printf("starting simple-volume controller scaffold driver=%s addr=%s", v1alpha1.DriverName, *addr)
	serveHealth(*addr)
}

func runAgent(args []string) {
	fs := flag.NewFlagSet("agent", flag.ExitOnError)
	addr := fs.String("http", ":8080", "agent listen address")
	poolName := fs.String("pool-name", getenv("SIMPLE_VOLUME_POOL_NAME", "default"), "storage pool name")
	poolPath := fs.String("pool-path", getenv("SIMPLE_VOLUME_POOL_PATH", "/var/lib/simple-volume"), "host-mounted storage pool path")
	allowNonEmptyPool := fs.Bool("allow-non-empty-pool", getenv("SIMPLE_VOLUME_ALLOW_NON_EMPTY_POOL", "") == "true", "allow adopting a non-empty uninitialized storage pool")
	webdavEnabled := fs.Bool("webdav-enabled", getenv("SIMPLE_VOLUME_WEBDAV_ENABLED", "true") == "true", "serve the pool root over read-only rclone WebDAV")
	webdavAddr := fs.String("webdav-addr", getenv("SIMPLE_VOLUME_WEBDAV_ADDR", ":8081"), "rclone WebDAV listen address")
	token := fs.String("token", os.Getenv("SIMPLE_VOLUME_TOKEN"), "bearer token for sync endpoints")
	_ = fs.Parse(args)

	if err := agent.EnsurePool(agent.Pool{Name: *poolName, Path: *poolPath}, *allowNonEmptyPool); err != nil {
		log.Fatalf("initialize storage pool: %v", err)
	}
	if *webdavEnabled {
		startBackgroundCommand(context.Background(), agent.BuildRcloneServeWebDAVCommand(*poolPath, *webdavAddr, true))
	}

	auth := agent.TokenAuthorizer{Token: *token}
	pool := agent.Pool{Name: *poolName, Path: *poolPath}
	watchManager := agent.NewWatchManager(pool, nil)
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("/pool", func(w http.ResponseWriter, r *http.Request) {
		if !auth.Authorize(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		_, _ = fmt.Fprintf(w, "name=%s\npath=%s\n", *poolName, *poolPath)
	})
	mux.HandleFunc("/volumes/prepare", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !auth.Authorize(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var req struct {
			Namespace string `json:"namespace"`
			Name      string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		path, err := agent.EnsureVolumePath(agent.VolumePath{
			Pool:      agent.Pool{Name: *poolName, Path: *poolPath},
			Namespace: req.Namespace,
			Name:      req.Name,
		}, 0o755)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"path": path})
	})
	mux.HandleFunc("/replication/sync-batch", agent.SyncBatchHandler(pool, auth, agent.ExecRunner{}, 10*time.Minute))
	mux.HandleFunc("/replication/watch/start", watchManager.StartHandler(auth))
	mux.HandleFunc("/replication/watch/stop", watchManager.StopHandler(auth))
	mux.HandleFunc("/replication/watch/status", watchManager.StatusHandler(auth))
	log.Printf("starting simple-volume agent pool=%s path=%s addr=%s", *poolName, *poolPath, *addr)
	log.Fatal(http.ListenAndServe(*addr, mux))
}

func startBackgroundCommand(ctx context.Context, spec agent.CommandSpec) {
	cmd := exec.CommandContext(ctx, spec.Name, spec.Args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		log.Fatalf("start %s: %v", spec.Name, err)
	}
	go func() {
		if err := cmd.Wait(); err != nil {
			log.Fatalf("%s exited: %v", spec.Name, err)
		}
	}()
}

func runCSINode(args []string) {
	fs := flag.NewFlagSet("csi-node", flag.ExitOnError)
	endpoint := fs.String("endpoint", getenv("CSI_ENDPOINT", "unix:///csi/csi.sock"), "CSI endpoint")
	node := fs.String("node-name", os.Getenv("NODE_NAME"), "node name")
	poolName := fs.String("pool-name", getenv("SIMPLE_VOLUME_POOL_NAME", "default"), "storage pool name")
	poolPath := fs.String("pool-path", getenv("SIMPLE_VOLUME_POOL_PATH", "/var/lib/simple-volume"), "storage pool path")
	allowNonEmptyPool := fs.Bool("allow-non-empty-pool", getenv("SIMPLE_VOLUME_ALLOW_NON_EMPTY_POOL", "") == "true", "allow adopting a non-empty uninitialized storage pool")
	_ = fs.Parse(args)
	log.Printf("starting simple-volume CSI node scaffold driver=%s endpoint=%s node=%s", v1alpha1.DriverName, *endpoint, *node)
	log.Fatal(simplecsi.RunServer(context.Background(), simplecsi.ServerConfig{
		Endpoint:          *endpoint,
		NodeName:          *node,
		PoolName:          *poolName,
		PoolPath:          *poolPath,
		ValidatePool:      true,
		AllowNonEmptyPool: *allowNonEmptyPool,
	}))
}

func runCSIController(args []string) {
	fs := flag.NewFlagSet("csi-controller", flag.ExitOnError)
	endpoint := fs.String("endpoint", getenv("CSI_ENDPOINT", "unix:///csi/csi.sock"), "CSI endpoint")
	poolName := fs.String("pool-name", getenv("SIMPLE_VOLUME_POOL_NAME", "default"), "storage pool name")
	poolPath := fs.String("pool-path", getenv("SIMPLE_VOLUME_POOL_PATH", "/var/lib/simple-volume"), "storage pool path")
	_ = fs.Parse(args)
	log.Printf("starting simple-volume CSI controller scaffold driver=%s endpoint=%s pool=%s", v1alpha1.DriverName, *endpoint, *poolName)
	log.Fatal(simplecsi.RunServer(context.Background(), simplecsi.ServerConfig{
		Endpoint: *endpoint,
		PoolName: *poolName,
		PoolPath: *poolPath,
		NodeName: "controller",
	}))
}

func serveHealth(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	log.Fatal(http.ListenAndServe(addr, mux))
}

func usage() {
	fmt.Fprintf(os.Stderr, "usage: simple-volume <controller|csi-controller|agent|csi-node> [flags]\n")
}

func getenv(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
