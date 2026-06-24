package agent

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const PoolMarkerFile = ".simple-volume-pool"

var (
	ErrPathOutsidePool = errors.New("volume path escapes storage pool")
	ErrNonEmptyPool    = errors.New("storage pool is non-empty and not initialized")
)

type Pool struct {
	Name string
	Path string
}

type VolumePath struct {
	Pool       Pool
	Namespace  string
	Name       string
	Generation string
}

func (p Pool) Clean() Pool {
	return Pool{Name: p.Name, Path: filepath.Clean(p.Path)}
}

func EnsurePool(pool Pool, allowNonEmpty bool) error {
	clean := pool.Clean()
	if clean.Name == "" {
		return fmt.Errorf("pool name is required")
	}
	if clean.Path == "." || clean.Path == string(filepath.Separator) || clean.Path == "" {
		return fmt.Errorf("invalid pool path %q", pool.Path)
	}
	if err := os.MkdirAll(clean.Path, 0o755); err != nil {
		return err
	}

	marker := filepath.Join(clean.Path, PoolMarkerFile)
	if _, err := os.Stat(marker); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}

	entries, err := os.ReadDir(clean.Path)
	if err != nil {
		return err
	}
	if len(entries) > 0 && !allowNonEmpty {
		return fmt.Errorf("%w: %s has %d entries and no %s marker", ErrNonEmptyPool, clean.Path, len(entries), PoolMarkerFile)
	}

	content := fmt.Sprintf("name=%s\npath=%s\n", clean.Name, clean.Path)
	return os.WriteFile(marker, []byte(content), 0o644)
}

func ResolveVolumePath(v VolumePath) (string, error) {
	pool := v.Pool.Clean()
	if pool.Path == "." || pool.Path == string(filepath.Separator) || pool.Path == "" {
		return "", fmt.Errorf("invalid pool path %q", v.Pool.Path)
	}
	rel := filepath.Join(safeSegment(v.Namespace), safeSegment(v.Name))
	full := filepath.Clean(filepath.Join(pool.Path, rel))
	if full != pool.Path && !strings.HasPrefix(full, pool.Path+string(filepath.Separator)) {
		return "", ErrPathOutsidePool
	}
	return full, nil
}

func EnsureVolumePath(v VolumePath, perm os.FileMode) (string, error) {
	path, err := ResolveVolumePath(v)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(path, perm); err != nil {
		return "", err
	}
	return path, nil
}

func ShadowSourceBasePath(namespace, volume string) string {
	return filepath.ToSlash(filepath.Join(shadowRootDir, safeSegment(namespace), safeSegment(volume), shadowCurrentDir, shadowDataDir))
}

func ResolveShadowPath(pool Pool, namespace, volume string) (string, error) {
	return resolvePoolRelativePath(pool, ShadowSourceBasePath(namespace, volume))
}

func EnsureShadowPath(pool Pool, namespace, volume string) (string, error) {
	path, err := ResolveShadowPath(pool, namespace, volume)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		return "", err
	}
	return path, nil
}

func resolvePoolRelativePath(pool Pool, rel string) (string, error) {
	cleanPool := pool.Clean()
	if cleanPool.Path == "." || cleanPool.Path == string(filepath.Separator) || cleanPool.Path == "" {
		return "", fmt.Errorf("invalid pool path %q", pool.Path)
	}
	rel = strings.Trim(strings.ReplaceAll(rel, "\\", "/"), "/")
	if rel == "" {
		return "", fmt.Errorf("relative path is required")
	}
	full := filepath.Clean(filepath.Join(cleanPool.Path, filepath.FromSlash(rel)))
	if full != cleanPool.Path && !strings.HasPrefix(full, cleanPool.Path+string(filepath.Separator)) {
		return "", ErrPathOutsidePool
	}
	return full, nil
}

func BackupVolumePath(v VolumePath, now time.Time) (string, error) {
	path, err := ResolveVolumePath(v)
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return "", nil
	} else if err != nil {
		return "", err
	}
	empty, err := IsEmptyDir(path)
	if err != nil {
		return "", err
	}
	if empty {
		return "", nil
	}
	pool := v.Pool.Clean()
	backupRoot := filepath.Join(pool.Path, ".simple-volume-backups", safeSegment(v.Namespace), safeSegment(v.Name))
	if err := os.MkdirAll(backupRoot, 0o755); err != nil {
		return "", err
	}
	backupPath := filepath.Join(backupRoot, now.UTC().Format("20060102T150405.000000000Z"))
	if err := os.Rename(path, backupPath); err != nil {
		return "", err
	}
	return backupPath, nil
}

type SyncRequest struct {
	Method     string
	SourceURL  string
	TargetPath string
	Excludes   []string
	Delete     bool
}

func BuildSyncCommand(req SyncRequest) (string, []string, error) {
	switch req.Method {
	case "", "rsync":
		args := []string{"-a"}
		if req.Delete {
			args = append(args, "--delete")
		}
		for _, exclude := range req.Excludes {
			args = append(args, "--exclude", exclude)
		}
		args = append(args, req.SourceURL, req.TargetPath)
		return "rsync", args, nil
	case "rclone":
		args := []string{"sync", req.SourceURL, req.TargetPath}
		for _, exclude := range req.Excludes {
			args = append(args, "--exclude", exclude)
		}
		return "rclone", args, nil
	default:
		return "", nil, fmt.Errorf("unsupported sync method %q", req.Method)
	}
}

type TokenAuthorizer struct {
	Token string
}

func (a TokenAuthorizer) Authorize(r *http.Request) bool {
	if a.Token == "" {
		return false
	}
	header := r.Header.Get("Authorization")
	return header == "Bearer "+a.Token
}

func safeSegment(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, "..", "")
	s = strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z':
			return r
		case r >= 'A' && r <= 'Z':
			return r
		case r >= '0' && r <= '9':
			return r
		case r == '-', r == '_', r == '.':
			return r
		default:
			return '-'
		}
	}, s)
	s = strings.Trim(s, ".-")
	if s == "" {
		return "default"
	}
	return s
}

func IsEmptyDir(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()
	_, err = f.Readdirnames(1)
	if errors.Is(err, io.EOF) {
		return true, nil
	}
	return false, err
}
