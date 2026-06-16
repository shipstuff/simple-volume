package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type EventOp string

const (
	EventOpUpsert EventOp = "upsert"
	EventOpDelete EventOp = "delete"
)

type FileEvent struct {
	Path string  `json:"path"`
	Op   EventOp `json:"op"`
}

type EventBatch struct {
	Namespace  string      `json:"namespace,omitempty"`
	Volume     string      `json:"volume"`
	Generation string      `json:"generation,omitempty"`
	Source     SourceRef   `json:"source"`
	Events     []FileEvent `json:"events"`
}

type SourceRef struct {
	WebDAVURL string `json:"webdavUrl"`
	User      string `json:"user,omitempty"`
	Password  string `json:"password,omitempty"`
}

type FullSyncRequest struct {
	Namespace      string    `json:"namespace,omitempty"`
	Volume         string    `json:"volume"`
	Source         SourceRef `json:"source"`
	IncludePaths   []string  `json:"includePaths,omitempty"`
	ExcludePaths   []string  `json:"excludePaths,omitempty"`
	BackupExisting bool      `json:"backupExisting,omitempty"`
}

type PathFilter struct {
	IncludePaths []string
	ExcludePaths []string
}

type CommandSpec struct {
	Name string
	Args []string
}

type Runner interface {
	Run(ctx context.Context, spec CommandSpec) error
}

type ExecRunner struct{}

func (ExecRunner) Run(ctx context.Context, spec CommandSpec) error {
	cmd := exec.CommandContext(ctx, spec.Name, spec.Args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func NormalizeEventPath(p string) (string, error) {
	p = strings.TrimSpace(strings.ReplaceAll(p, "\\", "/"))
	p = strings.TrimPrefix(p, "/")
	clean := path.Clean(p)
	if clean == "." || clean == "" {
		return "", fmt.Errorf("empty event path")
	}
	if strings.HasPrefix(clean, "../") || clean == ".." {
		return "", fmt.Errorf("event path escapes volume: %q", p)
	}
	return clean, nil
}

func (f PathFilter) ShouldReplicate(p string) bool {
	clean, err := NormalizeEventPath(p)
	if err != nil {
		return false
	}
	for _, exclude := range f.ExcludePaths {
		if matchesPath(exclude, clean) {
			return false
		}
	}
	if len(f.IncludePaths) == 0 {
		return true
	}
	for _, include := range f.IncludePaths {
		if matchesPath(include, clean) {
			return true
		}
	}
	return false
}

func (f PathFilter) ShouldTraverse(p string) bool {
	clean, err := NormalizeEventPath(p)
	if err != nil {
		return false
	}
	for _, exclude := range f.ExcludePaths {
		if matchesPath(exclude, clean) {
			return false
		}
	}
	if len(f.IncludePaths) == 0 {
		return true
	}
	for _, include := range f.IncludePaths {
		if matchesPath(include, clean) {
			return true
		}
		root := filterStaticPrefix(include)
		if root != "" && isPathAncestor(clean, root) {
			return true
		}
	}
	return false
}

func CoalesceEvents(events []FileEvent, filter PathFilter) []FileEvent {
	byPath := make(map[string]FileEvent)
	for _, event := range events {
		clean, err := NormalizeEventPath(event.Path)
		if err != nil || !filter.ShouldReplicate(clean) {
			continue
		}
		op := event.Op
		if op == "" {
			op = EventOpUpsert
		}
		byPath[clean] = FileEvent{Path: clean, Op: op}
	}
	paths := make([]string, 0, len(byPath))
	for p := range byPath {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	out := make([]FileEvent, 0, len(paths))
	for _, p := range paths {
		out = append(out, byPath[p])
	}
	return out
}

func BuildRcloneServeWebDAVCommand(rootPath, addr string, readOnly bool) CommandSpec {
	args := []string{"serve", "webdav", rootPath, "--config", "/dev/null", "--addr", addr, "--dir-cache-time", "1s"}
	if readOnly {
		args = append(args, "--read-only")
	}
	return CommandSpec{Name: "rclone", Args: args}
}

func BuildRcloneCopyToCommand(source SourceRef, sourcePath, targetPath string) (CommandSpec, error) {
	clean, err := NormalizeEventPath(sourcePath)
	if err != nil {
		return CommandSpec{}, err
	}
	args := []string{
		"copyto",
		"--config", "/dev/null",
		"--webdav-url", strings.TrimRight(source.WebDAVURL, "/"),
		"--webdav-vendor", "other",
	}
	if source.User != "" {
		args = append(args, "--webdav-user", source.User)
	}
	if source.Password != "" {
		args = append(args, "--webdav-pass", source.Password)
	}
	args = append(args, ":webdav:"+clean, targetPath)
	return CommandSpec{Name: "rclone", Args: args}, nil
}

func BuildRcloneFullSyncCommand(source SourceRef, sourceVolume, targetRoot string, filter PathFilter) CommandSpec {
	args := []string{
		"sync",
		"--config", "/dev/null",
		"--webdav-url", strings.TrimRight(source.WebDAVURL, "/"),
		"--webdav-vendor", "other",
	}
	if source.User != "" {
		args = append(args, "--webdav-user", source.User)
	}
	if source.Password != "" {
		args = append(args, "--webdav-pass", source.Password)
	}
	for _, exclude := range filter.ExcludePaths {
		args = append(args, "--filter", "- "+normalizeFilterPattern(exclude))
	}
	for _, include := range filter.IncludePaths {
		args = append(args, "--filter", "+ "+normalizeFilterPattern(include))
	}
	if len(filter.IncludePaths) > 0 {
		args = append(args, "--filter", "- **")
	}
	args = append(args, ":webdav:"+strings.Trim(sourceVolume, "/"), targetRoot)
	return CommandSpec{Name: "rclone", Args: args}
}

func ApplyEventBatch(ctx context.Context, runner Runner, pool Pool, batch EventBatch) error {
	targetRoot, err := EnsureVolumePath(VolumePath{
		Pool:      pool,
		Namespace: batch.Namespace,
		Name:      batch.Volume,
	}, 0o755)
	if err != nil {
		return err
	}
	for _, event := range CoalesceEvents(batch.Events, PathFilter{}) {
		target := filepath.Join(targetRoot, filepath.FromSlash(event.Path))
		switch event.Op {
		case EventOpDelete:
			if err := os.RemoveAll(target); err != nil {
				return err
			}
		default:
			sourcePath := path.Join(safeSegment(batch.Namespace), safeSegment(batch.Volume), event.Path)
			targetPath := filepath.Join(targetRoot, filepath.FromSlash(event.Path))
			spec, err := BuildRcloneCopyToCommand(batch.Source, sourcePath, targetPath)
			if err != nil {
				return err
			}
			if err := runner.Run(ctx, spec); err != nil {
				return err
			}
		}
	}
	return nil
}

func ApplyFullSync(ctx context.Context, runner Runner, pool Pool, req FullSyncRequest) error {
	if runner == nil {
		runner = ExecRunner{}
	}
	if req.Volume == "" {
		return fmt.Errorf("volume is required")
	}
	if req.Source.WebDAVURL == "" {
		return fmt.Errorf("source.webdavUrl is required")
	}
	volumePath := VolumePath{
		Pool:      pool,
		Namespace: req.Namespace,
		Name:      req.Volume,
	}
	if req.BackupExisting {
		if _, err := BackupVolumePath(volumePath, time.Now().UTC()); err != nil {
			return err
		}
	}
	targetRoot, err := EnsureVolumePath(volumePath, 0o755)
	if err != nil {
		return err
	}
	sourceVolume := path.Join(safeSegment(req.Namespace), safeSegment(req.Volume))
	spec := BuildRcloneFullSyncCommand(req.Source, sourceVolume, targetRoot, PathFilter{
		IncludePaths: req.IncludePaths,
		ExcludePaths: req.ExcludePaths,
	})
	return runner.Run(ctx, spec)
}

func SyncBatchHandler(pool Pool, auth TokenAuthorizer, runner Runner, timeout time.Duration) http.HandlerFunc {
	if runner == nil {
		runner = ExecRunner{}
	}
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !auth.Authorize(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var batch EventBatch
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		if err := ApplyEventBatch(ctx, runner, pool, batch); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"volume": batch.Volume,
			"events": len(batch.Events),
			"ok":     true,
		})
	}
}

func FullSyncHandler(pool Pool, auth TokenAuthorizer, runner Runner, timeout time.Duration) http.HandlerFunc {
	if runner == nil {
		runner = ExecRunner{}
	}
	if timeout <= 0 {
		timeout = time.Hour
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !auth.Authorize(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var req FullSyncRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		if err := ApplyFullSync(ctx, runner, pool, req); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"volume": req.Volume,
			"ok":     true,
		})
	}
}

func matchesPath(pattern, p string) bool {
	pattern = strings.Trim(strings.ReplaceAll(pattern, "\\", "/"), "/")
	if pattern == "" {
		return false
	}
	if strings.HasSuffix(pattern, "/**") {
		prefix := strings.TrimSuffix(pattern, "/**")
		return p == prefix || strings.HasPrefix(p, prefix+"/")
	}
	if strings.HasSuffix(pattern, "/") {
		return strings.HasPrefix(p, strings.TrimSuffix(pattern, "/")+"/")
	}
	if ok, _ := path.Match(pattern, p); ok {
		return true
	}
	return p == pattern || strings.HasPrefix(p, pattern+"/")
}

func filterStaticPrefix(pattern string) string {
	pattern = strings.Trim(strings.ReplaceAll(pattern, "\\", "/"), "/")
	if pattern == "" {
		return ""
	}
	cut := len(pattern)
	for _, marker := range []string{"*", "?", "["} {
		if idx := strings.Index(pattern, marker); idx >= 0 && idx < cut {
			cut = idx
		}
	}
	prefix := strings.TrimSuffix(pattern[:cut], "/")
	if slash := strings.LastIndex(prefix, "/"); slash >= 0 && cut < len(pattern) {
		prefix = prefix[:slash]
	}
	return strings.Trim(prefix, "/")
}

func isPathAncestor(candidate, descendant string) bool {
	candidate = strings.Trim(candidate, "/")
	descendant = strings.Trim(descendant, "/")
	if candidate == "" {
		return true
	}
	return candidate == descendant || strings.HasPrefix(descendant, candidate+"/")
}

func normalizeFilterPattern(pattern string) string {
	pattern = strings.Trim(strings.ReplaceAll(pattern, "\\", "/"), "/")
	if pattern == "" {
		return "/**"
	}
	if strings.HasSuffix(pattern, "/**") {
		return "/" + pattern
	}
	if strings.HasSuffix(pattern, "/") {
		return "/" + strings.TrimSuffix(pattern, "/") + "/**"
	}
	return "/" + pattern
}
