package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
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

type OutputRunner interface {
	RunOutput(ctx context.Context, spec CommandSpec) ([]byte, error)
}

type ExecRunner struct{}

func (ExecRunner) Run(ctx context.Context, spec CommandSpec) error {
	cmd := exec.CommandContext(ctx, spec.Name, spec.Args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func (ExecRunner) RunOutput(ctx context.Context, spec CommandSpec) ([]byte, error) {
	cmd := exec.CommandContext(ctx, spec.Name, spec.Args...)
	output := &limitedOutput{limit: 64 * 1024}
	cmd.Stdout = output
	cmd.Stderr = output
	err := cmd.Run()
	return output.Bytes(), err
}

type VolumeLocker struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

func NewVolumeLocker() *VolumeLocker {
	return &VolumeLocker{locks: make(map[string]*sync.Mutex)}
}

func (l *VolumeLocker) Lock(namespace, volume string) func() {
	if l == nil {
		return func() {}
	}
	key := safeSegment(namespace) + "/" + safeSegment(volume)
	l.mu.Lock()
	lock := l.locks[key]
	if lock == nil {
		lock = &sync.Mutex{}
		l.locks[key] = lock
	}
	l.mu.Unlock()
	lock.Lock()
	return lock.Unlock
}

type CommandError struct {
	Err    error
	Output []byte
}

func (e CommandError) Error() string {
	if len(e.Output) == 0 {
		return e.Err.Error()
	}
	return strings.TrimSpace(string(e.Output)) + ": " + e.Err.Error()
}

func (e CommandError) Unwrap() error {
	return e.Err
}

type sourceExistsResult int

const (
	sourceExistsUnknown sourceExistsResult = iota
	sourceExistsYes
	sourceExistsNo
)

type limitedOutput struct {
	buf       bytes.Buffer
	limit     int
	truncated int
}

func (o *limitedOutput) Write(p []byte) (int, error) {
	if o.limit <= 0 {
		o.truncated += len(p)
		return len(p), nil
	}
	remaining := o.limit - o.buf.Len()
	if remaining > 0 {
		if len(p) <= remaining {
			_, _ = o.buf.Write(p)
		} else {
			_, _ = o.buf.Write(p[:remaining])
			o.truncated += len(p) - remaining
		}
	} else {
		o.truncated += len(p)
	}
	return len(p), nil
}

func (o *limitedOutput) Bytes() []byte {
	if o.truncated == 0 {
		return o.buf.Bytes()
	}
	out := append([]byte(nil), o.buf.Bytes()...)
	out = append(out, fmt.Sprintf("\n... truncated %d bytes of command output ...\n", o.truncated)...)
	return out
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
	args = appendRcloneLowMemoryArgs(args)
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
	args = appendRcloneLowMemoryArgs(args)
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

func appendRcloneLowMemoryArgs(args []string) []string {
	return append(args,
		"--transfers", "1",
		"--checkers", "1",
		"--buffer-size", "0",
		"--multi-thread-streams", "0",
	)
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
			exists, err := sourcePathExists(ctx, batch.Source, sourcePath)
			if err != nil {
				return err
			}
			if exists == sourceExistsNo {
				if err := os.RemoveAll(targetPath); err != nil {
					return err
				}
				continue
			}
			spec, err := BuildRcloneCopyToCommand(batch.Source, sourcePath, targetPath)
			if err != nil {
				return err
			}
			if err := runEventCopy(ctx, runner, spec); err != nil {
				exists, existsErr := sourcePathExists(ctx, batch.Source, sourcePath)
				if existsErr != nil {
					return err
				}
				if exists == sourceExistsNo || isMissingSourceCopyError(err) {
					if removeErr := os.RemoveAll(targetPath); removeErr != nil {
						return removeErr
					}
					continue
				}
				return err
			}
		}
	}
	return nil
}

func runEventCopy(ctx context.Context, runner Runner, spec CommandSpec) error {
	outputRunner, ok := runner.(OutputRunner)
	if !ok {
		return runner.Run(ctx, spec)
	}
	output, err := outputRunner.RunOutput(ctx, spec)
	if err == nil {
		if len(output) > 0 {
			_, _ = os.Stdout.Write(output)
		}
		return nil
	}
	if len(output) > 0 && !isMissingSourceOutput(output) {
		_, _ = os.Stderr.Write(output)
	}
	return CommandError{Err: err, Output: output}
}

func isMissingSourceCopyError(err error) bool {
	var commandErr CommandError
	if !errors.As(err, &commandErr) {
		return false
	}
	return isMissingSourceOutput(commandErr.Output)
}

func isMissingSourceOutput(output []byte) bool {
	normalized := bytes.ToLower(output)
	if !bytes.Contains(normalized, []byte("not found")) && !bytes.Contains(normalized, []byte("directory not found")) {
		return false
	}
	return bytes.Contains(normalized, []byte("error reading source")) ||
		bytes.Contains(normalized, []byte("failed to open source")) ||
		bytes.Contains(normalized, []byte("webdav root"))
}

func sourcePathExists(ctx context.Context, source SourceRef, sourcePath string) (sourceExistsResult, error) {
	if strings.TrimSpace(source.WebDAVURL) == "" {
		return sourceExistsUnknown, nil
	}
	base, err := url.Parse(strings.TrimRight(source.WebDAVURL, "/") + "/")
	if err != nil {
		return sourceExistsUnknown, err
	}
	rel, err := url.Parse(path.Join(strings.Trim(sourcePath, "/")))
	if err != nil {
		return sourceExistsUnknown, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, base.ResolveReference(rel).String(), nil)
	if err != nil {
		return sourceExistsUnknown, err
	}
	if source.User != "" || source.Password != "" {
		req.SetBasicAuth(source.User, source.Password)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return sourceExistsUnknown, nil
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK, http.StatusNoContent, http.StatusPartialContent:
		return sourceExistsYes, nil
	case http.StatusNotFound, http.StatusGone:
		return sourceExistsNo, nil
	default:
		return sourceExistsUnknown, nil
	}
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
	return runFullSyncCommand(ctx, runner, spec)
}

func runFullSyncCommand(ctx context.Context, runner Runner, spec CommandSpec) error {
	outputRunner, ok := runner.(OutputRunner)
	if !ok {
		return runner.Run(ctx, spec)
	}
	output, err := outputRunner.RunOutput(ctx, spec)
	if err == nil {
		return nil
	}
	if len(output) > 0 {
		_, _ = os.Stderr.Write(output)
	}
	return CommandError{Err: err, Output: output}
}

func SyncBatchHandler(pool Pool, auth TokenAuthorizer, runner Runner, timeout time.Duration, lockers ...*VolumeLocker) http.HandlerFunc {
	if runner == nil {
		runner = ExecRunner{}
	}
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	var locker *VolumeLocker
	if len(lockers) > 0 {
		locker = lockers[0]
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
		unlock := locker.Lock(batch.Namespace, batch.Volume)
		defer unlock()
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

func FullSyncHandler(pool Pool, auth TokenAuthorizer, runner Runner, timeout time.Duration, lockers ...*VolumeLocker) http.HandlerFunc {
	if runner == nil {
		runner = ExecRunner{}
	}
	if timeout <= 0 {
		timeout = time.Hour
	}
	var locker *VolumeLocker
	if len(lockers) > 0 {
		locker = lockers[0]
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
		unlock := locker.Lock(req.Namespace, req.Volume)
		defer unlock()
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
