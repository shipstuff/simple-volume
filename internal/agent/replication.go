package agent

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
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
	Namespace            string          `json:"namespace,omitempty"`
	Volume               string          `json:"volume"`
	Generation           string          `json:"generation,omitempty"`
	Source               SourceRef       `json:"source"`
	SourceBasePath       string          `json:"sourceBasePath,omitempty"`
	Ownership            OwnershipPolicy `json:"ownership,omitempty"`
	DeletesAuthoritative bool            `json:"deletesAuthoritative,omitempty"`
	Events               []FileEvent     `json:"events"`
}

const (
	archiveCopyBufferSize    = 256 * 1024
	archiveCacheDropInterval = 1024 * 1024
	requiredPathRepairTries  = 3
	ConsistencyModeLive      = "live"
	ConsistencyModeShadow    = "shadow"
	shadowRootDir            = ".simple-volume-shadows"
	shadowCurrentDir         = "current"
	shadowDataDir            = "data"
	shadowManifestName       = ".simple-volume-generation.json"
)

type SourceRef struct {
	WebDAVURL string `json:"webdavUrl"`
	User      string `json:"user,omitempty"`
	Password  string `json:"password,omitempty"`
}

type FullSyncRequest struct {
	Namespace      string          `json:"namespace,omitempty"`
	Volume         string          `json:"volume"`
	Source         SourceRef       `json:"source"`
	SourceBasePath string          `json:"sourceBasePath,omitempty"`
	IncludePaths   []string        `json:"includePaths,omitempty"`
	ExcludePaths   []string        `json:"excludePaths,omitempty"`
	PruneExcluded  bool            `json:"pruneExcluded,omitempty"`
	RequiredPaths  []string        `json:"requiredPaths,omitempty"`
	Ownership      OwnershipPolicy `json:"ownership,omitempty"`
	BackupExisting bool            `json:"backupExisting,omitempty"`
}

type ShadowPrepareRequest struct {
	Namespace     string   `json:"namespace,omitempty"`
	Volume        string   `json:"volume"`
	IncludePaths  []string `json:"includePaths,omitempty"`
	ExcludePaths  []string `json:"excludePaths,omitempty"`
	PruneExcluded bool     `json:"pruneExcluded,omitempty"`
	RequiredPaths []string `json:"requiredPaths,omitempty"`
}

type ShadowPrepareResponse struct {
	Namespace      string `json:"namespace,omitempty"`
	Volume         string `json:"volume"`
	SourceBasePath string `json:"sourceBasePath"`
	Generation     string `json:"generation"`
	OK             bool   `json:"ok"`
}

type OwnershipPolicy struct {
	UID      *int64  `json:"uid,omitempty"`
	GID      *int64  `json:"gid,omitempty"`
	FileMode *uint32 `json:"fileMode,omitempty"`
	DirMode  *uint32 `json:"dirMode,omitempty"`
}

func (p OwnershipPolicy) Empty() bool {
	return p.UID == nil && p.GID == nil && p.FileMode == nil && p.DirMode == nil
}

type PathFilter struct {
	IncludePaths  []string
	ExcludePaths  []string
	PruneExcluded bool
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
	order := make([]string, 0, len(events))
	position := make(map[string]int)
	for _, event := range events {
		clean, err := NormalizeEventPath(event.Path)
		if err != nil || !filter.ShouldReplicate(clean) {
			continue
		}
		op := event.Op
		if op == "" {
			op = EventOpUpsert
		}
		if idx, ok := position[clean]; ok {
			order = append(order[:idx], order[idx+1:]...)
			for i := idx; i < len(order); i++ {
				position[order[i]] = i
			}
		}
		position[clean] = len(order)
		order = append(order, clean)
		byPath[clean] = FileEvent{Path: clean, Op: op}
	}
	out := make([]FileEvent, 0, len(order))
	for _, p := range order {
		out = append(out, byPath[p])
	}
	return out
}

func BuildRcloneServeWebDAVCommand(rootPath, addr string, readOnly bool) CommandSpec {
	args := []string{"serve", "webdav", rootPath, "--config", "/dev/null", "--addr", addr, "--dir-cache-time", "1s", "--metadata", "--links"}
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
		"--metadata",
		"--links",
		"--ignore-times",
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
		"--metadata",
		"--links",
	}
	if filter.PruneExcluded {
		args = append(args, "--delete-excluded")
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

func BuildRcloneLocalSyncCommand(sourceRoot, targetRoot string, filter PathFilter) CommandSpec {
	args := []string{
		"sync",
		sourceRoot,
		targetRoot,
		"--metadata",
		"--links",
	}
	if filter.PruneExcluded {
		args = append(args, "--delete-excluded")
	}
	args = appendRcloneLowMemoryArgs(args)
	for _, exclude := range filter.ExcludePaths {
		args = append(args, "--filter", "- "+normalizeFilterPattern(exclude))
	}
	for _, include := range filter.IncludePaths {
		args = append(args, "--filter", "+ "+normalizeFilterPattern(include))
	}
	if len(filter.IncludePaths) > 0 {
		args = append(args, "--filter", "- **")
	}
	return CommandSpec{Name: "rclone", Args: args}
}

func BuildRcloneLocalCopyToCommand(sourcePath, targetPath string) CommandSpec {
	args := []string{
		"copyto",
		sourcePath,
		targetPath,
		"--metadata",
		"--links",
		"--ignore-times",
	}
	args = appendRcloneLowMemoryArgs(args)
	return CommandSpec{Name: "rclone", Args: args}
}

func BuildRcloneCheckCommand(source SourceRef, sourcePath, targetPath string) CommandSpec {
	args := []string{
		"check",
		"--one-way",
		"--size-only",
		"--config", "/dev/null",
		"--webdav-url", strings.TrimRight(source.WebDAVURL, "/"),
		"--webdav-vendor", "other",
		"--checkers", "1",
	}
	if source.User != "" {
		args = append(args, "--webdav-user", source.User)
	}
	if source.Password != "" {
		args = append(args, "--webdav-pass", source.Password)
	}
	args = append(args, ":webdav:"+strings.Trim(sourcePath, "/"), targetPath)
	return CommandSpec{Name: "rclone", Args: args}
}

func BuildRclonePathSyncCommand(source SourceRef, sourcePath, targetPath string) CommandSpec {
	args := []string{
		"sync",
		"--config", "/dev/null",
		"--webdav-url", strings.TrimRight(source.WebDAVURL, "/"),
		"--webdav-vendor", "other",
		"--metadata",
		"--links",
		"--ignore-times",
	}
	args = appendRcloneLowMemoryArgs(args)
	if source.User != "" {
		args = append(args, "--webdav-user", source.User)
	}
	if source.Password != "" {
		args = append(args, "--webdav-pass", source.Password)
	}
	args = append(args, ":webdav:"+strings.Trim(sourcePath, "/"), targetPath)
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
		switch event.Op {
		case EventOpDelete:
			if batch.DeletesAuthoritative {
				targetPath := filepath.Join(targetRoot, filepath.FromSlash(event.Path))
				if err := ensurePathInside(targetRoot, targetPath); err != nil {
					return err
				}
				if err := os.RemoveAll(targetPath); err != nil {
					return err
				}
			}
			// Watch deletes are advisory. Full sync owns pruning so a racy
			// delete/create sequence cannot drain the replica when the
			// replacement upsert has already disappeared from the live source.
			continue
		default:
			sourcePath := path.Join(sourceBasePath(batch.Namespace, batch.Volume, batch.SourceBasePath), event.Path)
			targetPath := filepath.Join(targetRoot, filepath.FromSlash(event.Path))
			exists, err := sourcePathExists(ctx, batch.Source, sourcePath)
			if err != nil {
				return err
			}
			if exists == sourceExistsNo {
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
					continue
				}
				return err
			}
			if err := ApplyOwnershipPolicy(targetRoot, targetPath, batch.Ownership, true); err != nil {
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
	return isMissingSourceOutput(commandErr.Output) || isDirectorySourceCopyOutput(commandErr.Output)
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

func isDirectorySourceCopyOutput(output []byte) bool {
	normalized := bytes.ToLower(output)
	return bytes.Contains(normalized, []byte("source doesn't exist or is a directory and destination is a file")) ||
		bytes.Contains(normalized, []byte("source does not exist or is a directory and destination is a file"))
}

func sourcePathExists(ctx context.Context, source SourceRef, sourcePath string) (sourceExistsResult, error) {
	if strings.TrimSpace(source.WebDAVURL) == "" {
		return sourceExistsUnknown, nil
	}
	base, err := url.Parse(strings.TrimRight(source.WebDAVURL, "/") + "/")
	if err != nil {
		return sourceExistsUnknown, err
	}
	candidates := []string{path.Join(strings.Trim(sourcePath, "/"))}
	if !strings.HasSuffix(sourcePath, "/") {
		candidates = append(candidates, path.Join(strings.Trim(sourcePath, "/"))+"/")
	}
	sawMissing := false
	sawUnknown := false
	for _, candidate := range candidates {
		rel, err := url.Parse(candidate)
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
		status := resp.StatusCode
		resp.Body.Close()
		switch status {
		case http.StatusOK, http.StatusNoContent, http.StatusPartialContent:
			return sourceExistsYes, nil
		case http.StatusNotFound, http.StatusGone:
			sawMissing = true
			continue
		case http.StatusMethodNotAllowed:
			sawUnknown = true
			continue
		default:
			return sourceExistsUnknown, nil
		}
	}
	if sawMissing {
		return sourceExistsNo, nil
	}
	if sawUnknown {
		return sourceExistsUnknown, nil
	}
	return sourceExistsUnknown, nil
}

func ApplyFullSync(ctx context.Context, runner Runner, pool Pool, req FullSyncRequest) error {
	return ApplyFullSyncWithToken(ctx, runner, pool, req, "")
}

func ApplyFullSyncWithToken(ctx context.Context, runner Runner, pool Pool, req FullSyncRequest, token string) error {
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
	sourceVolume := sourceBasePath(req.Namespace, req.Volume, req.SourceBasePath)
	if err := validateFullSyncRequiredPaths(ctx, req.Source, sourceVolume, req.RequiredPaths); err != nil {
		return err
	}
	spec := BuildRcloneFullSyncCommand(req.Source, sourceVolume, targetRoot, PathFilter{
		IncludePaths:  req.IncludePaths,
		ExcludePaths:  req.ExcludePaths,
		PruneExcluded: req.PruneExcluded,
	})
	if err := runFullSyncCommand(ctx, runner, spec); err != nil {
		return err
	}
	if err := forceSyncIncludedFiles(ctx, runner, req.Source, sourceVolume, targetRoot, req.IncludePaths); err != nil {
		return err
	}
	if err := validateFullSyncTargetRequiredPaths(ctx, runner, req.Source, sourceVolume, targetRoot, req.RequiredPaths); err != nil {
		return err
	}
	return ApplyOwnershipPolicyForFilter(targetRoot, PathFilter{
		IncludePaths:  req.IncludePaths,
		ExcludePaths:  req.ExcludePaths,
		PruneExcluded: req.PruneExcluded,
	}, req.Ownership)
}

func PrepareShadow(ctx context.Context, runner Runner, pool Pool, req ShadowPrepareRequest) (ShadowPrepareResponse, error) {
	if runner == nil {
		runner = ExecRunner{}
	}
	if req.Volume == "" {
		return ShadowPrepareResponse{}, fmt.Errorf("volume is required")
	}
	liveRoot, err := ResolveVolumePath(VolumePath{Pool: pool, Namespace: req.Namespace, Name: req.Volume})
	if err != nil {
		return ShadowPrepareResponse{}, err
	}
	if _, err := os.Stat(liveRoot); err != nil {
		return ShadowPrepareResponse{}, err
	}
	if err := validateRequiredLocalPaths(liveRoot, req.RequiredPaths); err != nil {
		return ShadowPrepareResponse{}, err
	}
	shadowRoot, err := EnsureShadowPath(pool, req.Namespace, req.Volume)
	if err != nil {
		return ShadowPrepareResponse{}, err
	}
	manifest := filepath.Join(filepath.Dir(shadowRoot), shadowManifestName)
	_ = os.Remove(manifest)
	filter := PathFilter{IncludePaths: req.IncludePaths, ExcludePaths: req.ExcludePaths, PruneExcluded: req.PruneExcluded}
	if err := runFullSyncCommand(ctx, runner, BuildRcloneLocalSyncCommand(liveRoot, shadowRoot, filter)); err != nil {
		return ShadowPrepareResponse{}, err
	}
	if err := validateRequiredLocalPaths(shadowRoot, req.RequiredPaths); err != nil {
		return ShadowPrepareResponse{}, err
	}
	generation := time.Now().UTC().Format(time.RFC3339Nano)
	payload, err := json.MarshalIndent(map[string]any{
		"namespace":  req.Namespace,
		"volume":     req.Volume,
		"generation": generation,
		"createdAt":  generation,
		"mode":       ConsistencyModeShadow,
	}, "", "  ")
	if err != nil {
		return ShadowPrepareResponse{}, err
	}
	if err := os.WriteFile(manifest, append(payload, '\n'), 0o644); err != nil {
		return ShadowPrepareResponse{}, err
	}
	return ShadowPrepareResponse{
		Namespace:      req.Namespace,
		Volume:         req.Volume,
		SourceBasePath: ShadowSourceBasePath(req.Namespace, req.Volume),
		Generation:     generation,
		OK:             true,
	}, nil
}

func ApplyShadowEventBatch(ctx context.Context, runner Runner, pool Pool, batch EventBatch, filter PathFilter) (EventBatch, error) {
	if runner == nil {
		runner = ExecRunner{}
	}
	liveRoot, err := ResolveVolumePath(VolumePath{Pool: pool, Namespace: batch.Namespace, Name: batch.Volume})
	if err != nil {
		return EventBatch{}, err
	}
	shadowRoot, err := EnsureShadowPath(pool, batch.Namespace, batch.Volume)
	if err != nil {
		return EventBatch{}, err
	}
	events := CoalesceEvents(batch.Events, filter)
	outEvents := make([]FileEvent, 0, len(events))
	for _, event := range events {
		sourcePath := filepath.Join(liveRoot, filepath.FromSlash(event.Path))
		shadowPath := filepath.Join(shadowRoot, filepath.FromSlash(event.Path))
		if err := ensurePathInside(liveRoot, sourcePath); err != nil {
			return EventBatch{}, err
		}
		if err := ensurePathInside(shadowRoot, shadowPath); err != nil {
			return EventBatch{}, err
		}
		if event.Op == EventOpDelete {
			if err := os.RemoveAll(shadowPath); err != nil {
				return EventBatch{}, err
			}
			outEvents = append(outEvents, event)
			continue
		}
		info, err := os.Lstat(sourcePath)
		if os.IsNotExist(err) {
			if err := os.RemoveAll(shadowPath); err != nil {
				return EventBatch{}, err
			}
			outEvents = append(outEvents, FileEvent{Path: event.Path, Op: EventOpDelete})
			continue
		}
		if err != nil {
			return EventBatch{}, err
		}
		if info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
			continue
		}
		if err := os.MkdirAll(filepath.Dir(shadowPath), 0o755); err != nil {
			return EventBatch{}, err
		}
		if err := runFullSyncCommand(ctx, runner, BuildRcloneLocalCopyToCommand(sourcePath, shadowPath)); err != nil {
			_, statErr := os.Lstat(sourcePath)
			if os.IsNotExist(statErr) || isMissingSourceCopyError(err) {
				if removeErr := os.RemoveAll(shadowPath); removeErr != nil {
					return EventBatch{}, removeErr
				}
				outEvents = append(outEvents, FileEvent{Path: event.Path, Op: EventOpDelete})
				continue
			}
			return EventBatch{}, err
		}
		outEvents = append(outEvents, FileEvent{Path: event.Path, Op: EventOpUpsert})
	}
	if batch.Generation == "" {
		batch.Generation = time.Now().UTC().Format(time.RFC3339Nano)
	}
	batch.SourceBasePath = ShadowSourceBasePath(batch.Namespace, batch.Volume)
	batch.DeletesAuthoritative = true
	batch.Events = outEvents
	return batch, nil
}

func validateRequiredLocalPaths(root string, requiredPaths []string) error {
	for _, required := range requiredPaths {
		clean, err := NormalizeEventPath(required)
		if err != nil {
			return fmt.Errorf("required path %q: %w", required, err)
		}
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(clean))); err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("required shadow path missing: %s", clean)
			}
			return err
		}
	}
	return nil
}

func sourceBasePath(namespace, volume, override string) string {
	if strings.TrimSpace(override) != "" {
		return strings.Trim(strings.ReplaceAll(override, "\\", "/"), "/")
	}
	return path.Join(safeSegment(namespace), safeSegment(volume))
}

func validateFullSyncRequiredPaths(ctx context.Context, source SourceRef, sourceVolume string, requiredPaths []string) error {
	for _, required := range requiredPaths {
		clean, err := NormalizeEventPath(required)
		if err != nil {
			return fmt.Errorf("required path %q: %w", required, err)
		}
		sourcePath := path.Join(strings.Trim(sourceVolume, "/"), clean)
		exists, err := sourcePathExists(ctx, source, sourcePath)
		if err != nil {
			return fmt.Errorf("check required source path %s: %w", clean, err)
		}
		if exists != sourceExistsYes {
			return fmt.Errorf("required source path missing: %s", clean)
		}
	}
	return nil
}

func validateFullSyncTargetRequiredPaths(ctx context.Context, runner Runner, source SourceRef, sourceVolume, targetRoot string, requiredPaths []string) error {
	for _, required := range requiredPaths {
		clean, err := NormalizeEventPath(required)
		if err != nil {
			return fmt.Errorf("required path %q: %w", required, err)
		}
		targetPath := filepath.Join(targetRoot, filepath.FromSlash(clean))
		sourcePath := path.Join(strings.Trim(sourceVolume, "/"), clean)
		var lastErr error
		for attempt := 1; attempt <= requiredPathRepairTries; attempt++ {
			if err := runFullSyncCommand(ctx, runner, BuildRclonePathSyncCommand(source, sourcePath, targetPath)); err != nil {
				lastErr = fmt.Errorf("required target path repair sync failed %s: %w", clean, err)
				continue
			}
			if _, err := os.Stat(targetPath); err != nil {
				if os.IsNotExist(err) {
					lastErr = fmt.Errorf("required target path missing after full sync: %s", clean)
					continue
				}
				return fmt.Errorf("check required target path %s: %w", clean, err)
			}
			if err := runFullSyncCommand(ctx, runner, BuildRcloneCheckCommand(source, sourcePath, targetPath)); err != nil {
				lastErr = fmt.Errorf("required target path failed post-sync check %s: %w", clean, err)
				continue
			}
			lastErr = nil
			break
		}
		if lastErr != nil {
			return lastErr
		}
	}
	return nil
}

func forceSyncIncludedFiles(ctx context.Context, runner Runner, source SourceRef, sourceVolume, targetRoot string, includePaths []string) error {
	for _, include := range includePaths {
		clean, ok, err := exactIncludePath(include)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		sourcePath := path.Join(strings.Trim(sourceVolume, "/"), clean)
		exists, err := sourcePathExists(ctx, source, sourcePath)
		if err != nil {
			return fmt.Errorf("check included source path %s: %w", clean, err)
		}
		if exists == sourceExistsNo {
			continue
		}
		spec, err := BuildRcloneCopyToCommand(source, sourcePath, filepath.Join(targetRoot, filepath.FromSlash(clean)))
		if err != nil {
			return err
		}
		if err := runFullSyncCommand(ctx, runner, spec); err != nil {
			if isMissingSourceCopyError(err) {
				continue
			}
			return fmt.Errorf("force sync included file %s: %w", clean, err)
		}
	}
	return nil
}

func exactIncludePath(include string) (string, bool, error) {
	clean, err := NormalizeEventPath(include)
	if err != nil {
		return "", false, err
	}
	if strings.ContainsAny(clean, "*?[") {
		return "", false, nil
	}
	return clean, true, nil
}

func applyAgentArchiveFullSync(ctx context.Context, source SourceRef, token, sourceVolume, targetRoot string, filter PathFilter) error {
	archiveURL, err := archiveURLFromSource(source, sourceVolume, filter)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, archiveURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		responseBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("%s returned %s: %s", archiveURL, resp.Status, strings.TrimSpace(string(responseBody)))
	}
	if err := clearFullSyncTarget(targetRoot, filter); err != nil {
		return err
	}
	return ExtractTarArchive(targetRoot, resp.Body)
}

func archiveURLFromSource(source SourceRef, sourceVolume string, filter PathFilter) (string, error) {
	agentURL, err := agentURLFromWebDAV(source.WebDAVURL)
	if err != nil {
		return "", err
	}
	u, err := url.Parse(strings.TrimRight(agentURL, "/") + "/replication/archive")
	if err != nil {
		return "", err
	}
	parts := strings.SplitN(strings.Trim(sourceVolume, "/"), "/", 2)
	q := u.Query()
	q.Set("namespace", parts[0])
	if len(parts) > 1 {
		q.Set("volume", parts[1])
	}
	for _, include := range filter.IncludePaths {
		q.Add("include", include)
	}
	for _, exclude := range filter.ExcludePaths {
		q.Add("exclude", exclude)
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func agentURLFromWebDAV(webdavURL string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(webdavURL))
	if err != nil {
		return "", err
	}
	if u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("invalid source.webdavUrl %q", webdavURL)
	}
	host := u.Hostname()
	if host == "" {
		return "", fmt.Errorf("invalid source.webdavUrl %q", webdavURL)
	}
	u.Host = net.JoinHostPort(host, "8080")
	u.Path = ""
	u.RawQuery = ""
	u.Fragment = ""
	return u.String(), nil
}

func clearFullSyncTarget(targetRoot string, filter PathFilter) error {
	if len(filter.IncludePaths) == 0 {
		return clearDirectoryContentsPreservingDirs(targetRoot)
	}
	seen := make(map[string]bool)
	for _, include := range filter.IncludePaths {
		root := filterStaticPrefix(include)
		if root == "" {
			continue
		}
		clean, err := NormalizeEventPath(root)
		if err != nil {
			return err
		}
		if seen[clean] {
			continue
		}
		seen[clean] = true
		target := filepath.Join(targetRoot, filepath.FromSlash(clean))
		info, err := os.Lstat(target)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		if info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
			if err := clearDirectoryContentsPreservingDirs(target); err != nil {
				return err
			}
			continue
		}
		if err := os.RemoveAll(target); err != nil {
			return err
		}
	}
	return nil
}

func clearDirectoryContentsPreservingDirs(root string) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		target := filepath.Join(root, entry.Name())
		info, err := os.Lstat(target)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		if info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
			if err := clearDirectoryContentsPreservingDirs(target); err != nil {
				return err
			}
			continue
		}
		if err := os.RemoveAll(target); err != nil {
			return err
		}
	}
	return nil
}

func ApplyOwnershipPolicy(root, target string, policy OwnershipPolicy, recursive bool) error {
	if policy.Empty() {
		return nil
	}
	root = filepath.Clean(root)
	target = filepath.Clean(target)
	rel, err := filepath.Rel(root, target)
	if err != nil {
		return err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("ownership target escapes volume: %s", target)
	}
	if err := applyOwnershipToParentDirs(root, target, policy); err != nil {
		return err
	}
	info, err := os.Lstat(target)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if recursive && info.IsDir() {
		return filepath.WalkDir(target, func(p string, d os.DirEntry, err error) error {
			if err != nil {
				if os.IsNotExist(err) {
					return nil
				}
				return err
			}
			info, err := d.Info()
			if err != nil {
				if os.IsNotExist(err) {
					return nil
				}
				return err
			}
			return applyOwnershipToOne(p, info, policy)
		})
	}
	return applyOwnershipToOne(target, info, policy)
}

func ApplyOwnershipPolicyForFilter(root string, filter PathFilter, policy OwnershipPolicy) error {
	if policy.Empty() {
		return nil
	}
	if len(filter.IncludePaths) == 0 {
		return ApplyOwnershipPolicy(root, root, policy, true)
	}
	seen := make(map[string]bool)
	for _, include := range filter.IncludePaths {
		prefix := filterStaticPrefix(include)
		if prefix == "" {
			return ApplyOwnershipPolicy(root, root, policy, true)
		}
		clean, err := NormalizeEventPath(prefix)
		if err != nil {
			return err
		}
		if seen[clean] {
			continue
		}
		seen[clean] = true
		if err := ApplyOwnershipPolicy(root, filepath.Join(root, filepath.FromSlash(clean)), policy, true); err != nil {
			return err
		}
	}
	return nil
}

func applyOwnershipToParentDirs(root, target string, policy OwnershipPolicy) error {
	dir := target
	if info, err := os.Lstat(target); err == nil && !info.IsDir() {
		dir = filepath.Dir(target)
	} else if err != nil && os.IsNotExist(err) {
		dir = filepath.Dir(target)
	} else if err != nil {
		return err
	}
	for {
		info, err := os.Lstat(dir)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if err := applyOwnershipToOne(dir, info, OwnershipPolicy{
			UID:     policy.UID,
			GID:     policy.GID,
			DirMode: policy.DirMode,
		}); err != nil {
			return err
		}
		if dir == root {
			return nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return nil
		}
		dir = parent
	}
}

func applyOwnershipToOne(p string, info os.FileInfo, policy OwnershipPolicy) error {
	if policy.UID != nil || policy.GID != nil {
		uid := -1
		gid := -1
		if policy.UID != nil {
			uid = int(*policy.UID)
		}
		if policy.GID != nil {
			gid = int(*policy.GID)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			if err := os.Lchown(p, uid, gid); err != nil {
				return err
			}
		} else if err := os.Chown(p, uid, gid); err != nil {
			return err
		}
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil
	}
	if info.IsDir() && policy.DirMode != nil {
		return os.Chmod(p, os.FileMode(*policy.DirMode))
	}
	if !info.IsDir() && policy.FileMode != nil {
		return os.Chmod(p, fileModePreservingExecute(info.Mode(), os.FileMode(*policy.FileMode)))
	}
	return nil
}

func fileModePreservingExecute(current, desired os.FileMode) os.FileMode {
	return desired.Perm() | (current.Perm() & 0o111)
}

func WriteTarArchive(root string, w io.Writer, filter PathFilter) error {
	root = filepath.Clean(root)
	tw := tar.NewWriter(w)
	defer tw.Close()
	return filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if p == root {
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		archivePath := filepath.ToSlash(rel)
		info, err := os.Lstat(p)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if info.IsDir() {
			if !filter.ShouldTraverse(archivePath) {
				return filepath.SkipDir
			}
		} else if !filter.ShouldReplicate(archivePath) {
			return nil
		}
		linkTarget := ""
		if info.Mode()&os.ModeSymlink != 0 {
			linkTarget, err = os.Readlink(p)
			if err != nil {
				return err
			}
		}
		header, err := tar.FileInfoHeader(info, linkTarget)
		if err != nil {
			return err
		}
		header.Name = archivePath
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		file, err := os.Open(p)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		copyErr := copyFileToArchive(tw, file)
		closeErr := file.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	})
}

func ExtractTarArchive(root string, r io.Reader) error {
	root = filepath.Clean(root)
	tr := tar.NewReader(r)
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		rel, err := NormalizeEventPath(header.Name)
		if err != nil {
			return err
		}
		target := filepath.Join(root, filepath.FromSlash(rel))
		if err := ensurePathInside(root, target); err != nil {
			return err
		}
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, os.FileMode(header.Mode).Perm()); err != nil {
				return err
			}
			if err := restorePathMetadata(target, header, false); err != nil {
				return err
			}
		case tar.TypeSymlink:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			if err := os.RemoveAll(target); err != nil {
				return err
			}
			if err := os.Symlink(header.Linkname, target); err != nil {
				return err
			}
			if err := restorePathMetadata(target, header, true); err != nil {
				return err
			}
		case tar.TypeReg, tar.TypeRegA:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			file, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(header.Mode).Perm())
			if err != nil {
				return err
			}
			if err := copyArchiveToFile(file, tr); err != nil {
				_ = file.Close()
				return err
			}
			if err := file.Close(); err != nil {
				return err
			}
			if err := restorePathMetadata(target, header, false); err != nil {
				return err
			}
		default:
			continue
		}
	}
}

func copyFileToArchive(dst io.Writer, src *os.File) error {
	buf := make([]byte, archiveCopyBufferSize)
	var offset int64
	var dropStart int64
	for {
		n, readErr := src.Read(buf)
		if n > 0 {
			if _, err := dst.Write(buf[:n]); err != nil {
				return err
			}
			offset += int64(n)
			if offset-dropStart >= archiveCacheDropInterval {
				if err := adviseFileRange(src, dropStart, offset-dropStart); err != nil {
					return err
				}
				dropStart = offset
			}
		}
		if errors.Is(readErr, io.EOF) {
			if offset > dropStart {
				return adviseFileRange(src, dropStart, offset-dropStart)
			}
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

func copyArchiveToFile(dst *os.File, src io.Reader) error {
	buf := make([]byte, archiveCopyBufferSize)
	var offset int64
	var dropStart int64
	for {
		n, readErr := src.Read(buf)
		if n > 0 {
			if _, err := dst.Write(buf[:n]); err != nil {
				return err
			}
			offset += int64(n)
			if offset-dropStart >= archiveCacheDropInterval {
				if err := syncAndAdviseFileRange(dst, dropStart, offset-dropStart); err != nil {
					return err
				}
				dropStart = offset
			}
		}
		if errors.Is(readErr, io.EOF) {
			if offset > dropStart {
				return syncAndAdviseFileRange(dst, dropStart, offset-dropStart)
			}
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

func syncAndAdviseFileRange(file *os.File, offset, length int64) error {
	if length <= 0 {
		return nil
	}
	const syncFlags = unix.SYNC_FILE_RANGE_WAIT_BEFORE | unix.SYNC_FILE_RANGE_WRITE | unix.SYNC_FILE_RANGE_WAIT_AFTER
	if err := unix.SyncFileRange(int(file.Fd()), offset, length, syncFlags); err != nil &&
		!errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.ENOSYS) {
		if syncErr := unix.Fdatasync(int(file.Fd())); syncErr != nil && !errors.Is(syncErr, unix.EINVAL) {
			return err
		}
	}
	return adviseFileRange(file, offset, length)
}

func adviseFileRange(file *os.File, offset, length int64) error {
	if length <= 0 {
		return nil
	}
	if err := unix.Fadvise(int(file.Fd()), offset, length, unix.FADV_DONTNEED); err != nil &&
		!errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.ENOSYS) {
		return err
	}
	return nil
}

func ensurePathInside(root, target string) error {
	rel, err := filepath.Rel(root, target)
	if err != nil {
		return err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("archive entry escapes volume: %s", target)
	}
	return nil
}

func restorePathMetadata(target string, header *tar.Header, symlink bool) error {
	uid := header.Uid
	gid := header.Gid
	var err error
	if symlink {
		err = os.Lchown(target, uid, gid)
	} else {
		err = os.Chown(target, uid, gid)
	}
	if err != nil && !os.IsPermission(err) {
		return err
	}
	if !symlink {
		if err := os.Chmod(target, os.FileMode(header.Mode).Perm()); err != nil {
			return err
		}
		if err := os.Chtimes(target, header.ModTime, header.ModTime); err != nil {
			return err
		}
	}
	return nil
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
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if err := ApplyFullSyncWithToken(ctx, runner, pool, req, token); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"volume": req.Volume,
			"ok":     true,
		})
	}
}

func ShadowPrepareHandler(pool Pool, auth TokenAuthorizer, runner Runner, timeout time.Duration, lockers ...*VolumeLocker) http.HandlerFunc {
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
		var req ShadowPrepareRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		unlock := locker.Lock(req.Namespace, req.Volume)
		defer unlock()
		resp, err := PrepareShadow(ctx, runner, pool, req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(resp)
	}
}

func ArchiveHandler(pool Pool, auth TokenAuthorizer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !auth.Authorize(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		namespace := r.URL.Query().Get("namespace")
		volume := r.URL.Query().Get("volume")
		if volume == "" {
			http.Error(w, "volume is required", http.StatusBadRequest)
			return
		}
		root, err := ResolveVolumePath(VolumePath{Pool: pool, Namespace: namespace, Name: volume})
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if _, err := os.Stat(root); err != nil {
			if os.IsNotExist(err) {
				http.Error(w, "volume not found", http.StatusNotFound)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/x-tar")
		if err := WriteTarArchive(root, w, PathFilter{
			IncludePaths: r.URL.Query()["include"],
			ExcludePaths: r.URL.Query()["exclude"],
		}); err != nil {
			logArchiveError(err)
		}
	}
}

func logArchiveError(err error) {
	fmt.Fprintf(os.Stderr, "archive stream failed: %v\n", err)
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
