package agent

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

type recordingRunner struct {
	specs []CommandSpec
	err   error
	out   []byte
}

func (r *recordingRunner) Run(_ context.Context, spec CommandSpec) error {
	r.specs = append(r.specs, spec)
	return r.err
}

func (r *recordingRunner) RunOutput(_ context.Context, spec CommandSpec) ([]byte, error) {
	r.specs = append(r.specs, spec)
	return r.out, r.err
}

type sequenceRunner struct {
	specs []CommandSpec
	errs  []error
}

func (r *sequenceRunner) Run(_ context.Context, spec CommandSpec) error {
	r.specs = append(r.specs, spec)
	if len(r.errs) == 0 {
		return nil
	}
	err := r.errs[0]
	r.errs = r.errs[1:]
	return err
}

func (r *sequenceRunner) RunOutput(ctx context.Context, spec CommandSpec) ([]byte, error) {
	return nil, r.Run(ctx, spec)
}

type runnerFunc func(context.Context, CommandSpec) error

func (f runnerFunc) Run(ctx context.Context, spec CommandSpec) error {
	return f(ctx, spec)
}

func (f runnerFunc) RunOutput(ctx context.Context, spec CommandSpec) ([]byte, error) {
	return nil, f(ctx, spec)
}

func containsArg(args []string, want string) bool {
	for _, arg := range args {
		if arg == want {
			return true
		}
	}
	return false
}

func TestPathFilterIncludesAndExcludes(t *testing.T) {
	filter := PathFilter{
		IncludePaths: []string{"savegame/**", "config/server.json"},
		ExcludePaths: []string{"savegame/cache/**"},
	}
	cases := map[string]bool{
		"savegame/world/1":    true,
		"config/server.json":  true,
		"savegame/cache/tmp":  false,
		"steamapps/game.bin":  false,
		"../savegame/world/1": false,
		"/savegame/world/2":   true,
	}
	for p, want := range cases {
		if got := filter.ShouldReplicate(p); got != want {
			t.Fatalf("ShouldReplicate(%q) = %t, want %t", p, got, want)
		}
	}
}

func TestPathFilterTraversesParentsOfDeepIncludes(t *testing.T) {
	filter := PathFilter{
		IncludePaths: []string{
			"steam-root/windrose/WindowsServer/R5/ServerDescription.json",
			"steam-root/windrose/WindowsServer/R5/Saved/**",
		},
		ExcludePaths: []string{"steam-root/windrose/WindowsServer/R5/Saved/Logs/**"},
	}
	cases := map[string]bool{
		"steam-root":                                      true,
		"steam-root/windrose":                             true,
		"steam-root/windrose/WindowsServer/R5":            true,
		"steam-root/windrose/WindowsServer/R5/Saved":      true,
		"steam-root/windrose/WindowsServer/R5/Saved/abc":  true,
		"steam-root/windrose/WindowsServer/R5/Saved/Logs": false,
		"steam-root/Steam":                                false,
	}
	for p, want := range cases {
		if got := filter.ShouldTraverse(p); got != want {
			t.Fatalf("ShouldTraverse(%q) = %t, want %t", p, got, want)
		}
	}
}

func TestCoalesceEventsKeepsLastEventPerPath(t *testing.T) {
	events := CoalesceEvents([]FileEvent{
		{Path: "save/a", Op: EventOpUpsert},
		{Path: "save/a", Op: EventOpDelete},
		{Path: "cache/tmp", Op: EventOpUpsert},
	}, PathFilter{IncludePaths: []string{"save/**"}})
	if len(events) != 1 {
		t.Fatalf("events = %#v", events)
	}
	if events[0].Path != "save/a" || events[0].Op != EventOpDelete {
		t.Fatalf("event = %#v", events[0])
	}
}

func TestCoalesceEventsPreservesFinalOccurrenceOrder(t *testing.T) {
	events := CoalesceEvents([]FileEvent{
		{Path: "db/MANIFEST-0001", Op: EventOpUpsert},
		{Path: "db/000123.sst", Op: EventOpUpsert},
		{Path: "db/CURRENT", Op: EventOpUpsert},
		{Path: "db/000123.sst", Op: EventOpDelete},
		{Path: "db/000124.sst", Op: EventOpUpsert},
	}, PathFilter{IncludePaths: []string{"db/**"}})
	want := []FileEvent{
		{Path: "db/MANIFEST-0001", Op: EventOpUpsert},
		{Path: "db/CURRENT", Op: EventOpUpsert},
		{Path: "db/000123.sst", Op: EventOpDelete},
		{Path: "db/000124.sst", Op: EventOpUpsert},
	}
	if len(events) != len(want) {
		t.Fatalf("events = %#v, want %#v", events, want)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("events[%d] = %#v, want %#v; all=%#v", i, events[i], want[i], events)
		}
	}
}

func TestBuildRcloneCopyToCommand(t *testing.T) {
	spec, err := BuildRcloneCopyToCommand(SourceRef{WebDAVURL: "http://source:8081"}, "default/demo/save/a.txt", filepath.Join("/target", "save", "a.txt"))
	if err != nil {
		t.Fatalf("BuildRcloneCopyToCommand returned error: %v", err)
	}
	if spec.Name != "rclone" {
		t.Fatalf("name = %q", spec.Name)
	}
	if spec.Args[len(spec.Args)-2] != ":webdav:default/demo/save/a.txt" {
		t.Fatalf("source arg = %q; args=%#v", spec.Args[len(spec.Args)-2], spec.Args)
	}
	if spec.Args[len(spec.Args)-1] != filepath.Join("/target", "save", "a.txt") {
		t.Fatalf("target arg = %q; args=%#v", spec.Args[len(spec.Args)-1], spec.Args)
	}
	assertLowMemoryRcloneArgs(t, spec.Args)
	if !containsArg(spec.Args, "--metadata") {
		t.Fatalf("args missing --metadata: %#v", spec.Args)
	}
	if !containsArg(spec.Args, "--links") {
		t.Fatalf("args missing --links: %#v", spec.Args)
	}
	if !containsArg(spec.Args, "--ignore-times") {
		t.Fatalf("args missing --ignore-times: %#v", spec.Args)
	}
}

func TestBuildRcloneServeWebDAVCommandPreservesMetadataAndLinks(t *testing.T) {
	spec := BuildRcloneServeWebDAVCommand("/pool", ":8081", true)
	if spec.Name != "rclone" {
		t.Fatalf("name = %q", spec.Name)
	}
	if !containsArg(spec.Args, "--metadata") {
		t.Fatalf("args missing --metadata: %#v", spec.Args)
	}
	if !containsArg(spec.Args, "--links") {
		t.Fatalf("args missing --links: %#v", spec.Args)
	}
	if !containsArg(spec.Args, "--read-only") {
		t.Fatalf("args missing --read-only: %#v", spec.Args)
	}
}

func TestBuildRcloneFullSyncCommandUsesOrderedFilters(t *testing.T) {
	spec := BuildRcloneFullSyncCommand(SourceRef{WebDAVURL: "http://source:8081"}, "games/demo", "/target", PathFilter{
		IncludePaths: []string{"steam-root/windrose/WindowsServer/R5/Saved/**"},
		ExcludePaths: []string{"steam-root/windrose/WindowsServer/R5/Saved/Logs/**"},
	})
	got := strings.Join(spec.Args, "\x00")
	if strings.Contains(got, "--include") || strings.Contains(got, "--exclude") {
		t.Fatalf("args use unordered include/exclude flags: %#v", spec.Args)
	}
	want := []string{
		"--filter", "- /steam-root/windrose/WindowsServer/R5/Saved/Logs/**",
		"--filter", "+ /steam-root/windrose/WindowsServer/R5/Saved/**",
		"--filter", "- **",
	}
	for _, part := range want {
		if !containsArg(spec.Args, part) {
			t.Fatalf("args missing %q: %#v", part, spec.Args)
		}
	}
	assertLowMemoryRcloneArgs(t, spec.Args)
	if !containsArg(spec.Args, "--metadata") {
		t.Fatalf("args missing --metadata: %#v", spec.Args)
	}
	if !containsArg(spec.Args, "--links") {
		t.Fatalf("args missing --links: %#v", spec.Args)
	}
	if containsArg(spec.Args, "--delete-excluded") {
		t.Fatalf("full sync should preserve excluded paths by default: %#v", spec.Args)
	}
	if containsArg(spec.Args, "--ignore-times") {
		t.Fatalf("full sync should not force unchanged trees with --ignore-times: %#v", spec.Args)
	}
}

func TestBuildRcloneFullSyncCommandCanPruneExcludedPaths(t *testing.T) {
	spec := BuildRcloneFullSyncCommand(SourceRef{WebDAVURL: "http://source:8081"}, "games/demo", "/target", PathFilter{
		IncludePaths:  []string{"save/**"},
		ExcludePaths:  []string{"save/tmp/**"},
		PruneExcluded: true,
	})
	if !containsArg(spec.Args, "--delete-excluded") {
		t.Fatalf("args missing opt-in --delete-excluded: %#v", spec.Args)
	}
}

func TestBuildRcloneLocalSyncCommandUsesLowMemoryFilters(t *testing.T) {
	spec := BuildRcloneLocalSyncCommand("/live", "/shadow", PathFilter{
		IncludePaths: []string{"save/**"},
		ExcludePaths: []string{"save/tmp/**"},
	})
	if spec.Name != "rclone" {
		t.Fatalf("name = %q", spec.Name)
	}
	if got := spec.Args[0]; got != "sync" {
		t.Fatalf("command = %q", got)
	}
	if got := spec.Args[1]; got != "/live" {
		t.Fatalf("source = %q", got)
	}
	if got := spec.Args[2]; got != "/shadow" {
		t.Fatalf("target = %q", got)
	}
	for _, part := range []string{"--metadata", "--links", "--filter", "- /save/tmp/**", "+ /save/**", "- **"} {
		if !containsArg(spec.Args, part) {
			t.Fatalf("args missing %q: %#v", part, spec.Args)
		}
	}
	if containsArg(spec.Args, "--delete-excluded") {
		t.Fatalf("local sync should preserve excluded paths by default: %#v", spec.Args)
	}
	if containsArg(spec.Args, "--ignore-times") {
		t.Fatalf("local sync should not force unchanged trees with --ignore-times: %#v", spec.Args)
	}
	assertLowMemoryRcloneArgs(t, spec.Args)
}

func TestBuildRcloneLocalSyncCommandCanPruneExcludedPaths(t *testing.T) {
	spec := BuildRcloneLocalSyncCommand("/live", "/shadow", PathFilter{
		IncludePaths:  []string{"save/**"},
		ExcludePaths:  []string{"save/tmp/**"},
		PruneExcluded: true,
	})
	if !containsArg(spec.Args, "--delete-excluded") {
		t.Fatalf("args missing opt-in --delete-excluded: %#v", spec.Args)
	}
}

func TestBuildRcloneCheckCommandUsesRequiredPath(t *testing.T) {
	spec := BuildRcloneCheckCommand(
		SourceRef{WebDAVURL: "http://source:8081"},
		"games/demo/save/required",
		"/target/save/required",
	)
	if spec.Name != "rclone" {
		t.Fatalf("name = %q", spec.Name)
	}
	want := []string{
		"check",
		"--one-way",
		"--size-only",
		"--checkers",
		"1",
		":webdav:games/demo/save/required",
		"/target/save/required",
	}
	for _, part := range want {
		if !containsArg(spec.Args, part) {
			t.Fatalf("args missing %q: %#v", part, spec.Args)
		}
	}
}

func TestBuildRclonePathSyncCommandUsesRequiredPath(t *testing.T) {
	spec := BuildRclonePathSyncCommand(
		SourceRef{WebDAVURL: "http://source:8081"},
		"games/demo/save/required",
		"/target/save/required",
	)
	if spec.Name != "rclone" {
		t.Fatalf("name = %q", spec.Name)
	}
	want := []string{
		"sync",
		"--metadata",
		"--links",
		":webdav:games/demo/save/required",
		"/target/save/required",
	}
	for _, part := range want {
		if !containsArg(spec.Args, part) {
			t.Fatalf("args missing %q: %#v", part, spec.Args)
		}
	}
	assertLowMemoryRcloneArgs(t, spec.Args)
}

func assertLowMemoryRcloneArgs(t *testing.T, args []string) {
	t.Helper()
	want := map[string]string{
		"--transfers":            "1",
		"--checkers":             "1",
		"--buffer-size":          "0",
		"--multi-thread-streams": "0",
	}
	for flag, value := range want {
		found := false
		for i := 0; i < len(args)-1; i++ {
			if args[i] == flag && args[i+1] == value {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("args missing %s %s: %#v", flag, value, args)
		}
	}
}

func TestApplyEventBatchRunsCopyAndKeepsDeleteUntilFullSync(t *testing.T) {
	dir := t.TempDir()
	pool := Pool{Name: "default", Path: dir}
	if err := EnsurePool(pool, false); err != nil {
		t.Fatal(err)
	}
	deleteTarget := filepath.Join(dir, "default", "demo", "old.tmp")
	if _, err := EnsureVolumePath(VolumePath{Pool: pool, Namespace: "default", Name: "demo"}, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := writeFile(deleteTarget, "old"); err != nil {
		t.Fatal(err)
	}
	runner := &recordingRunner{}
	err := ApplyEventBatch(context.Background(), runner, pool, EventBatch{
		Namespace: "default",
		Volume:    "demo",
		Source:    SourceRef{WebDAVURL: "http://source:8081"},
		Events: []FileEvent{
			{Path: "save/a.txt", Op: EventOpUpsert},
			{Path: "old.tmp", Op: EventOpDelete},
		},
	})
	if err != nil {
		t.Fatalf("ApplyEventBatch returned error: %v", err)
	}
	if len(runner.specs) != 1 {
		t.Fatalf("ran %d commands, want 1", len(runner.specs))
	}
	if _, err := os.Stat(deleteTarget); err != nil {
		t.Fatalf("delete target should remain until full sync pruning: %v", err)
	}
}

func TestApplyEventBatchAppliesAuthoritativeDeletes(t *testing.T) {
	dir := t.TempDir()
	pool := Pool{Name: "default", Path: dir}
	if err := EnsurePool(pool, false); err != nil {
		t.Fatal(err)
	}
	deleteTarget := filepath.Join(dir, "default", "demo", "old.tmp")
	if err := writeFile(deleteTarget, "old"); err != nil {
		t.Fatal(err)
	}
	err := ApplyEventBatch(context.Background(), &recordingRunner{}, pool, EventBatch{
		Namespace:            "default",
		Volume:               "demo",
		Source:               SourceRef{WebDAVURL: "http://source:8081"},
		DeletesAuthoritative: true,
		Events:               []FileEvent{{Path: "old.tmp", Op: EventOpDelete}},
	})
	if err != nil {
		t.Fatalf("ApplyEventBatch returned error: %v", err)
	}
	if _, err := os.Stat(deleteTarget); !os.IsNotExist(err) {
		t.Fatalf("delete target still exists or stat failed: %v", err)
	}
}

func TestApplyEventBatchAppliesOwnershipPolicyModes(t *testing.T) {
	dir := t.TempDir()
	pool := Pool{Name: "default", Path: dir}
	if err := EnsurePool(pool, false); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dir, "default", "demo", "save", "a.txt")
	if err := writeFile(target, "new"); err != nil {
		t.Fatal(err)
	}
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer source.Close()
	fileMode := uint32(0o660)
	dirMode := uint32(0o770)

	runner := &recordingRunner{}
	err := ApplyEventBatch(context.Background(), runner, pool, EventBatch{
		Namespace: "default",
		Volume:    "demo",
		Source:    SourceRef{WebDAVURL: source.URL},
		Ownership: OwnershipPolicy{FileMode: &fileMode, DirMode: &dirMode},
		Events:    []FileEvent{{Path: "save/a.txt", Op: EventOpUpsert}},
	})
	if err != nil {
		t.Fatalf("ApplyEventBatch returned error: %v", err)
	}
	assertMode(t, target, 0o660)
	assertMode(t, filepath.Dir(target), 0o770)
}

func TestApplyEventBatchPreservesExecutableBitsWhenApplyingFileMode(t *testing.T) {
	dir := t.TempDir()
	pool := Pool{Name: "default", Path: dir}
	if err := EnsurePool(pool, false); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dir, "default", "demo", "bin", "launch.sh")
	if err := writeFile(target, "new"); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(target, 0o755); err != nil {
		t.Fatal(err)
	}
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer source.Close()
	fileMode := uint32(0o664)

	runner := &recordingRunner{}
	err := ApplyEventBatch(context.Background(), runner, pool, EventBatch{
		Namespace: "default",
		Volume:    "demo",
		Source:    SourceRef{WebDAVURL: source.URL},
		Ownership: OwnershipPolicy{FileMode: &fileMode},
		Events:    []FileEvent{{Path: "bin/launch.sh", Op: EventOpUpsert}},
	})
	if err != nil {
		t.Fatalf("ApplyEventBatch returned error: %v", err)
	}
	assertMode(t, target, 0o775)
}

func TestApplyEventBatchSkipsUpsertWhenSourceAlreadyGone(t *testing.T) {
	dir := t.TempDir()
	pool := Pool{Name: "default", Path: dir}
	if err := EnsurePool(pool, false); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dir, "default", "demo", "save", "a.txt")
	if err := writeFile(target, "old"); err != nil {
		t.Fatal(err)
	}
	source := httptest.NewServer(http.NotFoundHandler())
	defer source.Close()

	runner := &recordingRunner{}
	err := ApplyEventBatch(context.Background(), runner, pool, EventBatch{
		Namespace: "default",
		Volume:    "demo",
		Source:    SourceRef{WebDAVURL: source.URL},
		Events:    []FileEvent{{Path: "save/a.txt", Op: EventOpUpsert}},
	})
	if err != nil {
		t.Fatalf("ApplyEventBatch returned error: %v", err)
	}
	if len(runner.specs) != 0 {
		t.Fatalf("ran %d commands, want none", len(runner.specs))
	}
	if _, err := os.Stat(target); err != nil {
		t.Fatalf("target should remain after racy missing upsert source: %v", err)
	}
}

func TestApplyEventBatchSkipsUpsertWhenSourceDisappearsAfterCopyFailure(t *testing.T) {
	dir := t.TempDir()
	pool := Pool{Name: "default", Path: dir}
	if err := EnsurePool(pool, false); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dir, "default", "demo", "save", "a.txt")
	if err := writeFile(target, "old"); err != nil {
		t.Fatal(err)
	}
	var checks int
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		checks++
		if checks == 1 {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.NotFound(w, r)
	}))
	defer source.Close()

	runner := &recordingRunner{err: errors.New("copy failed")}
	err := ApplyEventBatch(context.Background(), runner, pool, EventBatch{
		Namespace: "default",
		Volume:    "demo",
		Source:    SourceRef{WebDAVURL: source.URL},
		Events:    []FileEvent{{Path: "save/a.txt", Op: EventOpUpsert}},
	})
	if err != nil {
		t.Fatalf("ApplyEventBatch returned error: %v", err)
	}
	if len(runner.specs) != 1 {
		t.Fatalf("ran %d commands, want 1", len(runner.specs))
	}
	if _, err := os.Stat(target); err != nil {
		t.Fatalf("target should remain after racy disappeared upsert source: %v", err)
	}
}

func TestApplyEventBatchSkipsUpsertForRcloneMissingSourceOutput(t *testing.T) {
	dir := t.TempDir()
	pool := Pool{Name: "default", Path: dir}
	if err := EnsurePool(pool, false); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dir, "default", "demo", "save", "068848.sst")
	if err := writeFile(target, "old"); err != nil {
		t.Fatal(err)
	}
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer source.Close()

	runner := &recordingRunner{
		err: errors.New("exit status 3"),
		out: []byte("ERROR : webdav root 'default/demo/save/068848.sst': error reading source root directory: directory not found\nFailed to copyto: directory not found\n"),
	}
	err := ApplyEventBatch(context.Background(), runner, pool, EventBatch{
		Namespace: "default",
		Volume:    "demo",
		Source:    SourceRef{WebDAVURL: source.URL},
		Events:    []FileEvent{{Path: "save/068848.sst", Op: EventOpUpsert}},
	})
	if err != nil {
		t.Fatalf("ApplyEventBatch returned error: %v", err)
	}
	if len(runner.specs) != 1 {
		t.Fatalf("ran %d commands, want 1", len(runner.specs))
	}
	if _, err := os.Stat(target); err != nil {
		t.Fatalf("target should remain after racy rclone missing-source output: %v", err)
	}
}

func TestApplyEventBatchSkipsDirectoryCopyToOutput(t *testing.T) {
	dir := t.TempDir()
	pool := Pool{Name: "default", Path: dir}
	if err := EnsurePool(pool, false); err != nil {
		t.Fatal(err)
	}
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusMethodNotAllowed)
	}))
	defer source.Close()

	runner := &recordingRunner{
		err: errors.New("exit status 3"),
		out: []byte("Source doesn't exist or is a directory and destination is a file\n"),
	}
	err := ApplyEventBatch(context.Background(), runner, pool, EventBatch{
		Namespace: "default",
		Volume:    "demo",
		Source:    SourceRef{WebDAVURL: source.URL},
		Events:    []FileEvent{{Path: "save", Op: EventOpUpsert}},
	})
	if err != nil {
		t.Fatalf("ApplyEventBatch returned error: %v", err)
	}
	if len(runner.specs) != 1 {
		t.Fatalf("ran %d commands, want 1", len(runner.specs))
	}
}

func TestApplyFullSyncRunsScopedRcloneSync(t *testing.T) {
	dir := t.TempDir()
	pool := Pool{Name: "default", Path: dir}
	if err := EnsurePool(pool, false); err != nil {
		t.Fatal(err)
	}
	runner := &recordingRunner{}
	err := ApplyFullSync(context.Background(), runner, pool, FullSyncRequest{
		Namespace:    "default",
		Volume:       "demo",
		Source:       SourceRef{WebDAVURL: "http://source:8081"},
		IncludePaths: []string{"saves/**", "server.json"},
		ExcludePaths: []string{"downloads/**"},
	})
	if err != nil {
		t.Fatalf("ApplyFullSync returned error: %v", err)
	}
	if len(runner.specs) != 2 {
		t.Fatalf("ran %d commands, want scoped sync plus exact file repair", len(runner.specs))
	}
	spec := runner.specs[0]
	if spec.Name != "rclone" {
		t.Fatalf("name = %q", spec.Name)
	}
	if spec.Args[0] != "sync" {
		t.Fatalf("args = %#v", spec.Args)
	}
	if got := spec.Args[len(spec.Args)-2]; got != ":webdav:default/demo" {
		t.Fatalf("source = %q; args=%#v", got, spec.Args)
	}
	if got := spec.Args[len(spec.Args)-1]; got != filepath.Join(dir, "default", "demo") {
		t.Fatalf("target = %q; args=%#v", got, spec.Args)
	}
	exact := runner.specs[1]
	if exact.Args[0] != "copyto" {
		t.Fatalf("exact repair args = %#v, want copyto", exact.Args)
	}
	if !containsArg(exact.Args, "--ignore-times") {
		t.Fatalf("exact repair should force copy: %#v", exact.Args)
	}
	if got := exact.Args[len(exact.Args)-2]; got != ":webdav:default/demo/server.json" {
		t.Fatalf("exact repair source = %q; args=%#v", got, exact.Args)
	}
}

func TestApplyFullSyncWithTokenStillUsesRclone(t *testing.T) {
	dir := t.TempDir()
	pool := Pool{Name: "default", Path: dir}
	if err := EnsurePool(pool, false); err != nil {
		t.Fatal(err)
	}
	runner := &recordingRunner{}
	err := ApplyFullSyncWithToken(context.Background(), runner, pool, FullSyncRequest{
		Namespace: "default",
		Volume:    "demo",
		Source:    SourceRef{WebDAVURL: "http://source:8081"},
	}, "sync-token")
	if err != nil {
		t.Fatalf("ApplyFullSyncWithToken returned error: %v", err)
	}
	if len(runner.specs) != 1 {
		t.Fatalf("ran %d commands, want 1", len(runner.specs))
	}
	if got := runner.specs[0].Name; got != "rclone" {
		t.Fatalf("command = %q, want rclone", got)
	}
}

func TestApplyFullSyncCanBackupExistingTarget(t *testing.T) {
	dir := t.TempDir()
	pool := Pool{Name: "default", Path: dir}
	if err := EnsurePool(pool, false); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dir, "default", "demo", "save.dat")
	if err := writeFile(target, "old state"); err != nil {
		t.Fatal(err)
	}
	runner := &recordingRunner{}
	err := ApplyFullSync(context.Background(), runner, pool, FullSyncRequest{
		Namespace:      "default",
		Volume:         "demo",
		Source:         SourceRef{WebDAVURL: "http://source:8081"},
		BackupExisting: true,
	})
	if err != nil {
		t.Fatalf("ApplyFullSync returned error: %v", err)
	}
	backups, err := filepath.Glob(filepath.Join(dir, ".simple-volume-backups", "default", "demo", "*", "save.dat"))
	if err != nil {
		t.Fatal(err)
	}
	if len(backups) != 1 {
		t.Fatalf("backups = %#v, want one backed up save.dat", backups)
	}
	if _, err := os.Stat(filepath.Join(dir, "default", "demo")); err != nil {
		t.Fatalf("restored target directory missing: %v", err)
	}
	if len(runner.specs) != 1 {
		t.Fatalf("ran %d commands, want 1", len(runner.specs))
	}
}

func TestPrepareShadowBuildsLocalShadowAndManifest(t *testing.T) {
	dir := t.TempDir()
	pool := Pool{Name: "default", Path: dir}
	if err := EnsurePool(pool, false); err != nil {
		t.Fatal(err)
	}
	if err := writeFile(filepath.Join(dir, "default", "demo", "save", "world.db"), "state"); err != nil {
		t.Fatal(err)
	}
	runner := &recordingRunner{}
	resp, err := PrepareShadow(context.Background(), runner, pool, ShadowPrepareRequest{
		Namespace:     "default",
		Volume:        "demo",
		IncludePaths:  []string{"save/**"},
		RequiredPaths: []string{"save"},
	})
	if err == nil || !strings.Contains(err.Error(), "required shadow path missing") {
		t.Fatalf("PrepareShadow error = %v, want missing shadow path with recording runner", err)
	}
	if resp.OK {
		t.Fatalf("response = %#v, want not ok on failed prepare", resp)
	}
	if len(runner.specs) != 1 {
		t.Fatalf("ran %d commands, want 1", len(runner.specs))
	}
	if got := runner.specs[0].Args[1]; got != filepath.Join(dir, "default", "demo") {
		t.Fatalf("shadow sync source = %q", got)
	}
	if got := runner.specs[0].Args[2]; got != filepath.Join(dir, ".simple-volume-shadows", "default", "demo", "current", "data") {
		t.Fatalf("shadow sync target = %q", got)
	}
}

func TestApplyShadowEventBatchStagesChangedFiles(t *testing.T) {
	dir := t.TempDir()
	pool := Pool{Name: "default", Path: dir}
	if err := EnsurePool(pool, false); err != nil {
		t.Fatal(err)
	}
	if err := writeFile(filepath.Join(dir, "default", "demo", "save", "world.db"), "state"); err != nil {
		t.Fatal(err)
	}
	staleShadow := filepath.Join(dir, ".simple-volume-shadows", "default", "demo", "current", "data", "old.sst")
	if err := writeFile(staleShadow, "old"); err != nil {
		t.Fatal(err)
	}
	runner := &recordingRunner{}
	batch, err := ApplyShadowEventBatch(context.Background(), runner, pool, EventBatch{
		Namespace:  "default",
		Volume:     "demo",
		Source:     SourceRef{WebDAVURL: "http://source:8081"},
		Generation: "g1",
		Events: []FileEvent{
			{Path: "save/world.db", Op: EventOpUpsert},
			{Path: "old.sst", Op: EventOpDelete},
		},
	}, PathFilter{IncludePaths: []string{"save/**", "old.sst"}})
	if err != nil {
		t.Fatalf("ApplyShadowEventBatch returned error: %v", err)
	}
	if batch.SourceBasePath != ShadowSourceBasePath("default", "demo") {
		t.Fatalf("SourceBasePath = %q", batch.SourceBasePath)
	}
	if !batch.DeletesAuthoritative {
		t.Fatalf("DeletesAuthoritative = false")
	}
	if len(runner.specs) != 1 {
		t.Fatalf("ran %d commands, want one copyto", len(runner.specs))
	}
	if _, err := os.Stat(staleShadow); !os.IsNotExist(err) {
		t.Fatalf("stale shadow still exists or stat failed: %v", err)
	}
	if got := runner.specs[0].Args[1]; got != filepath.Join(dir, "default", "demo", "save", "world.db") {
		t.Fatalf("copy source = %q", got)
	}
	if got := runner.specs[0].Args[2]; got != filepath.Join(dir, ".simple-volume-shadows", "default", "demo", "current", "data", "save", "world.db") {
		t.Fatalf("copy target = %q", got)
	}
}

func TestApplyShadowEventBatchTreatsVanishedSourceAsDelete(t *testing.T) {
	dir := t.TempDir()
	pool := Pool{Name: "default", Path: dir}
	if err := EnsurePool(pool, false); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(dir, "default", "demo", "save", "compacted.sst")
	if err := writeFile(source, "state"); err != nil {
		t.Fatal(err)
	}
	shadow := filepath.Join(dir, ".simple-volume-shadows", "default", "demo", "current", "data", "save", "compacted.sst")
	if err := writeFile(shadow, "old"); err != nil {
		t.Fatal(err)
	}

	runner := runnerFunc(func(_ context.Context, _ CommandSpec) error {
		if err := os.Remove(source); err != nil {
			t.Fatal(err)
		}
		return errors.New("directory not found")
	})
	batch, err := ApplyShadowEventBatch(context.Background(), runner, pool, EventBatch{
		Namespace: "default",
		Volume:    "demo",
		Events: []FileEvent{
			{Path: "save/compacted.sst", Op: EventOpUpsert},
		},
	}, PathFilter{IncludePaths: []string{"save/**"}})
	if err != nil {
		t.Fatalf("ApplyShadowEventBatch returned error: %v", err)
	}
	if len(batch.Events) != 1 || batch.Events[0].Op != EventOpDelete {
		t.Fatalf("events = %#v, want authoritative delete", batch.Events)
	}
	if _, err := os.Stat(shadow); !os.IsNotExist(err) {
		t.Fatalf("shadow file still exists or stat failed: %v", err)
	}
}

func TestApplyShadowEventBatchTreatsRcloneMissingSourceOutputAsDelete(t *testing.T) {
	dir := t.TempDir()
	pool := Pool{Name: "default", Path: dir}
	if err := EnsurePool(pool, false); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(dir, "default", "demo", "save", "reused.sst")
	if err := writeFile(source, "new state"); err != nil {
		t.Fatal(err)
	}
	shadow := filepath.Join(dir, ".simple-volume-shadows", "default", "demo", "current", "data", "save", "reused.sst")
	if err := writeFile(shadow, "old"); err != nil {
		t.Fatal(err)
	}

	runner := &recordingRunner{
		err: errors.New("exit status 3"),
		out: []byte("ERROR : Local file system at /pool/default/demo/save/reused.sst: error reading source root directory: directory not found\n"),
	}
	batch, err := ApplyShadowEventBatch(context.Background(), runner, pool, EventBatch{
		Namespace: "default",
		Volume:    "demo",
		Events: []FileEvent{
			{Path: "save/reused.sst", Op: EventOpUpsert},
		},
	}, PathFilter{IncludePaths: []string{"save/**"}})
	if err != nil {
		t.Fatalf("ApplyShadowEventBatch returned error: %v", err)
	}
	if len(batch.Events) != 1 || batch.Events[0].Op != EventOpDelete {
		t.Fatalf("events = %#v, want authoritative delete", batch.Events)
	}
	if _, err := os.Stat(shadow); !os.IsNotExist(err) {
		t.Fatalf("shadow file still exists or stat failed: %v", err)
	}
}

func TestApplyFullSyncRequiredPathFailsBeforeClearingTarget(t *testing.T) {
	dir := t.TempDir()
	pool := Pool{Name: "default", Path: dir}
	if err := EnsurePool(pool, false); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dir, "default", "demo", "save", "existing.sst")
	if err := writeFile(target, "keep"); err != nil {
		t.Fatal(err)
	}
	source := httptest.NewServer(http.NotFoundHandler())
	defer source.Close()

	err := ApplyFullSync(context.Background(), &recordingRunner{}, pool, FullSyncRequest{
		Namespace:     "default",
		Volume:        "demo",
		Source:        SourceRef{WebDAVURL: source.URL},
		RequiredPaths: []string{"save/required"},
	})
	if err == nil || !strings.Contains(err.Error(), "required source path missing") {
		t.Fatalf("ApplyFullSync error = %v, want missing required path", err)
	}
	if _, statErr := os.Stat(target); statErr != nil {
		t.Fatalf("target was cleared despite missing required path: %v", statErr)
	}
}

func TestApplyFullSyncRequiredPathAllowsWebDAVDirectorySlash(t *testing.T) {
	dir := t.TempDir()
	pool := Pool{Name: "default", Path: dir}
	if err := EnsurePool(pool, false); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "default", "demo", "save", "required"), 0o755); err != nil {
		t.Fatal(err)
	}
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead && r.URL.Path == "/default/demo/save/required" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if r.Method == http.MethodHead && r.URL.Path == "/default/demo/save/required/" {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.NotFound(w, r)
	}))
	defer source.Close()

	err := ApplyFullSync(context.Background(), &recordingRunner{}, pool, FullSyncRequest{
		Namespace:     "default",
		Volume:        "demo",
		Source:        SourceRef{WebDAVURL: source.URL},
		RequiredPaths: []string{"save/required"},
	})
	if err != nil {
		t.Fatalf("ApplyFullSync returned error: %v", err)
	}
}

func TestApplyFullSyncRequiredPathFailsWhenTargetCheckFails(t *testing.T) {
	dir := t.TempDir()
	pool := Pool{Name: "default", Path: dir}
	if err := EnsurePool(pool, false); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "default", "demo", "save", "required"), 0o755); err != nil {
		t.Fatal(err)
	}
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead && r.URL.Path == "/default/demo/save/required" {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.NotFound(w, r)
	}))
	defer source.Close()

	runner := &sequenceRunner{errs: []error{
		nil,
		nil, errors.New("missing files"),
		nil, errors.New("missing files"),
		nil, errors.New("missing files"),
	}}
	err := ApplyFullSync(context.Background(), runner, pool, FullSyncRequest{
		Namespace:     "default",
		Volume:        "demo",
		Source:        SourceRef{WebDAVURL: source.URL},
		RequiredPaths: []string{"save/required"},
	})
	if err == nil || !strings.Contains(err.Error(), "required target path failed post-sync check") {
		t.Fatalf("ApplyFullSync error = %v, want post-sync check failure", err)
	}
	if len(runner.specs) != 7 {
		t.Fatalf("ran %d commands, want full sync plus three required path sync/check attempts", len(runner.specs))
	}
	if runner.specs[1].Args[0] != "sync" || runner.specs[2].Args[0] != "check" {
		t.Fatalf("required path commands = %#v / %#v, want sync then check", runner.specs[1], runner.specs[2])
	}
}

func TestApplyFullSyncAppliesOwnershipPolicyModes(t *testing.T) {
	dir := t.TempDir()
	pool := Pool{Name: "default", Path: dir}
	if err := EnsurePool(pool, false); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dir, "default", "demo", "save", "a.txt")
	if err := writeFile(target, "state"); err != nil {
		t.Fatal(err)
	}
	fileMode := uint32(0o664)
	dirMode := uint32(0o775)
	runner := &recordingRunner{}
	err := ApplyFullSync(context.Background(), runner, pool, FullSyncRequest{
		Namespace: "default",
		Volume:    "demo",
		Source:    SourceRef{WebDAVURL: "http://source:8081"},
		Ownership: OwnershipPolicy{FileMode: &fileMode, DirMode: &dirMode},
	})
	if err != nil {
		t.Fatalf("ApplyFullSync returned error: %v", err)
	}
	assertMode(t, target, 0o664)
	assertMode(t, filepath.Dir(target), 0o775)
}

func TestApplyFullSyncAppliesOwnershipPolicyOnlyToIncludedPaths(t *testing.T) {
	dir := t.TempDir()
	pool := Pool{Name: "default", Path: dir}
	if err := EnsurePool(pool, false); err != nil {
		t.Fatal(err)
	}
	included := filepath.Join(dir, "default", "demo", "save", "a.txt")
	excluded := filepath.Join(dir, "default", "demo", "downloads", "cache.bin")
	if err := writeFile(included, "state"); err != nil {
		t.Fatal(err)
	}
	if err := writeFile(excluded, "cache"); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(included, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(excluded, 0o600); err != nil {
		t.Fatal(err)
	}
	fileMode := uint32(0o664)
	dirMode := uint32(0o775)
	runner := &recordingRunner{}
	err := ApplyFullSync(context.Background(), runner, pool, FullSyncRequest{
		Namespace:    "default",
		Volume:       "demo",
		Source:       SourceRef{WebDAVURL: "http://source:8081"},
		IncludePaths: []string{"save/**"},
		Ownership:    OwnershipPolicy{FileMode: &fileMode, DirMode: &dirMode},
	})
	if err != nil {
		t.Fatalf("ApplyFullSync returned error: %v", err)
	}
	assertMode(t, included, 0o664)
	assertMode(t, filepath.Dir(included), 0o775)
	assertMode(t, excluded, 0o600)
}

func TestApplyFullSyncPreservesExecutableBitsWhenApplyingFileMode(t *testing.T) {
	dir := t.TempDir()
	pool := Pool{Name: "default", Path: dir}
	if err := EnsurePool(pool, false); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dir, "default", "demo", "bin", "launch.sh")
	if err := writeFile(target, "state"); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(target, 0o755); err != nil {
		t.Fatal(err)
	}
	fileMode := uint32(0o664)
	runner := &recordingRunner{}
	err := ApplyFullSync(context.Background(), runner, pool, FullSyncRequest{
		Namespace: "default",
		Volume:    "demo",
		Source:    SourceRef{WebDAVURL: "http://source:8081"},
		Ownership: OwnershipPolicy{FileMode: &fileMode},
	})
	if err != nil {
		t.Fatalf("ApplyFullSync returned error: %v", err)
	}
	assertMode(t, target, 0o775)
}

func TestClearFullSyncTargetPreservesDirectoryInodes(t *testing.T) {
	target := t.TempDir()
	steamRoot := filepath.Join(target, "steam-root")
	saved := filepath.Join(steamRoot, "windrose", "WindowsServer", "R5", "Saved")
	if err := writeFile(filepath.Join(saved, "stale.sav"), "old"); err != nil {
		t.Fatal(err)
	}
	if err := writeFile(filepath.Join(steamRoot, "other.txt"), "old"); err != nil {
		t.Fatal(err)
	}
	steamRootInode := inodeOf(t, steamRoot)
	savedInode := inodeOf(t, saved)

	if err := clearFullSyncTarget(target, PathFilter{}); err != nil {
		t.Fatalf("clearFullSyncTarget returned error: %v", err)
	}

	if got := inodeOf(t, steamRoot); got != steamRootInode {
		t.Fatalf("steam-root inode changed: got %d, want %d", got, steamRootInode)
	}
	if got := inodeOf(t, saved); got != savedInode {
		t.Fatalf("Saved inode changed: got %d, want %d", got, savedInode)
	}
	if _, err := os.Stat(filepath.Join(saved, "stale.sav")); !os.IsNotExist(err) {
		t.Fatalf("stale file still exists or stat failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(steamRoot, "other.txt")); !os.IsNotExist(err) {
		t.Fatalf("top-level stale file still exists or stat failed: %v", err)
	}
}

func TestClearFullSyncTargetPreservesScopedDirectoryInodes(t *testing.T) {
	target := t.TempDir()
	steamRoot := filepath.Join(target, "steam-root")
	saved := filepath.Join(steamRoot, "windrose", "WindowsServer", "R5", "Saved")
	if err := writeFile(filepath.Join(saved, "stale.sav"), "old"); err != nil {
		t.Fatal(err)
	}
	if err := writeFile(filepath.Join(steamRoot, "keep.txt"), "not in scope"); err != nil {
		t.Fatal(err)
	}
	steamRootInode := inodeOf(t, steamRoot)
	savedInode := inodeOf(t, saved)

	if err := clearFullSyncTarget(target, PathFilter{
		IncludePaths: []string{"steam-root/windrose/WindowsServer/R5/Saved/**"},
	}); err != nil {
		t.Fatalf("clearFullSyncTarget returned error: %v", err)
	}

	if got := inodeOf(t, steamRoot); got != steamRootInode {
		t.Fatalf("steam-root inode changed: got %d, want %d", got, steamRootInode)
	}
	if got := inodeOf(t, saved); got != savedInode {
		t.Fatalf("Saved inode changed: got %d, want %d", got, savedInode)
	}
	if _, err := os.Stat(filepath.Join(saved, "stale.sav")); !os.IsNotExist(err) {
		t.Fatalf("scoped stale file still exists or stat failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(steamRoot, "keep.txt")); err != nil {
		t.Fatalf("out-of-scope file was removed: %v", err)
	}
}

func TestTarArchiveRoundTripPreservesFilesystemMetadata(t *testing.T) {
	source := t.TempDir()
	if err := os.MkdirAll(filepath.Join(source, "prefix", "empty"), 0o755); err != nil {
		t.Fatal(err)
	}
	executable := filepath.Join(source, "prefix", "bin", "launch.sh")
	if err := writeFile(executable, "run"); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(executable, 0o755); err != nil {
		t.Fatal(err)
	}
	regular := filepath.Join(source, "prefix", "config.json")
	if err := writeFile(regular, "{}"); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(regular, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../config.json", filepath.Join(source, "prefix", "bin", "config-link")); err != nil {
		t.Fatal(err)
	}

	var archive bytes.Buffer
	if err := WriteTarArchive(source, &archive, PathFilter{}); err != nil {
		t.Fatalf("WriteTarArchive returned error: %v", err)
	}
	target := t.TempDir()
	if err := ExtractTarArchive(target, &archive); err != nil {
		t.Fatalf("ExtractTarArchive returned error: %v", err)
	}
	assertMode(t, filepath.Join(target, "prefix", "bin", "launch.sh"), 0o755)
	assertMode(t, filepath.Join(target, "prefix", "config.json"), 0o640)
	assertMode(t, filepath.Join(target, "prefix", "empty"), 0o755)
	linkInfo, err := os.Lstat(filepath.Join(target, "prefix", "bin", "config-link"))
	if err != nil {
		t.Fatal(err)
	}
	if linkInfo.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("restored link mode = %s, want symlink", linkInfo.Mode())
	}
	linkTarget, err := os.Readlink(filepath.Join(target, "prefix", "bin", "config-link"))
	if err != nil {
		t.Fatal(err)
	}
	if linkTarget != "../config.json" {
		t.Fatalf("link target = %q, want ../config.json", linkTarget)
	}
}

func assertMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("%s mode = %o, want %o", path, got, want)
	}
}

func inodeOf(t *testing.T, path string) uint64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("stat(%s) did not return syscall.Stat_t", path)
	}
	return stat.Ino
}

func writeFile(path, content string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(content), 0o644)
}
