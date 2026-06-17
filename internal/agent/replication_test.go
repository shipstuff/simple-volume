package agent

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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

func TestApplyEventBatchRunsCopyAndDelete(t *testing.T) {
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
	if _, err := os.Stat(deleteTarget); !os.IsNotExist(err) {
		t.Fatalf("delete target still exists")
	}
}

func TestApplyEventBatchRemovesTargetWhenSourceAlreadyGone(t *testing.T) {
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
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("target was not removed after missing source: %v", err)
	}
}

func TestApplyEventBatchRemovesTargetWhenSourceDisappearsAfterCopyFailure(t *testing.T) {
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
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("target was not removed after disappeared source: %v", err)
	}
}

func TestApplyEventBatchRemovesTargetForRcloneMissingSourceOutput(t *testing.T) {
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
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("target was not removed after rclone missing-source output: %v", err)
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
	if len(runner.specs) != 1 {
		t.Fatalf("ran %d commands, want 1", len(runner.specs))
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

func writeFile(path, content string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(content), 0o644)
}
