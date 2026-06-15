package agent

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

type recordingRunner struct {
	specs []CommandSpec
}

func (r *recordingRunner) Run(_ context.Context, spec CommandSpec) error {
	r.specs = append(r.specs, spec)
	return nil
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

func writeFile(path, content string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(content), 0o644)
}
