package agent

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
)

const maxPendingWatchEvents = 512
const defaultWatchRescanInterval = 30 * time.Second

type WatchConfig struct {
	Pool         Pool
	Namespace    string
	Volume       string
	IncludePaths []string
	ExcludePaths []string
	Debounce     time.Duration
	Rescan       time.Duration
}

type BatchSink func(context.Context, EventBatch) error

func WatchVolume(ctx context.Context, cfg WatchConfig, sink BatchSink) error {
	root, err := EnsureVolumePath(VolumePath{
		Pool:      cfg.Pool,
		Namespace: cfg.Namespace,
		Name:      cfg.Volume,
	}, 0o755)
	if err != nil {
		return err
	}
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()

	if err := addRecursiveWatches(watcher, root, PathFilter{IncludePaths: cfg.IncludePaths, ExcludePaths: cfg.ExcludePaths}); err != nil {
		return err
	}

	debounce := cfg.Debounce
	if debounce <= 0 {
		debounce = 5 * time.Second
	}
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	pending := make([]FileEvent, 0)
	filter := PathFilter{IncludePaths: cfg.IncludePaths, ExcludePaths: cfg.ExcludePaths}
	snapshot, err := snapshotWatchedFiles(root, filter)
	if err != nil {
		return err
	}
	rescan := cfg.Rescan
	if rescan <= 0 {
		rescan = defaultWatchRescanInterval
	}
	rescanTicker := time.NewTicker(rescan)
	defer rescanTicker.Stop()

	flush := func() error {
		events := CoalesceEvents(pending, filter)
		pending = pending[:0]
		if len(events) == 0 {
			return nil
		}
		return sink(ctx, EventBatch{
			Namespace:  cfg.Namespace,
			Volume:     cfg.Volume,
			Generation: time.Now().UTC().Format(time.RFC3339Nano),
			Events:     events,
		})
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-watcher.Errors:
			if err != nil {
				return err
			}
		case event := <-watcher.Events:
			rel, ok := relativeWatchPath(root, event.Name)
			if !ok {
				continue
			}
			if event.Has(fsnotify.Create) {
				if info, err := os.Stat(event.Name); err == nil && info.IsDir() && filter.ShouldTraverse(rel) {
					if err := addRecursiveWatches(watcher, event.Name, filter); err != nil {
						return err
					}
					collected := false
					if err := collectExistingEvents(root, event.Name, filter, func(event FileEvent) error {
						collected = true
						pending = append(pending, event)
						if len(pending) >= maxPendingWatchEvents {
							return flush()
						}
						return nil
					}); err != nil {
						return err
					}
					if collected && len(pending) > 0 {
						timer.Reset(debounce)
					}
					continue
				}
			}
			if !filter.ShouldReplicate(rel) {
				continue
			}
			op := EventOpUpsert
			if event.Has(fsnotify.Remove) || event.Has(fsnotify.Rename) {
				op = EventOpDelete
			}
			pending = append(pending, FileEvent{Path: rel, Op: op})
			if len(pending) >= maxPendingWatchEvents {
				if err := flush(); err != nil {
					return err
				}
				continue
			}
			timer.Reset(debounce)
		case <-timer.C:
			if err := flush(); err != nil {
				return err
			}
		case <-rescanTicker.C:
			current, err := snapshotWatchedFiles(root, filter)
			if err != nil {
				return err
			}
			events := diffWatchedSnapshots(snapshot, current)
			if len(events) == 0 {
				snapshot = current
				continue
			}
			if err := sink(ctx, EventBatch{
				Namespace:  cfg.Namespace,
				Volume:     cfg.Volume,
				Generation: time.Now().UTC().Format(time.RFC3339Nano),
				Events:     events,
			}); err != nil {
				return err
			}
			snapshot = current
		}
	}
}

type watchedFileState struct {
	Size    int64
	Mode    os.FileMode
	ModTime int64
	Link    string
}

func snapshotWatchedFiles(root string, filter PathFilter) (map[string]watchedFileState, error) {
	snapshot := make(map[string]watchedFileState)
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		rel, ok := relativeWatchPath(root, p)
		if !ok {
			return nil
		}
		if d.IsDir() {
			if rel != "." && !filter.ShouldTraverse(rel) {
				return filepath.SkipDir
			}
			return nil
		}
		if !filter.ShouldReplicate(rel) {
			return nil
		}
		info, err := os.Lstat(p)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		state := watchedFileState{
			Size:    info.Size(),
			Mode:    info.Mode(),
			ModTime: info.ModTime().UnixNano(),
		}
		if info.Mode()&os.ModeSymlink != 0 {
			link, err := os.Readlink(p)
			if err != nil {
				if os.IsNotExist(err) {
					return nil
				}
				return err
			}
			state.Link = link
		}
		snapshot[rel] = state
		return nil
	})
	return snapshot, err
}

func diffWatchedSnapshots(previous, current map[string]watchedFileState) []FileEvent {
	paths := make([]string, 0)
	for path, state := range current {
		if previous[path] != state {
			paths = append(paths, path)
		}
	}
	for path := range previous {
		if _, ok := current[path]; !ok {
			paths = append(paths, path)
		}
	}
	sort.Strings(paths)
	events := make([]FileEvent, 0, len(paths))
	for _, path := range paths {
		op := EventOpUpsert
		if _, ok := current[path]; !ok {
			op = EventOpDelete
		}
		events = append(events, FileEvent{Path: path, Op: op})
	}
	return events
}

func addRecursiveWatches(watcher *fsnotify.Watcher, root string, filter PathFilter) error {
	return filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		if rel, ok := relativeWatchPath(root, p); ok && rel != "." && !filter.ShouldTraverse(rel) {
			return filepath.SkipDir
		}
		return watcher.Add(p)
	})
}

func collectExistingEvents(root, start string, filter PathFilter, yield func(FileEvent) error) error {
	return filepath.WalkDir(start, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		rel, ok := relativeWatchPath(root, p)
		if !ok {
			return nil
		}
		if d.IsDir() {
			if rel != "." && !filter.ShouldTraverse(rel) {
				return filepath.SkipDir
			}
			return nil
		}
		if filter.ShouldReplicate(rel) {
			return yield(FileEvent{Path: rel, Op: EventOpUpsert})
		}
		return nil
	})
}

func relativeWatchPath(root, p string) (string, bool) {
	rel, err := filepath.Rel(root, p)
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", false
	}
	return filepath.ToSlash(rel), true
}
