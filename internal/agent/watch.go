package agent

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
)

type WatchConfig struct {
	Pool         Pool
	Namespace    string
	Volume       string
	IncludePaths []string
	ExcludePaths []string
	Debounce     time.Duration
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
					created, err := collectExistingEvents(root, event.Name, filter)
					if err != nil {
						return err
					}
					pending = append(pending, created...)
					if len(created) > 0 {
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
			timer.Reset(debounce)
		case <-timer.C:
			if err := flush(); err != nil {
				return err
			}
		}
	}
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

func collectExistingEvents(root, start string, filter PathFilter) ([]FileEvent, error) {
	events := make([]FileEvent, 0)
	err := filepath.WalkDir(start, func(p string, d os.DirEntry, err error) error {
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
			events = append(events, FileEvent{Path: rel, Op: EventOpUpsert})
		}
		return nil
	})
	return events, err
}

func relativeWatchPath(root, p string) (string, bool) {
	rel, err := filepath.Rel(root, p)
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", false
	}
	return filepath.ToSlash(rel), true
}
