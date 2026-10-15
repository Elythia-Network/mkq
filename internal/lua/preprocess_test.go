package lua

import (
	"strings"
	"testing"
	"testing/fstest"
)

func TestPreprocess_VendoredEntryPoints(t *testing.T) {
	t.Parallel()

	for _, name := range []string{
		"addStandardJob-9",
		"addDelayedJob-6",
		"addPrioritizedJob-9",
		"moveToActive-11",
		"moveToFinished-14",
		"extendLock-2",
		"releaseLock-1",
		"retryJob-11",
		"moveToDelayed-11",
		"moveStalledJobsToWait-9",
		"addJobScheduler-11",
		"updateJobScheduler-12",
		"updateProgress-3",
		"updateData-1",
		"addLog-2",
		"getCounts-1",
		"getRanges-1",
		"removeJob-2",
		"drain-5",
		"promote-9",
		"reprocessJob-9",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			out, err := Preprocess(name)
			if err != nil {
				t.Fatalf("preprocess %s: %v", name, err)
			}
			if strings.Contains(out, "@include") {
				t.Errorf("preprocess %s left an @include directive unresolved", name)
			}
			if out == "" {
				t.Errorf("preprocess %s produced empty output", name)
			}
		})
	}
}

func TestPreprocess_ResolvesNestedAndDeduplicates(t *testing.T) {
	t.Parallel()

	fsys := fstest.MapFS{
		"scripts/main.lua": &fstest.MapFile{Data: []byte(
			"-- main\n" +
				`--- @include "includes/a"` + "\n" +
				`--- @include "includes/b"` + "\n" +
				"-- end\n",
		)},
		"scripts/includes/a.lua": &fstest.MapFile{Data: []byte(
			"-- a\n" +
				`--- @include "shared"` + "\n",
		)},
		"scripts/includes/b.lua": &fstest.MapFile{Data: []byte(
			"-- b\n" +
				`--- @include "shared"` + "\n",
		)},
		"scripts/includes/shared.lua": &fstest.MapFile{Data: []byte(
			"-- shared\n",
		)},
	}

	got, err := preprocess(fsys, "main")
	if err != nil {
		t.Fatalf("preprocess: %v", err)
	}

	if c := strings.Count(got, "-- shared"); c != 1 {
		t.Errorf("shared include should appear once, got %d times in:\n%s", c, got)
	}
	if !strings.Contains(got, "-- a") || !strings.Contains(got, "-- b") {
		t.Errorf("missing include bodies in:\n%s", got)
	}
	if strings.Contains(got, "@include") {
		t.Errorf("unresolved directive in:\n%s", got)
	}
}

func TestPreprocess_RejectsCycles(t *testing.T) {
	t.Parallel()

	fsys := fstest.MapFS{
		"scripts/main.lua": &fstest.MapFile{Data: []byte(
			`--- @include "includes/a"` + "\n",
		)},
		"scripts/includes/a.lua": &fstest.MapFile{Data: []byte(
			`--- @include "b"` + "\n",
		)},
		"scripts/includes/b.lua": &fstest.MapFile{Data: []byte(
			`--- @include "a"` + "\n",
		)},
	}

	if _, err := preprocess(fsys, "main"); err == nil {
		t.Fatal("expected cycle error, got nil")
	} else if !strings.Contains(err.Error(), "cycle") {
		t.Errorf("error should mention cycle, got: %v", err)
	}
}

func TestPreprocess_RejectsTraversal(t *testing.T) {
	t.Parallel()

	fsys := fstest.MapFS{
		"scripts/main.lua": &fstest.MapFile{Data: []byte(
			`--- @include "../escape"` + "\n",
		)},
	}

	if _, err := preprocess(fsys, "main"); err == nil {
		t.Fatal("expected traversal error, got nil")
	}
}
