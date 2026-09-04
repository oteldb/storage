// Package crashmodel is the conformance suite for the crash model itself.
//
// [github.com/oteldb/storage/internal/vfs/faultfs.FS.Crash] implements a rule — a file sync commits
// bytes, only a directory sync commits the name that reaches them — which the tree asserts rather
// than verifies, and every durability claim built on the fake inherits that assertion. So the
// scenarios live here once and are driven twice: against the fake, and against a real ext4
// filesystem on a dm-flakey device that drops writes. The second driver is behind the `crashmodel`
// build tag, because it needs Linux, root and device-mapper.
//
// The two drivers are held to different standards on purpose, because "the model and the kernel
// agree" is not the property worth having. The model must be *exact*: it is deterministic, and a
// deterministic answer that drifts is a broken fake. The kernel is held to the set of outcomes a
// scenario declares legal, because a real filesystem is free to persist more than the model keeps
// — ext4's fsync commits the whole journal, so syncing a file usually publishes its name too — and
// being stricter than the disk is exactly what makes the fake safe to test against. What the kernel
// may never do is lose something a scenario calls durable or resurrect something it calls erased.
// Those are the assertions that would catch a wrong model, and they name specific files and exact
// bytes rather than checking that the filesystem merely looks intact.
package crashmodel

import (
	"io/fs"
	"slices"
	"strings"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/internal/vfs"
)

// Scenario is one crash story: operations to perform, then the power cut, then what must be left.
type Scenario struct {
	// Name identifies the scenario and names its subtest.
	Name string
	// Why states the distinction the scenario pins, so a failure says what broke rather than which
	// byte differed.
	Why string
	// Run performs the operations. The power cut happens when it returns.
	Run func(t *testing.T, f vfs.FS)
	// Expect covers every name the scenario touched, including the ones that must be gone.
	Expect []Expect
}

// Expect is one name's post-crash outcome: what the model must say, and what a real filesystem is
// allowed to say.
type Expect struct {
	// Name is the path, relative to the filesystem root.
	Name string
	// Present and Data are what the model must produce, exactly.
	Present bool
	Data    string
	// Allow lists the contents a real filesystem may come back with; AllowAbsent adds "the name is
	// gone". An empty Allow with AllowAbsent false is unsatisfiable and never written.
	Allow       []string
	AllowAbsent bool
}

// legal reports whether a real filesystem's outcome is one the scenario permits.
func (e Expect) legal(data []byte, present bool) bool {
	if !present {
		return e.AllowAbsent
	}

	return slices.Contains(e.Allow, string(data))
}

// CheckModel asserts the crashed model filesystem is exactly what the scenario declares.
func (s Scenario) CheckModel(t *testing.T, after vfs.FS) {
	t.Helper()

	for _, e := range s.Expect {
		data, ok := read(t, after, e.Name)

		if !e.Present {
			assert.Falsef(t, ok, "%s: the model must not keep %s (%s); it came back %q",
				s.Name, e.Name, s.Why, data)

			continue
		}

		if assert.Truef(t, ok, "%s: the model must keep %s (%s)", s.Name, e.Name, s.Why) {
			assert.Equalf(t, e.Data, string(data), "%s: model contents of %s", s.Name, e.Name)
		}
	}
}

// CheckReal asserts the remounted filesystem reached an outcome the scenario permits, and returns
// the names where it disagreed with the model. A disagreement is not a failure — the model is
// deliberately the most pessimistic legal outcome — but it is what a reader of the run wants to
// see, so the caller logs it.
func (s Scenario) CheckReal(t *testing.T, after vfs.FS) []string {
	t.Helper()

	var diverged []string

	for _, e := range s.Expect {
		data, ok := read(t, after, e.Name)

		if !e.legal(data, ok) {
			t.Errorf("%s: %s came back %s, which the scenario does not permit (%s); legal outcomes: %s",
				s.Name, e.Name, state(ok, string(data)), s.Why, legalSet(e))

			continue
		}

		if ok != e.Present || (ok && string(data) != e.Data) {
			diverged = append(diverged, s.Name+": "+e.Name+
				" model="+state(e.Present, e.Data)+" kernel="+state(ok, string(data)))
		}
	}

	return diverged
}

func state(present bool, data string) string {
	if !present {
		return "absent"
	}

	return "present " + quote(data)
}

func legalSet(e Expect) string {
	var parts []string
	if e.AllowAbsent {
		parts = append(parts, "absent")
	}

	for _, a := range e.Allow {
		parts = append(parts, "present "+quote(a))
	}

	return strings.Join(parts, ", ")
}

func quote(s string) string { return `"` + s + `"` }

// read returns name's contents and whether it exists, failing the test on any other error.
func read(t *testing.T, f vfs.FS, name string) ([]byte, bool) {
	t.Helper()

	data, err := f.ReadFile(name)

	switch {
	case err == nil:
		return data, true
	case errors.Is(err, fs.ErrNotExist):
		return nil, false
	default:
		require.NoErrorf(t, err, "read %s", name)

		return nil, false
	}
}
