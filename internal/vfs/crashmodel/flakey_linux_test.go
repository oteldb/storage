//go:build linux && crashmodel

// This file is the dm-flakey half of the crash-model conformance suite. It is doubly gated —
// GOOS=linux and the `crashmodel` build tag — so an ordinary `go test ./...` never compiles it,
// let alone runs it. It needs root, device-mapper and mkfs.ext4; run it with
// scripts/crashmodel-test.sh.

package crashmodel_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/oteldb/storage/internal/vfs"
	"github.com/oteldb/storage/internal/vfs/crashmodel"
)

// imgSize is the backing image for one scenario. It is sparse, and mkfs over it is the slowest step
// in a run, so it is small — the scenarios write bytes, not megabytes.
const imgSize = 256 << 20

// commitDelay keeps ext4's journal thread from committing on its own timer while a scenario runs.
// The scenarios take milliseconds; without this the default 5s commit would make everything durable
// and the suite would prove nothing.
const commitDelay = "commit=1000"

// TestRealKernel drives the same scenario table against ext4 on a dm-flakey device, cutting the
// power by telling the device to drop every write, then unmounting and remounting.
//
// It asserts the outcome is one the scenario permits and reports, rather than fails, where the
// kernel kept more than the model does: the model is deliberately the most pessimistic legal
// outcome, so it being stricter is the design, and it being *looser* anywhere is the bug this test
// exists to find.
//
//nolint:paralleltest // each subtest owns a loopback device, a dm target and a mount; running them at once multiplies the kernel resources at stake and makes a failure impossible to attribute.
func TestRealKernel(t *testing.T) {
	requirePrerequisites(t)

	var diverged []string

	for _, s := range crashmodel.Scenarios() {
		t.Run(s.Name, func(t *testing.T) {
			dev := newFlakey(t)

			live := dev.mount(t, commitDelay)
			s.Run(t, live.fs)
			// Release the root handle but leave the filesystem mounted: the unmount has to happen
			// *after* the device starts dropping writes, so its flush goes nowhere.
			require.NoError(t, live.fs.Close())

			dev.cutPower(t)

			after := dev.mount(t, "")
			defer after.close(t)

			diverged = append(diverged, s.CheckReal(t, after.fs)...)
		})
	}

	if len(diverged) > 0 {
		t.Logf("the kernel kept more than the model does in %d place(s); this is legal pessimism, "+
			"not a failure:\n\t%s", len(diverged), strings.Join(diverged, "\n\t"))
	}
}

// requirePrerequisites skips — never fails — when the machine cannot host the harness.
func requirePrerequisites(t *testing.T) {
	t.Helper()

	if os.Getuid() != 0 {
		t.Skip("crash-model conformance needs root: it creates a loopback device, a dm-flakey " +
			"target and an ext4 mount (run scripts/crashmodel-test.sh)")
	}

	if _, err := os.Stat("/dev/mapper/control"); err != nil {
		t.Skipf("crash-model conformance needs device-mapper: /dev/mapper/control: %v", err)
	}

	for _, bin := range []string{"dmsetup", "losetup", "blockdev", "mkfs.ext4", "mount"} {
		if _, err := exec.LookPath(bin); err != nil {
			t.Skipf("crash-model conformance needs %s in PATH: %v", bin, err)
		}
	}
}

// flakey is one scenario's disk: a sparse ext4 image on a loopback device behind a dm-flakey target
// that can be told to silently drop every write.
type flakey struct {
	name    string
	img     string
	loop    string
	sectors string
	dir     string
}

// devices numbers the device-mapper targets within a process, so two scenarios never collide.
var devices atomic.Int64

// newFlakey builds the device and registers every teardown step as it succeeds, so a failure
// half-way leaves nothing behind. It refuses to proceed if the target name is already taken: the
// harness must only ever touch devices it created itself.
func newFlakey(t *testing.T) *flakey {
	t.Helper()

	d := &flakey{
		name: "oteldb-crashmodel-" + strconv.Itoa(os.Getpid()) + "-" + strconv.FormatInt(devices.Add(1), 10),
		dir:  t.TempDir(),
	}
	d.img = filepath.Join(d.dir, "fs.img")

	_, err := os.Stat("/dev/mapper/" + d.name)
	require.Truef(t, os.IsNotExist(err), "device-mapper target %s already exists; refusing to touch it", d.name)

	f, err := os.Create(d.img)
	require.NoError(t, err)
	require.NoError(t, f.Truncate(imgSize))
	require.NoError(t, f.Close())

	run(t, "mkfs.ext4", "-q", "-F", d.img)

	d.loop = strings.TrimSpace(run(t, "losetup", "--find", "--show", d.img))
	require.NotEmpty(t, d.loop, "losetup returned no device")

	t.Cleanup(func() { assert.NoError(t, retry(func() error { return try("losetup", "-d", d.loop) })) })

	d.sectors = strings.TrimSpace(run(t, "blockdev", "--getsz", d.loop))

	run(t, "dmsetup", "create", d.name, "--table", d.table(true))
	t.Cleanup(func() { assert.NoError(t, retry(func() error { return try("dmsetup", "remove", d.name) })) })

	return d
}

// table is the dm-flakey table. The device alternates an "up" and a "down" interval; parking it at
// up=3600/down=0 passes writes through, and up=0/down=3600 with the drop_writes feature swallows
// them silently while reads keep working — a power failure, from the filesystem's point of view.
func (d *flakey) table(up bool) string {
	if up {
		return "0 " + d.sectors + " flakey " + d.loop + " 0 3600 0"
	}

	return "0 " + d.sectors + " flakey " + d.loop + " 0 0 3600 1 drop_writes"
}

func (d *flakey) path() string { return "/dev/mapper/" + d.name }

// reload swaps the device's table under a suspend/resume. --nolockfs is deliberate: flushing the
// filesystem first is the opposite of the failure being simulated.
func (d *flakey) reload(t *testing.T, up bool) {
	t.Helper()

	run(t, "dmsetup", "suspend", "--nolockfs", d.name)
	run(t, "dmsetup", "load", d.name, "--table", d.table(up))
	run(t, "dmsetup", "resume", d.name)
}

// cutPower drops writes, unmounts (so the kernel flushes into a device that discards it), then lets
// writes through again for the remount that follows.
func (d *flakey) cutPower(t *testing.T) {
	t.Helper()

	d.reload(t, false)
	require.NoError(t, unmountPath(d.mountpoint()))
	d.reload(t, true)
}

func (d *flakey) mountpoint() string { return filepath.Join(d.dir, "mnt") }

// mounted is the filesystem mounted on the device, seen through the same [vfs.FS] the scenarios
// drive the fake through.
type mounted struct {
	point string
	fs    vfs.FS
}

func (d *flakey) mount(t *testing.T, opts string) mounted {
	t.Helper()

	point := d.mountpoint()
	require.NoError(t, os.MkdirAll(point, 0o750))
	run(t, "mount", "-o", opts, d.path(), point)
	t.Cleanup(func() { assert.NoError(t, unmountPath(point)) })

	fsys, err := vfs.OpenRoot(point, 0o750)
	require.NoError(t, err)

	return mounted{point: point, fs: fsys}
}

// close releases the root handle and unmounts. The handle has to go first or the unmount is busy.
func (m mounted) close(t *testing.T) {
	t.Helper()

	assert.NoError(t, m.fs.Close())
	assert.NoError(t, unmountPath(m.point))
}

// unmountPath unmounts, tolerating "not mounted" so the cleanup paths are idempotent, and retrying
// a busy mount because the unmount can race a kernel thread still holding the superblock.
func unmountPath(point string) error {
	return retry(func() error {
		switch err := unix.Unmount(point, 0); err {
		case nil, unix.EINVAL, unix.ENOENT:
			return nil
		default:
			return errors.Wrapf(err, "unmount %s", point)
		}
	})
}

// retry gives a device-mapper or mount operation a few seconds to stop being busy.
func retry(op func() error) error {
	var err error
	for range 20 {
		if err = op(); err == nil {
			return nil
		}

		if !strings.Contains(err.Error(), "busy") && !errors.Is(err, unix.EBUSY) {
			return err
		}

		time.Sleep(250 * time.Millisecond)
	}

	return err
}

func run(t *testing.T, name string, args ...string) string {
	t.Helper()

	out, err := command(name, args...)
	require.NoErrorf(t, err, "%s %s: %s", name, strings.Join(args, " "), out)

	return string(out)
}

func try(name string, args ...string) error {
	out, err := command(name, args...)
	if err != nil {
		return errors.Wrapf(err, "%s %s: %s", name, strings.Join(args, " "), out)
	}

	return nil
}

// command runs one tool. The context is not the test's: these run from t.Cleanup too, where the
// test's context is already canceled, and a torn-down device is worse than a slow one. The timeout
// is only there so a wedged dmsetup fails the run instead of hanging it.
func command(name string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	return exec.CommandContext(ctx, name, args...).CombinedOutput()
}
