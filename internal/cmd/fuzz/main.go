// Command fuzz starts a fuzzing campaign for a randomly chosen fuzz target
// using gosentry (https://github.com/trailofbits/gosentry), Trail of Bits'
// Go-toolchain fork that runs `go test -fuzz` on top of LibAFL.
//
// It scans the module for Go fuzz targets (func FuzzXxx(f *testing.F)), picks
// one at random, and invokes the gosentry toolchain with LibAFL enabled. The
// gosentry binary, fuzz duration, target selection, and the LibAFL knobs are
// all configurable via flags; any trailing arguments after `--` are passed
// through verbatim to `go test`.
//
// Examples:
//
//	# Fuzz a random target for one minute with the default gosentry toolchain.
//	go run ./internal/cmd/fuzz -fuzztime=1m
//
//	# List every discovered target without running anything.
//	go run ./internal/cmd/fuzz -list
//
//	# Fuzz a specific target with grammar-based mutation.
//	go run ./internal/cmd/fuzz -target=FuzzManifestDecode -grammar=testdata/JSON.json
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"syscall"

	"github.com/go-faster/errors"
)

// target is a single discovered fuzz function and the package directory that
// contains it, relative to the module root.
type target struct {
	Name string // e.g. "FuzzManifestDecode"
	Dir  string // module-relative package dir, e.g. "block" or "." for root
}

// Pkg returns the `go test` package pattern for the target's directory.
func (t target) Pkg() string {
	if t.Dir == "." || t.Dir == "" {
		return "."
	}
	return "./" + filepath.ToSlash(t.Dir)
}

func (t target) String() string { return t.Name + " (" + t.Pkg() + ")" }

// fuzzFuncRe matches top-level Go fuzz target declarations.
var fuzzFuncRe = regexp.MustCompile(`^func (Fuzz[A-Za-z0-9_]*)\(`)

type config struct {
	root      string
	goBin     string
	goroot    string
	gocache   string
	targetSel string
	pkgSel    string
	fuzztime  string
	seed      int64
	list      bool
	dryRun    bool
	verbose   bool

	// LibAFL knobs. All three booleans are required by gosentry in LibAFL
	// mode and default to false to mirror its documented invocation.
	useLibAFL      bool
	focusOnNewCode bool
	catchRaces     bool
	catchLeaks     bool
	grammar        string
	libaflConfig   string

	passthrough []string // args after "--"
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fuzz: %+v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		return err
	}

	targets, err := discover(cfg.root)
	if err != nil {
		return errors.Wrap(err, "discover targets")
	}
	targets = filterTargets(targets, cfg.pkgSel)
	if len(targets) == 0 {
		return errors.New("no fuzz targets found")
	}

	if cfg.list {
		for _, t := range targets {
			_, _ = fmt.Fprintln(os.Stdout, t)
		}
		return nil
	}

	t, err := pick(targets, cfg.targetSel, cfg.seed)
	if err != nil {
		return err
	}

	args := buildArgs(cfg, t)

	_, _ = fmt.Fprintf(os.Stderr, "fuzz: selected %s\nfuzz: %s %s\n",
		t, cfg.goBin, strings.Join(args, " "))
	if cfg.dryRun {
		return nil
	}

	// Forward Ctrl-C/SIGTERM to the child so a campaign stops cleanly and persists its corpus.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cmd := exec.CommandContext(ctx, cfg.goBin, args...)
	cmd.Dir = cfg.root
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	cmd.Env = buildEnv(cfg)
	// CommandContext kills with SIGKILL on cancellation by default; send SIGINT instead so
	// the fuzzer shuts down gracefully and writes its corpus.
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}

		return cmd.Process.Signal(syscall.SIGINT)
	}

	if err := cmd.Run(); err != nil {
		return errors.Wrap(err, "fuzzing failed")
	}

	return nil
}

func parseFlags(argv []string) (config, error) {
	var cfg config
	fs := flag.NewFlagSet("fuzz", flag.ContinueOnError)
	fs.StringVar(&cfg.root, "C", ".", "module root directory to scan and run in")
	fs.StringVar(&cfg.goBin, "go", "gosentry", "go toolchain binary to invoke (gosentry for LibAFL)")
	fs.StringVar(&cfg.goroot, "goroot", "", "GOROOT for the toolchain (default: let the -go binary self-detect)")
	fs.StringVar(&cfg.gocache, "gocache", "", "GOCACHE for the run (default: inherit)")
	fs.StringVar(&cfg.targetSel, "target", "", "fuzz target to run (default: random)")
	fs.StringVar(&cfg.pkgSel, "pkg", "", "restrict selection to package dirs containing this substring")
	fs.StringVar(&cfg.fuzztime, "fuzztime", "", "duration to run, e.g. 30s or 1m (default: until interrupted)")
	fs.Int64Var(&cfg.seed, "seed", 0, "RNG seed for target selection (default: nondeterministic)")
	fs.BoolVar(&cfg.list, "list", false, "list discovered targets and exit")
	fs.BoolVar(&cfg.dryRun, "dry-run", false, "print the command without running it")
	fs.BoolVar(&cfg.verbose, "v", false, "pass -v to go test")

	fs.BoolVar(&cfg.useLibAFL, "use-libafl", true, "use the LibAFL engine (false for Go's native fuzzer)")
	fs.BoolVar(&cfg.focusOnNewCode, "focus-on-new-code", false, "LibAFL: focus mutation on newly covered code")
	fs.BoolVar(&cfg.catchRaces, "catch-races", false, "LibAFL: detect data races")
	fs.BoolVar(&cfg.catchLeaks, "catch-leaks", false, "LibAFL: detect goroutine leaks")
	fs.StringVar(&cfg.grammar, "grammar", "", "LibAFL: Nautilus grammar JSON file (enables grammar fuzzing)")
	fs.StringVar(&cfg.libaflConfig, "libafl-config", "", "LibAFL: path to a tuning config (jsonc)")

	fs.Usage = func() {
		_, _ = fmt.Fprintf(fs.Output(), "Usage: fuzz [flags] [-- extra go-test args]\n\n"+
			"Starts a gosentry+LibAFL fuzzing campaign for a random fuzz target.\n\nFlags:\n")
		fs.PrintDefaults()
	}

	// Split off pass-through args after a bare "--".
	for i, a := range argv {
		if a == "--" {
			cfg.passthrough = argv[i+1:]
			argv = argv[:i]
			break
		}
	}
	if err := fs.Parse(argv); err != nil {
		return cfg, err
	}

	abs, err := filepath.Abs(cfg.root)
	if err != nil {
		return cfg, errors.Wrap(err, "resolve root")
	}
	cfg.root = abs
	return cfg, nil
}

// discover walks the module rooted at dir and returns every fuzz target it
// finds, deduplicated and sorted for deterministic listing.
func discover(dir string) ([]target, error) {
	var targets []target
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			// Skip vendored, hidden, and reference trees.
			if path != dir && (name == "_ref" || name == "vendor" || name == "testdata" ||
				strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_")) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		found, err := scanFile(path)
		if err != nil {
			return errors.Wrapf(err, "scan %s", path)
		}
		rel, err := filepath.Rel(dir, filepath.Dir(path))
		if err != nil {
			return err
		}
		for _, name := range found {
			targets = append(targets, target{Name: name, Dir: rel})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].Dir != targets[j].Dir {
			return targets[i].Dir < targets[j].Dir
		}
		return targets[i].Name < targets[j].Name
	})
	return targets, nil
}

// scanFile returns the names of fuzz targets declared in a single test file.
func scanFile(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	var names []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		if m := fuzzFuncRe.FindStringSubmatch(sc.Text()); m != nil {
			names = append(names, m[1])
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return names, nil
}

func filterTargets(targets []target, pkg string) []target {
	if pkg == "" {
		return targets
	}
	var out []target
	for _, t := range targets {
		if strings.Contains(t.Dir, pkg) {
			out = append(out, t)
		}
	}
	return out
}

// pick chooses a target by name, or at random when no name is given.
func pick(targets []target, name string, seed int64) (target, error) {
	if name != "" {
		var matches []target
		for _, t := range targets {
			if t.Name == name {
				matches = append(matches, t)
			}
		}
		switch len(matches) {
		case 0:
			return target{}, errors.Errorf("target %q not found", name)
		case 1:
			return matches[0], nil
		default:
			return target{}, errors.Errorf("target %q is ambiguous across %d packages; use -pkg to disambiguate", name, len(matches))
		}
	}
	var rng *rand.Rand
	if seed != 0 {
		rng = rand.New(rand.NewSource(seed))
	} else {
		rng = rand.New(rand.NewSource(rand.Int63()))
	}
	return targets[rng.Intn(len(targets))], nil
}

// buildEnv constructs the child environment.
//
// It strips any inherited GOROOT so the chosen toolchain self-detects its own
// root from its binary location. This matters for gosentry: its `go` driver is
// a go1.27-devel fork, but a stale `GOROOT=/usr/local/go` in the ambient
// environment would pin the stock go1.26.3 `compile` tool, yielding
// "version does not match go tool version" errors. A `-goroot` override is
// honored when set.
func buildEnv(cfg config) []string {
	env := os.Environ()
	out := env[:0:0] // copy to avoid mutating the shared backing array
	for _, kv := range env {
		switch {
		case strings.HasPrefix(kv, "GOROOT="):
			continue // dropped; re-added below if overridden
		case strings.HasPrefix(kv, "GOCACHE=") && cfg.gocache != "":
			continue // replaced below
		case strings.HasPrefix(kv, "CGO_ENABLED="):
			continue // forced on below
		}
		out = append(out, kv)
	}
	if cfg.goroot != "" {
		out = append(out, "GOROOT="+cfg.goroot)
	}
	if cfg.gocache != "" {
		out = append(out, "GOCACHE="+cfg.gocache)
	}
	// gosentry's LibAFL harness exports C ABI symbols, so cgo is required.
	out = append(out, "CGO_ENABLED=1")
	return out
}

// buildArgs assembles the `go test` argument list for the chosen target.
func buildArgs(cfg config, t target) []string {
	args := []string{"test", "-run=^$", "-fuzz=^" + t.Name + "$"}
	if cfg.verbose {
		args = append(args, "-v")
	}
	if cfg.fuzztime != "" {
		args = append(args, "-fuzztime="+cfg.fuzztime)
	}
	if cfg.useLibAFL {
		// gosentry requires these three booleans to be set explicitly in
		// LibAFL mode; pass them through with the configured values.
		args = append(args,
			boolFlag("focus-on-new-code", cfg.focusOnNewCode),
			boolFlag("catch-races", cfg.catchRaces),
			boolFlag("catch-leaks", cfg.catchLeaks),
		)
		if cfg.grammar != "" {
			args = append(args, "--use-grammar", "--grammar="+cfg.grammar)
		}
		if cfg.libaflConfig != "" {
			args = append(args, "--libafl-config="+cfg.libaflConfig)
		}
	} else {
		args = append(args, "-use-libafl=false")
	}
	args = append(args, cfg.passthrough...)
	args = append(args, t.Pkg())
	return args
}

func boolFlag(name string, v bool) string {
	if v {
		return "--" + name + "=true"
	}
	return "--" + name + "=false"
}
