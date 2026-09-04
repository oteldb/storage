#!/usr/bin/env bash
# Runs the crash-model conformance suite against a real kernel.
#
# It validates internal/vfs/faultfs's Crash() against ext4 on a dm-flakey device that drops writes
# — a simulated power cut. That needs Linux, root, device-mapper and mkfs.ext4, and it creates
# loopback devices, so it is opt-in: an ordinary `go test ./...` does not compile it (the file is
# behind `//go:build linux && crashmodel`).
#
# The model half of the same scenario table runs unconditionally as TestModel.

set -euo pipefail

cd "$(dirname "$0")/.."

pkg=./internal/vfs/crashmodel
args=(test -tags crashmodel -count 1 -v -timeout 20m "$pkg")

if [[ "$(uname -s)" != "Linux" ]]; then
	echo "crash-model conformance needs Linux; this is $(uname -s)" >&2
	exit 1
fi

if [[ "$(id -u)" -eq 0 ]]; then
	exec go "${args[@]}"
fi

if ! command -v sudo >/dev/null; then
	echo "crash-model conformance needs root and sudo is not available; re-run as root" >&2
	exit 1
fi

# -E keeps GOCACHE/GOPATH so the build does not restart from scratch as root; PATH is passed
# explicitly because sudo resets it and go may not be on root's.
exec sudo -E env "PATH=$PATH" go "${args[@]}"
