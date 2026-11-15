#!/usr/bin/env bash
# Runs opengrep (https://github.com/opengrep/opengrep) over the reactor with the rules in .opengrep/rules: the
# CLAUDE.md conventions a bytecode census cannot see (test sources, workflow YAML, Studio JS, a source-only Java
# spelling). Java main-code semantics stay in the census module.
#
# Modes:
#   check [files...]  blocking: the ERROR rules over the given files (pre-commit) or the whole reactor; exit 1 on a finding
#   advise            advisory: the WARNING rules over the whole reactor; always exits 0
#   test              the rules' self-tests in .opengrep/tests: every rule must still see the shape it forbids
#
# The binary is a CI/pre-commit tool, never a dependency of any artifact. It is pinned by version and sha256 and
# cached under ${OPENGREP_CACHE_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/arcadedb-opengrep}.
set -euo pipefail

OPENGREP_VERSION=2.0.0

platform="$(uname -s)-$(uname -m)"
case "$platform" in
  Linux-x86_64)
    asset=opengrep_linux_x86
    digest=37e93f4da5fd04a18c5c1febbdd12f7e98dfccd4d0c9cf2593bcb25eeb462da0
    ;;
  Linux-aarch64)
    asset=opengrep_linux_aarch64
    digest=6257b65423a744b704af2ff9beeb8f222c2f6925236af386b56b031d4c06d6d8
    ;;
  Darwin-arm64)
    asset=opengrep_osx_arm64
    digest=b51b3d83f165a2e0934497b8f1b08abb1c3041ef20ef911c97e04cb39af1ad55
    ;;
  Darwin-x86_64)
    asset=opengrep_osx_x86
    digest=51d2f918d7aad0afc1426bb4fbf56e32fc70c3ffd5ddb78e32e7d61e51c029a2
    ;;
  *)
    asset=
    digest=
    ;;
esac

if [ -z "$asset" ]; then
  echo "opengrep: no pinned binary for $platform; put opengrep $OPENGREP_VERSION on PATH as 'opengrep'" >&2
  if command -v opengrep >/dev/null 2>&1 && [ "$(opengrep --version 2>/dev/null)" = "$OPENGREP_VERSION" ]; then
    bin="$(command -v opengrep)"
  else
    exit 1
  fi
else
  cache_dir="${OPENGREP_CACHE_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/arcadedb-opengrep}/$OPENGREP_VERSION"
  bin="$cache_dir/opengrep"
  if [ ! -x "$bin" ]; then
    mkdir -p "$cache_dir"
    curl -fsSL -o "$bin.tmp" "https://github.com/opengrep/opengrep/releases/download/v$OPENGREP_VERSION/$asset"
    if [ "$(uname -s)" = Darwin ]; then
      actual="$(shasum -a 256 "$bin.tmp" | cut -d' ' -f1)"
    else
      actual="$(sha256sum "$bin.tmp" | cut -d' ' -f1)"
    fi
    if [ "$actual" != "$digest" ]; then
      rm -f "$bin.tmp"
      echo "opengrep: sha256 mismatch for $asset: expected $digest, got $actual" >&2
      exit 1
    fi
    chmod +x "$bin.tmp"
    mv "$bin.tmp" "$bin"
  fi
fi

cd "$(dirname "$0")/../.."

mode="${1:-check}"
[ $# -gt 0 ] && shift

case "$mode" in
  check)
    exec "$bin" scan --quiet --config .opengrep/rules --severity ERROR --error "${@:-.}"
    ;;
  advise)
    "$bin" scan --quiet --config .opengrep/rules --severity WARNING . || true
    exit 0
    ;;
  test)
    exec "$bin" test --config .opengrep/rules .opengrep/tests
    ;;
  *)
    echo "usage: $0 [check [files...]|advise|test]" >&2
    exit 2
    ;;
esac
