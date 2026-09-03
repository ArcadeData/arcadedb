#!/usr/bin/env bash
#
# Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
# SPDX-License-Identifier: Apache-2.0
#
set -euo pipefail

# Usage: build-native.sh [options] [-- <extra maven args>]
#
# Builds the ArcadeDB GraalVM native image FOR THE MACHINE YOU ARE ON, wrapping the raw
#   ./mvnw -Pnative -pl native -am -DskipTests package
# from docs/native-image.md with the three preflight checks that turn this build's cryptic
# failure modes into an immediate, named error:
#
#   1. JAVA_HOME/GRAALVM_HOME actually point at a GraalVM home that contains bin/native-image.
#      native-maven-plugin resolves the builder from those variables, NOT by searching PATH, so a
#      version-manager shim that only puts `native-image` on PATH fails with "native-image is not
#      installed in your JAVA_HOME" after Maven has already resolved the whole reactor.
#   2. That builder is the version native/pom.xml pins its Truffle/polyglot artifacts to. A skew
#      there fails minutes into the build with `NoSuchMethodError:
#      OptimizedTruffleRuntime.getLoopNodeFactory()`, which names neither side of the mismatch.
#      This has shipped to main twice - see the long comment on native.graalvm.version in
#      native/pom.xml. Override with --allow-version-skew if you are deliberately testing another
#      builder.
#   3. For --link-mode musl-static, the musl toolchain AND a musl-built static libz are present
#      before the build starts, rather than failing at the final link step.
#
# Native Image cannot cross-compile: this produces a binary for the host OS/arch only. To build
# the LINUX binaries and the Docker images from a non-Linux machine, use build-native-docker.sh,
# which runs this same Maven build inside a Linux container.
#
# Options:
#   --link-mode <mode>     auto (default) | dynamic | musl-static | mostly-static
#                          auto picks the mode CI uses for this platform:
#                            linux/x86_64  -> musl-static   (fully static musl; runs on scratch)
#                            linux/aarch64 -> mostly-static (static except glibc; needs a glibc base)
#                            macOS/Windows -> dynamic       (the only mode those platforms have)
#                          On linux/x86_64 without a musl toolchain, auto degrades to dynamic with
#                          a warning rather than failing - a dynamic binary is still a usable local
#                          build, it just cannot go into the scratch Docker image.
#   --smoke                After building, run native/src/test/scripts/smoke.sh against the binary
#                          (boots the server, asserts HTTP/Studio/SQL/Cypher/JS round-trips).
#   --wire                 With --smoke, additionally enable every wire-protocol plugin and set
#                          WIRE_STRICT=1, matching what native-image.yml's two Linux legs assert.
#                          Needs psql/grpcurl/python3/xxd installed locally or the strict checks
#                          fail; without this flag those checks WARN-skip.
#   --allow-version-skew   Downgrade preflight check 2 from an error to a warning.
#   -h, --help             Print this help.
#
# Anything after a literal `--` is forwarded verbatim to Maven, e.g.
#   build-native.sh -- -o -Dnative.compile.threads=4
#
# Env:
#   GRAALVM_HOME / JAVA_HOME   GraalVM home. Either may be set; the script exports both to
#                              whichever one contains bin/native-image.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
NATIVE_POM="$REPO_ROOT/native/pom.xml"

LINK_MODE="auto"
RUN_SMOKE=0
WIRE=0
ALLOW_SKEW=0
MVN_EXTRA=()

log()  { echo "[build-native] $*"; }
fail() { echo "[build-native] ERROR: $*" >&2; exit 1; }

# Prints the header comment block above, from '# Usage:' to the first non-comment line, so the
# help text cannot drift out of sync with hardcoded line numbers.
usage() { awk '/^# Usage:/{f=1} f{ if ($0 !~ /^#/) exit; sub(/^# ?/,""); print }' "${BASH_SOURCE[0]}"; }

while [ $# -gt 0 ]; do
  case "$1" in
    --link-mode)         LINK_MODE="${2:-}"; shift 2 ;;
    --link-mode=*)       LINK_MODE="${1#*=}"; shift ;;
    --smoke)             RUN_SMOKE=1; shift ;;
    --wire)              WIRE=1; shift ;;
    --allow-version-skew) ALLOW_SKEW=1; shift ;;
    -h|--help)           usage; exit 0 ;;
    --)                  shift; MVN_EXTRA=("$@"); break ;;
    *)                   fail "unknown option '$1' (see --help)" ;;
  esac
done

case "$LINK_MODE" in
  auto|dynamic|musl-static|mostly-static) ;;
  *) fail "invalid --link-mode '$LINK_MODE' (auto|dynamic|musl-static|mostly-static)" ;;
esac

# ---------------------------------------------------------------------------
# 1. Locate the GraalVM home.
#
# native-maven-plugin reads GRAALVM_HOME first, then JAVA_HOME. Accept either as the input and
# export BOTH pointing at the one that actually has bin/native-image, so the plugin cannot pick
# up a different JDK than the one this script validated.
# ---------------------------------------------------------------------------
has_native_image() { [ -n "${1:-}" ] && [ -x "$1/bin/native-image" ]; }

GVM=""
for candidate in "${GRAALVM_HOME:-}" "${JAVA_HOME:-}"; do
  if has_native_image "$candidate"; then GVM="$candidate"; break; fi
done

if [ -z "$GVM" ]; then
  echo "[build-native] ERROR: no GraalVM home with bin/native-image found." >&2
  echo "  GRAALVM_HOME=${GRAALVM_HOME:-<unset>}" >&2
  echo "  JAVA_HOME=${JAVA_HOME:-<unset>}" >&2
  echo "" >&2
  echo "  native-maven-plugin resolves the builder from GRAALVM_HOME/JAVA_HOME, not from PATH:" >&2
  echo "  a jenv/sdkman shim that only exposes \`native-image\` on PATH is NOT enough. Point both" >&2
  echo "  at a real GraalVM home, e.g." >&2
  echo "    export JAVA_HOME=/path/to/graalvm-community-<version>/Contents/Home   # macOS" >&2
  echo "    export GRAALVM_HOME=\$JAVA_HOME" >&2
  echo "  See docs/native-image.md \"Prerequisites\"." >&2
  exit 1
fi

export GRAALVM_HOME="$GVM"
export JAVA_HOME="$GVM"
log "GraalVM home: $GVM"

# ---------------------------------------------------------------------------
# 2. Builder/Truffle pin check.
#
# native/pom.xml's native.graalvm.version pins graal-sdk/polyglot/js-language/truffle-* to the
# exact release the builder must be. Read the pin from the pom (single source of truth - the lint
# job already enforces that it matches native-image.yml) and look for it in the builder's own
# --version banner, which prints e.g.
#   GraalVM Runtime Environment GraalVM CE 25.0.2+10.1 (build ...)
# ---------------------------------------------------------------------------
PIN="$(sed -n 's:.*<native\.graalvm\.version>\(.*\)</native\.graalvm\.version>.*:\1:p' "$NATIVE_POM" | head -1)"
[ -n "$PIN" ] || fail "could not read <native.graalvm.version> from $NATIVE_POM"

NI_VERSION="$("$GVM/bin/native-image" --version 2>&1 || true)"
# The banner carries a build suffix - 25.0.2 reports "GraalVM CE 25.0.2+10.1", 25.2.4 reports
# "GraalVM CE 25.2.4+7.1" - so match the pin followed by a non-digit rather than the bare string,
# which would also accept a "25.0.20". Dots are escaped so they cannot match an arbitrary character.
if grep -qE "GraalVM CE ${PIN//./\\.}([^0-9]|\$)" <<<"$NI_VERSION"; then
  log "builder matches native/pom.xml pin (GraalVM CE $PIN)"
else
  {
    echo "[build-native] builder does NOT match native/pom.xml's <native.graalvm.version> ($PIN)."
    echo "  builder reports:"
    sed 's/^/    /' <<<"$NI_VERSION"
    echo ""
    echo "  A builder/Truffle skew fails this build minutes in, at feature registration, with"
    echo "    NoSuchMethodError: OptimizedTruffleRuntime.getLoopNodeFactory()"
    echo "  which names neither side of the mismatch. Install GraalVM CE $PIN, or pass"
    echo "  --allow-version-skew to proceed anyway. See docs/native-image.md \"Prerequisites\"."
  } >&2
  [ "$ALLOW_SKEW" = "1" ] || exit 1
  log "continuing despite the skew (--allow-version-skew)"
fi

# ---------------------------------------------------------------------------
# 3. Resolve the link mode, and preflight the musl toolchain if one is needed.
# ---------------------------------------------------------------------------
UNAME_S="$(uname -s)"
UNAME_M="$(uname -m)"

# Returns 0 when a musl toolchain with a musl-built static libz is usable for this host.
musl_ready() {
  local triplet="${UNAME_M}-linux-musl"
  command -v "${triplet}-gcc" >/dev/null 2>&1 || return 1
  "${triplet}-gcc" -print-file-name=libz.a 2>/dev/null | grep -q '/' || return 1
  return 0
}

if [ "$LINK_MODE" = "auto" ]; then
  case "$UNAME_S/$UNAME_M" in
    Linux/x86_64)
      if musl_ready; then
        LINK_MODE="musl-static"
      else
        LINK_MODE="dynamic"
        log "WARN: no musl toolchain with a musl-built static libz found; falling back to"
        log "WARN: --link-mode dynamic. CI's linux/amd64 leg builds musl-static, and only a"
        log "WARN: musl-static binary can go into the scratch Docker image. See"
        log "WARN: docs/native-image.md, or use build-native-docker.sh which sets the toolchain up."
      fi
      ;;
    Linux/aarch64|Linux/arm64) LINK_MODE="mostly-static" ;;
    *)                         LINK_MODE="dynamic" ;;
  esac
  log "auto-selected --link-mode $LINK_MODE for $UNAME_S/$UNAME_M"
fi

MODE_ARGS=()
case "$LINK_MODE" in
  musl-static)
    [ "$UNAME_S" = "Linux" ] || fail "--link-mode musl-static is Linux-only (host is $UNAME_S)"
    case "$UNAME_M" in
      aarch64|arm64)
        fail "--link-mode musl-static cannot link on aarch64: GraalVM CE ships no static musl JDK
  libraries for that architecture (oracle/graal#4645, closed not-planned). Use
  --link-mode mostly-static, which is what CI's linux/arm64 leg builds."
        ;;
    esac
    musl_ready || fail "--link-mode musl-static needs ${UNAME_M}-linux-musl-gcc and a musl-built
  static libz.a on its library path. Install musl-tools + musl-dev, then build zlib with
    CC=${UNAME_M}-linux-musl-gcc ./configure --static --prefix=/usr \\
      --includedir=/usr/include/${UNAME_M}-linux-musl --libdir=/usr/lib/${UNAME_M}-linux-musl
  musl-gcc's specs file restricts header/library search to that triplet directory and does NOT
  fall back to /usr/include or /usr/lib, so a plain --prefix=/usr install stays invisible to it.
  See docs/native-image.md, or use build-native-docker.sh which does all of this in a container."
    MODE_ARGS=(-Dnative.static=true)
    export CC="${UNAME_M}-linux-musl-gcc"
    ;;
  mostly-static)
    [ "$UNAME_S" = "Linux" ] || fail "--link-mode mostly-static is Linux-only (host is $UNAME_S)"
    MODE_ARGS=(-Dnative.mostlystatic=true)
    ;;
  dynamic) ;;
esac

# ---------------------------------------------------------------------------
# 4. Build. `package` is the phase that matters: native/pom.xml binds the plugin's
#    compile-no-fork goal to it, so `compile` resolves the reactor but produces no binary.
# ---------------------------------------------------------------------------
log "building (link mode: $LINK_MODE)"
# Run from the repository root, and select the module by artifactId rather than by path.
#
# `-pl <path>` is resolved relative to Maven's execution root, i.e. the directory Maven was
# launched from - NOT the directory holding the root pom. So `-pl native` from anywhere other
# than the repository root looks for <cwd>/native and dies with "Could not find the selected
# project in the reactor: native", which points at the reactor rather than at the caller's cwd.
# Running the script from native/ (./scripts/build-native.sh) hits this immediately.
#
# `-pl :arcadedb-native` selects by artifactId and is position-independent, so the two together
# make this work from any working directory.
cd "$REPO_ROOT"
set -x
# ${ARR[@]+"${ARR[@]}"} rather than a plain "${ARR[@]}": both arrays are empty in the common case
# (dynamic link mode, no forwarded Maven args) and macOS still ships bash 3.2, where expanding an
# empty array under `set -u` aborts with "unbound variable". native-image.yml carries the same
# caveat for its macOS leg.
"$REPO_ROOT/mvnw" -B -ntp -Pnative -pl :arcadedb-native -am -DskipTests \
  ${MODE_ARGS[@]+"${MODE_ARGS[@]}"} ${MVN_EXTRA[@]+"${MVN_EXTRA[@]}"} package
set +x

# ---------------------------------------------------------------------------
# 5. Locate the binary the same way native-image.yml does: by the executable bit, not by
#    reconstructing os-maven-plugin's os.detected.name/arch naming, and not by excluding known
#    non-binary extensions (native-image writes build-report files such as svm_err_*.md into the
#    same directory, and those match arcadedb-* too).
# ---------------------------------------------------------------------------
cd "$REPO_ROOT/native/target"
CANDIDATES=()
if [ "$UNAME_S" != "Linux" ] && [ "$UNAME_S" != "Darwin" ]; then
  while IFS= read -r f; do CANDIDATES+=("$f"); done \
    < <(find . -maxdepth 1 -type f -name 'arcadedb-*.exe' -print | sed 's|^\./||')
else
  while IFS= read -r f; do CANDIDATES+=("$f"); done \
    < <(find . -maxdepth 1 -type f -name 'arcadedb-*' -perm -u+x -print | sed 's|^\./||')
fi
[ "${#CANDIDATES[@]}" -eq 1 ] || fail "expected exactly one native binary in native/target, found ${#CANDIDATES[@]}"
BIN="$REPO_ROOT/native/target/${CANDIDATES[0]}"

log "binary: $BIN"
log "size:   $(du -h "$BIN" | cut -f1)"
if [ "$LINK_MODE" = "musl-static" ] && command -v ldd >/dev/null 2>&1; then
  log "ldd:    $(ldd "$BIN" 2>&1 | head -1)   # expected: 'not a dynamic executable'"
fi

# ---------------------------------------------------------------------------
# 6. Optional smoke test, reusing the exact script CI runs.
# ---------------------------------------------------------------------------
if [ "$RUN_SMOKE" = "1" ]; then
  SMOKE=("$REPO_ROOT/native/src/test/scripts/smoke.sh" "$BIN" -Dorg.jline.terminal.dumb=true)
  if [ "$WIRE" = "1" ]; then
    PLUGINS="Postgres:com.arcadedb.postgres.PostgresProtocolPlugin"
    PLUGINS="$PLUGINS,Redis:com.arcadedb.redis.RedisProtocolPlugin"
    PLUGINS="$PLUGINS,MongoDB:com.arcadedb.mongo.MongoDBProtocolPlugin"
    PLUGINS="$PLUGINS,Bolt:com.arcadedb.bolt.BoltProtocolPlugin"
    PLUGINS="$PLUGINS,Grpc:com.arcadedb.server.grpc.GrpcServerPlugin"
    SMOKE+=("-Darcadedb.server.plugins=$PLUGINS")
    log "running smoke test with every wire plugin and WIRE_STRICT=1"
    WIRE_STRICT=1 "${SMOKE[@]}"
  else
    log "running smoke test"
    "${SMOKE[@]}"
  fi
fi

log "done"
