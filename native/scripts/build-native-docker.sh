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

# Usage: build-native-docker.sh [options]
#
# Builds the native Docker image locally: the same per-arch image .github/workflows/native-image.yml
# publishes on a release, from your working tree, on your machine, without pushing anything.
#
# Native Image cannot cross-compile, so the LINUX binary the image needs cannot be built on macOS
# directly. This script therefore does what CI does across two jobs, in one pass:
#
#   1. builds a throwaway builder image (native/src/main/docker/Dockerfile.native-builder) holding
#      the GraalVM CE builder pinned by native/pom.xml, plus - for amd64 - the musl toolchain and
#      musl-built static zlib that a fully-static link needs;
#   2. runs the ordinary `./mvnw -Pnative -pl native -am -DskipTests package` INSIDE that image
#      with this repository bind-mounted, so the Linux binary lands in native/target on the host;
#   3. stages the build context exactly as native-image.yml's `docker` job does (binary + config,
#      plus a CA bundle for the scratch image) and builds the runtime image with buildx --load;
#   4. smoke-tests the resulting container with native/src/test/scripts/exercise.sh and scans its
#      startup log, the same two assertions CI makes.
#
# The runtime Dockerfiles are used unmodified, so what you get is the shipped image, not a
# local approximation:
#
#   amd64 -> fully static musl binary   -> Dockerfile.native.scratch      (FROM scratch)
#   arm64 -> mostly-static glibc binary -> Dockerfile.native.distroless   (distroless base)
#
# Nothing is pushed and nothing is tagged under a publishable name: the default tag ends in
# "-local" so it cannot be confused with a released arcadedata/arcadedb tag.
#
# Options:
#   --arch <amd64|arm64>   Target architecture. Defaults to the host's, because native-image under
#                          QEMU emulation is unusably slow; building the other arch needs
#                          --allow-emulation and a lot of patience.
#   --tag <tag>            Image tag to build. Default: arcadedb:<version>-native-<arch>-local
#   --skip-binary-build    Reuse the Linux binary already in native/target (steps 3-4 only).
#                          Useful when iterating on the Dockerfiles themselves.
#   --no-smoke             Build the image but do not run the container smoke test.
#   --port <port>          Host port the smoke test publishes 2480 on. Default 2480. A dev machine
#                          is not a clean CI runner: an IDE-launched server or a previous run
#                          already holding 2480 makes `docker run -p 2480:2480` fail, or - worse,
#                          if it started before the publish - makes exercise.sh assert against
#                          THAT server and pass while telling you nothing about this image.
#   --builder-memory <sz>  Cap the native-image builder heap, e.g. 10g, via NATIVE_IMAGE_OPTIONS
#                          (-J-Xmx). By default native-image sizes its own heap at ~80% of what it
#                          can see, which is the Docker VM's memory, not the host's.
#   --allow-low-memory     Proceed even though Docker has less memory than this build needs.
#   --rebuild-builder      Rebuild the builder image even if it already exists locally.
#   --allow-emulation      Permit --arch different from the host's (QEMU; expect hours, not minutes).
#   -h, --help             Print this help.
#
# Env:
#   ARCADEDB_NATIVE_M2   Host directory used as HOME for the containerised Maven build, so the
#                        local repository and the ./mvnw distribution survive between runs.
#                        Default: ~/.cache/arcadedb-native-m2
#
# First run downloads a GraalVM tarball and populates a fresh Maven repository, so budget a good
# while; later runs reuse both.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
NATIVE_POM="$REPO_ROOT/native/pom.xml"
DOCKER_DIR="$REPO_ROOT/native/src/main/docker"
M2_DIR="${ARCADEDB_NATIVE_M2:-$HOME/.cache/arcadedb-native-m2}"

ARCH=""
TAG=""
SKIP_BINARY=0
RUN_SMOKE=1
REBUILD_BUILDER=0
ALLOW_EMULATION=0
ALLOW_LOW_MEMORY=0
BUILDER_MEMORY=""
HTTP_PORT=2480

log()  { echo "[native-docker] $*"; }
fail() { echo "[native-docker] ERROR: $*" >&2; exit 1; }

# Prints the header comment block above, from '# Usage:' to the first non-comment line, so the
# help text cannot drift out of sync with hardcoded line numbers.
usage() { awk '/^# Usage:/{f=1} f{ if ($0 !~ /^#/) exit; sub(/^# ?/,""); print }' "${BASH_SOURCE[0]}"; }

while [ $# -gt 0 ]; do
  case "$1" in
    --arch)             ARCH="${2:-}"; shift 2 ;;
    --arch=*)           ARCH="${1#*=}"; shift ;;
    --tag)              TAG="${2:-}"; shift 2 ;;
    --tag=*)            TAG="${1#*=}"; shift ;;
    --skip-binary-build) SKIP_BINARY=1; shift ;;
    --no-smoke)         RUN_SMOKE=0; shift ;;
    --port)             HTTP_PORT="${2:-}"; shift 2 ;;
    --port=*)           HTTP_PORT="${1#*=}"; shift ;;
    --builder-memory)   BUILDER_MEMORY="${2:-}"; shift 2 ;;
    --builder-memory=*) BUILDER_MEMORY="${1#*=}"; shift ;;
    --allow-low-memory) ALLOW_LOW_MEMORY=1; shift ;;
    --rebuild-builder)  REBUILD_BUILDER=1; shift ;;
    --allow-emulation)  ALLOW_EMULATION=1; shift ;;
    -h|--help)          usage; exit 0 ;;
    *)                  fail "unknown option '$1' (see --help)" ;;
  esac
done

command -v docker >/dev/null 2>&1 || fail "docker not found on PATH"
docker buildx version >/dev/null 2>&1 || fail "docker buildx not available (needed to build with --load)"

case "$(uname -m)" in
  x86_64|amd64)  HOST_ARCH="amd64" ;;
  aarch64|arm64) HOST_ARCH="arm64" ;;
  *)             fail "unsupported host architecture '$(uname -m)'" ;;
esac
ARCH="${ARCH:-$HOST_ARCH}"
case "$ARCH" in
  amd64|arm64) ;;
  *) fail "invalid --arch '$ARCH' (amd64|arm64)" ;;
esac

if [ "$ARCH" != "$HOST_ARCH" ] && [ "$ALLOW_EMULATION" != "1" ]; then
  fail "--arch $ARCH on a $HOST_ARCH host would run the whole native-image build under QEMU
  emulation, which takes hours and frequently exhausts memory. Pass --allow-emulation if you
  really want that; otherwise build $ARCH on a $ARCH machine (that is why CI's docker job runs
  each arch on its own native runner rather than one runner with buildx --platform)."
fi

# ---------------------------------------------------------------------------
# Memory preflight.
#
# native-image sizes its build heap at roughly 80% of the memory it can SEE, and inside a
# container that is the Docker VM's allocation, not the host's. A Mac with 36 GB whose Docker
# Desktop is left on the 8 GB default therefore OOMs - and it does so ~21 minutes in, after the
# entire Maven reactor has already built, with "Terminating due to java.lang.OutOfMemoryError:
# Java heap space" and no hint that the VM setting is the cause. That is an expensive way to
# learn it, so check up front. This image embeds GraalJS, which is what makes it so hungry:
# native-image.yml had to move its macOS leg onto a paid larger runner for the same reason,
# because the free ~7 GB one OOM-thrashed for 40+ minutes.
#
# 12 GiB is the floor enforced here and 16 GiB is what CI's Linux runners have; 8 GiB is known
# to fail. Raise it in Docker Desktop under Settings -> Resources -> Memory.
# ---------------------------------------------------------------------------
DOCKER_MEM_BYTES="$(docker info --format '{{.MemTotal}}' 2>/dev/null || echo 0)"
case "$DOCKER_MEM_BYTES" in
  ''|*[!0-9]*) DOCKER_MEM_BYTES=0 ;;
esac
if [ "$DOCKER_MEM_BYTES" -gt 0 ] && [ "$SKIP_BINARY" = "0" ]; then
  # Tenths of a GiB, so a 7.6 GiB VM does not print as a flat "7" next to a "needs 12" and read
  # like a bigger shortfall than it is.
  DOCKER_MEM_MIB=$(( DOCKER_MEM_BYTES / 1024 / 1024 ))
  DOCKER_MEM="$(( DOCKER_MEM_MIB / 1024 )).$(( (DOCKER_MEM_MIB % 1024) * 10 / 1024 ))"
  HOST_MEM_BYTES="$(sysctl -n hw.memsize 2>/dev/null || echo 0)"
  HOST_MEM_NOTE=""
  if [ "$HOST_MEM_BYTES" -gt "$DOCKER_MEM_BYTES" ]; then
    HOST_MEM_NOTE=" (this machine has $(( HOST_MEM_BYTES / 1024 / 1024 / 1024 )) GiB, but Docker only gets what you allot it)"
  fi
  if [ "$DOCKER_MEM_MIB" -lt $(( 12 * 1024 )) ]; then
    if [ "$ALLOW_LOW_MEMORY" = "1" ]; then
      log "WARN: Docker has only ${DOCKER_MEM} GiB; proceeding anyway (--allow-low-memory)."
    else
      fail "Docker has ${DOCKER_MEM} GiB of memory, which is not enough to link this image${HOST_MEM_NOTE}.
  native-image sizes its build heap at ~80% of the memory it can see, and inside a container that
  is the Docker VM's allocation. At ${DOCKER_MEM} GiB it dies with a Java heap OutOfMemoryError
  about 21 minutes in, once the whole Maven reactor has already built. CI's Linux runners have
  16 GiB; 12 is the floor checked here.
  Raise it in Docker Desktop: Settings -> Resources -> Memory, then restart Docker.
  Or pass --allow-low-memory to try regardless, --builder-memory <size> to cap the builder heap
  explicitly, or --skip-binary-build if you only need to rebuild the image around an existing
  binary (that step is cheap and needs none of this)."
    fi
  else
    log "docker memory: ${DOCKER_MEM} GiB"
  fi
fi

# amd64 links fully static against musl and runs on scratch; arm64 cannot (GraalVM CE ships no
# static musl JDK libs for aarch64 - oracle/graal#4645) and builds mostly-static against glibc,
# so it needs a base image that has glibc. Same split as native-image.yml's docker matrix.
if [ "$ARCH" = "amd64" ]; then
  DOCKERFILE="$DOCKER_DIR/Dockerfile.native.scratch"
  MODE_ARG="-Dnative.static=true"
  GRAALVM_ASSET_ARCH="linux-x64"
else
  DOCKERFILE="$DOCKER_DIR/Dockerfile.native.distroless"
  MODE_ARG="-Dnative.mostlystatic=true"
  GRAALVM_ASSET_ARCH="linux-aarch64"
fi
[ -f "$DOCKERFILE" ] || fail "missing $DOCKERFILE"

# ---------------------------------------------------------------------------
# Builder image.
#
# The GraalVM version comes out of native/pom.xml, the one place it is written down (the lint job
# already enforces that native-image.yml agrees with it). The download URL is then asked of the
# GitHub release API rather than constructed, because the asset name is not derivable from the
# version, and the shape differs per release line: mainline 25.0.2 publishes as
# graalvm-community-jdk-25.0.2_linux-<arch>_bin.tar.gz, intermediate 25.2.4 as
# graalvm-community-jdk-25i2-25.0.4_linux-<arch>_bin.tar.gz.
# ---------------------------------------------------------------------------
PIN="$(sed -n 's:.*<native\.graalvm\.version>\(.*\)</native\.graalvm\.version>.*:\1:p' "$NATIVE_POM" | head -1)"
[ -n "$PIN" ] || fail "could not read <native.graalvm.version> from $NATIVE_POM"
BUILDER_IMAGE="arcadedb-native-builder:${PIN}-${ARCH}"

if [ "$REBUILD_BUILDER" = "1" ] || ! docker image inspect "$BUILDER_IMAGE" >/dev/null 2>&1; then
  log "resolving GraalVM CE $PIN ($GRAALVM_ASSET_ARCH) from the graalvm-ce-builds releases"

  # Fetches one release by tag; empty output (and success) when that tag does not exist, so the
  # caller can fall through to the next candidate rather than aborting under `set -e`.
  fetch_release() {
    if command -v gh >/dev/null 2>&1; then
      gh api "repos/graalvm/graalvm-ce-builds/releases/tags/$1" 2>/dev/null || true
    else
      curl -fsSL -H 'Accept: application/vnd.github+json' \
        "https://api.github.com/repos/graalvm/graalvm-ce-builds/releases/tags/$1" 2>/dev/null || true
    fi
  }

  # graal-<version> FIRST, jdk-<version> second - the same order setup-graalvm's own findReleaseTag
  # uses, so this resolves whichever release the workflow would install. The two tags are not
  # interchangeable and which one exists depends on the line the pin tracks: intermediate releases
  # publish graal-* (graal-25.2.4), mainline ones publish jdk-* (jdk-25.0.2). Trying only one
  # breaks the moment native/pom.xml's pin moves between the two lines.
  RELEASE_JSON=""
  RESOLVED_TAG=""
  for candidate in "graal-${PIN}" "jdk-${PIN}"; do
    RELEASE_JSON="$(fetch_release "$candidate")"
    # Plain grep rather than jq/python: neither is guaranteed present, and the field is unambiguous.
    GRAALVM_URL="$(grep -o "https://[^\"]*graalvm-community[^\"]*_${GRAALVM_ASSET_ARCH}_bin\.tar\.gz" <<<"$RELEASE_JSON" | head -1 || true)"
    if [ -n "$GRAALVM_URL" ]; then
      RESOLVED_TAG="$candidate"
      break
    fi
  done
  [ -n "$GRAALVM_URL" ] || fail "no ${GRAALVM_ASSET_ARCH} asset under either graal-${PIN} or jdk-${PIN}.
  A version has to be published as a CE tarball to be usable here, and being on Maven Central is
  not the same thing: 25.0.3 and 25.0.4 have full org.graalvm.* artifacts and no tarball at all.
  See the <native.graalvm.version> comment in native/pom.xml for which versions do exist."
  log "  release tag: $RESOLVED_TAG"
  GRAALVM_SHA256="$(curl -fsSL "${GRAALVM_URL}.sha256" | tr -d '[:space:]')"
  [ -n "$GRAALVM_SHA256" ] || fail "could not fetch ${GRAALVM_URL}.sha256"

  log "building builder image $BUILDER_IMAGE"
  log "  $GRAALVM_URL"
  docker buildx build \
    --platform "linux/$ARCH" \
    -f "$DOCKER_DIR/Dockerfile.native-builder" \
    --build-arg "GRAALVM_URL=$GRAALVM_URL" \
    --build-arg "GRAALVM_SHA256=$GRAALVM_SHA256" \
    --build-arg "TARGET_ARCH=$ARCH" \
    -t "$BUILDER_IMAGE" \
    --load \
    "$DOCKER_DIR"
else
  log "reusing builder image $BUILDER_IMAGE (--rebuild-builder to refresh)"
fi

# ---------------------------------------------------------------------------
# Native binary, built inside the builder image against the bind-mounted working tree.
# ---------------------------------------------------------------------------

# In a git WORKTREE, .git is a file holding "gitdir: <abs path>/.git/worktrees/<name>", which lives
# outside the directory bind-mounted below. buildnumber-maven-plugin shells out to `git log` during
# arcadedb-engine's build, so without the real git directory in scope the build dies 20 seconds in
# with "fatal: not a git repository: .../.git/worktrees/<name>" - and it names a host path that
# does exist, which makes the failure read like a git problem rather than a mount problem. Mount
# the main repository's .git at its own absolute path so that pointer resolves inside the
# container too. A normal (non-worktree) checkout has its .git inside the mount already and needs
# nothing extra.
GIT_MOUNT=()
if [ -f "$REPO_ROOT/.git" ]; then
  GITDIR="$(sed -n 's/^gitdir: *//p' "$REPO_ROOT/.git" | head -1)"
  case "$GITDIR" in
    /*) ;;
    *)  GITDIR="$REPO_ROOT/$GITDIR" ;;
  esac
  # Strip the /worktrees/<name> suffix to get the common git dir, which holds the object store the
  # worktree's own gitdir refers back to; mounting the common dir brings both into scope.
  GIT_COMMON="${GITDIR%%/worktrees/*}"
  if [ -d "$GIT_COMMON" ]; then
    GIT_MOUNT=(-v "$GIT_COMMON:$GIT_COMMON")
    log "git worktree detected; also mounting $GIT_COMMON"
  else
    log "WARN: $REPO_ROOT/.git points at '$GITDIR', which does not resolve to a git directory;"
    log "WARN: git-dependent Maven plugins may fail inside the container."
  fi
fi

# native-image reads NATIVE_IMAGE_OPTIONS from the environment and prepends it to its own
# arguments (it announces "Picked up NATIVE_IMAGE_OPTIONS: ..." when it does), so the builder heap
# can be capped without touching native/pom.xml's buildArgs, which are shared with CI.
NI_OPTS=()
if [ -n "$BUILDER_MEMORY" ]; then
  NI_OPTS=(-e "NATIVE_IMAGE_OPTIONS=-J-Xmx${BUILDER_MEMORY}")
  log "capping the native-image builder heap at $BUILDER_MEMORY"
fi

if [ "$SKIP_BINARY" = "0" ]; then
  mkdir -p "$M2_DIR"
  log "building the linux/$ARCH binary in the container (this is the slow part)"
  log "  maven cache: $M2_DIR"
  # -pl :arcadedb-native selects the module by artifactId. A path selector would work here too,
  # since -w below makes /workspace the execution root, but it is one less thing to get wrong and
  # it matches build-native.sh, where the path form breaks outright when the caller's cwd is not
  # the repository root.
  #
  # ${GIT_MOUNT[@]+...}: the array is empty for a normal checkout, and macOS still ships bash 3.2,
  # where expanding an empty array under `set -u` aborts with "unbound variable".
  docker run --rm \
    --platform "linux/$ARCH" \
    --user "$(id -u):$(id -g)" \
    -e HOME=/m2 \
    ${NI_OPTS[@]+"${NI_OPTS[@]}"} \
    -v "$REPO_ROOT:/workspace" \
    -v "$M2_DIR:/m2" \
    ${GIT_MOUNT[@]+"${GIT_MOUNT[@]}"} \
    -w /workspace \
    "$BUILDER_IMAGE" \
    ./mvnw -B -ntp -Pnative -pl :arcadedb-native -am -DskipTests "$MODE_ARG" package
else
  log "--skip-binary-build: reusing whatever is already in native/target"
fi

# Locate the LINUX binary by the executable bit, the same way native-image.yml does. The name
# carries os-maven-plugin's os.detected.arch (x86_64 / aarch64), not this script's amd64/arm64
# label, so match on the "linux-" infix and let the glob supply the rest. A host build for macOS
# leaves an arcadedb-*-osx-* binary in the same directory; the linux- prefix keeps them apart.
cd "$REPO_ROOT/native/target" 2>/dev/null || fail "native/target does not exist - drop --skip-binary-build"
CANDIDATES=()
while IFS= read -r f; do CANDIDATES+=("$f"); done \
  < <(find . -maxdepth 1 -type f -name 'arcadedb-*-linux-*' -perm -u+x -print | sed 's|^\./||')
if [ "${#CANDIDATES[@]}" -ne 1 ]; then
  echo "[native-docker] ERROR: expected exactly one linux binary in native/target, found ${#CANDIDATES[@]}" >&2
  ls -la "$REPO_ROOT/native/target" >&2
  exit 1
fi
BIN_NAME="${CANDIDATES[0]}"
# Version is parsed back out of the filename (greedy up to the single "-linux-" separator, so a
# version containing dashes like 26.9.1-SNAPSHOT survives) rather than by re-invoking Maven.
VERSION="$(sed -E 's/^arcadedb-(.*)-linux-[^-]+$/\1/' <<<"$BIN_NAME")"
[ -n "$VERSION" ] && [ "$VERSION" != "$BIN_NAME" ] || fail "could not parse a version out of '$BIN_NAME'"
TAG="${TAG:-arcadedb:${VERSION}-native-${ARCH}-local}"
log "binary:  native/target/$BIN_NAME"
log "version: $VERSION"

# ---------------------------------------------------------------------------
# Stage the build context, mirroring native-image.yml's "Stage build context" step.
# ---------------------------------------------------------------------------
CTX="$(mktemp -d)"
trap 'rm -rf "$CTX"' EXIT
mkdir -p "$CTX/config"
cp "$REPO_ROOT/native/target/$BIN_NAME" "$CTX/arcadedb"
chmod 0755 "$CTX/arcadedb"
cp -R "$REPO_ROOT/package/src/main/config/." "$CTX/config/"
if [ "$ARCH" = "amd64" ]; then
  # scratch has no trust store at all, so the scratch Dockerfile COPYs one in. CI takes the
  # runner's /etc/ssl/certs/ca-certificates.crt; a macOS host has no such file, so take it from
  # the builder image instead - which is a Debian-family bundle either way, and keeps this step
  # working identically on every host.
  log "extracting a CA bundle from the builder image for the scratch base"
  docker run --rm --platform "linux/$ARCH" --entrypoint cat "$BUILDER_IMAGE" \
    /etc/ssl/certs/ca-certificates.crt > "$CTX/ca-certificates.crt"
  [ -s "$CTX/ca-certificates.crt" ] || fail "extracted CA bundle is empty"
fi

log "building image $TAG from $(basename "$DOCKERFILE")"
docker buildx build \
  --platform "linux/$ARCH" \
  -f "$DOCKERFILE" \
  -t "$TAG" \
  --load \
  "$CTX"

log "image built: $TAG ($(docker image inspect "$TAG" --format '{{.Size}}' | awk '{printf "%.0f MB", $1/1024/1024}'))"

# ---------------------------------------------------------------------------
# Container smoke test, mirroring native-image.yml's "Smoke the container" step: exercise.sh
# hard-asserts HTTP/Studio/SQL/Cypher/JS against the RUNNING IMAGE, and the startup log is then
# scanned for the non-fatal failures exercise.sh can stay green through (a class the JUL config
# loads reflectively being absent from the image degrades logging without failing anything).
# ---------------------------------------------------------------------------
if [ "$RUN_SMOKE" = "1" ]; then
  CONTAINER="arcadedb-native-smoke-$$"
  # Refuse to start if something already holds the host port. Without this the run either fails
  # opaquely on the port bind, or - if the squatter was there first and docker picked a different
  # binding - exercise.sh happily asserts against the OTHER server and reports a green smoke for
  # an image it never touched.
  if command -v lsof >/dev/null 2>&1 && lsof -nP -iTCP:"$HTTP_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
    fail "port $HTTP_PORT is already in use, so the smoke test would either fail to bind or assert
  against whatever is already listening. Free it, or pass --port <other> / --no-smoke.
  Culprit: $(lsof -nP -iTCP:"$HTTP_PORT" -sTCP:LISTEN | awk 'NR==2 {print $1" (pid "$2")"}')"
  fi
  log "smoke-testing the container (host port $HTTP_PORT -> container 2480)"
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  docker run -d --name "$CONTAINER" --platform "linux/$ARCH" -p "$HTTP_PORT":2480 \
    "$TAG" -Darcadedb.server.rootPassword=PlayWithData123! >/dev/null

  rc=0
  HTTP="$HTTP_PORT" "$REPO_ROOT/native/src/test/scripts/exercise.sh" || rc=$?
  LOGS="$(docker logs "$CONTAINER" 2>&1)"
  echo "$LOGS"
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true

  [ "$rc" -eq 0 ] || fail "container exercise.sh failed (rc=$rc)"
  if grep -qiE "ClassNotFoundException|Can't load log handler" <<<"$LOGS"; then
    fail "container startup logged a ClassNotFoundException / log-handler load failure"
  fi
  log "smoke test passed"
fi

# The scratch image has no /etc/passwd and runs as UID 0 unless overridden, so the amd64 hint
# carries a --user line; the distroless :nonroot base already runs non-root and needs none.
# Built as a variable rather than inline in the heredoc: a $(...) substitution there emits its
# output without a trailing newline, which would fold the --user line into the tag line.
USER_HINT=""
if [ "$ARCH" = "amd64" ]; then
  USER_HINT='    --user $(id -u):$(id -g) \'$'\n'
fi

cat <<EOF

[native-docker] done. Run it with:

  docker run --rm -p 2480:2480 \\
    -v "\$(pwd)/databases:/home/arcadedb/databases" \\
${USER_HINT}    $TAG \\
    -Darcadedb.server.rootPassword=PlayWithData123!

Neither base image has a shell, so -D flags go as trailing arguments, not via
-e ARCADEDB_SETTINGS. See docs/native-image.md "Running the container".
EOF
