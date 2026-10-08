#!/bin/bash
# ArcadeDB Python Package Build Script
# Builds arcadedb-embedded with a bundled JRE (no Java install needed).
# JAR sourcing is explicit: provide a JAR directory to embed those artifacts;
# otherwise JARs are pulled from the arcadedata/arcadedb image.
#
# Quick local-jar workflow (no host Java install required):
#   1) Build ArcadeDB JARs in Docker:
#        docker run --rm -v "$PWD":/src -w /src maven:3.9-amazoncorretto-25 \
#          sh -c "git config --global --add safe.directory /src && ./mvnw -DskipTests -pl package -am package"
#   2) Point the build at the full assembly's lib directory:
#        cd bindings/python && ./scripts/build.sh linux/amd64 3.12 ../../package/target/arcadedb-<version>.dir/arcadedb-<version>/lib

set -euo pipefail

# Stamped before anything is built, so the wheel check at the end can ask
# whether an artifact is newer than this run rather than merely present.
# A marker file rather than an epoch: `test -nt` is portable, while
# `find -newermt` is GNU-only and this script also runs on macOS hosts.
BUILD_START_MARKER=$(mktemp -t arcadedb-build-start.XXXXXX)
trap 'rm -f "$BUILD_START_MARKER"' EXIT

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PY_BINDINGS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "$PY_BINDINGS_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Parse command line arguments
PLATFORM="${1:-}"
PYTHON_VERSION="${2:-3.12}"
JAR_LIB_DIR="${3:-}"

print_header() {
    echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║  🎮 ArcadeDB Python Package - Build Script                 ║${NC}"
    echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

print_usage() {
    echo "Usage: $0 [PLATFORM] [PYTHON_VERSION] [JAR_LIB_DIR]"
    echo ""
    echo "Builds arcadedb-embedded package with bundled JRE"
    echo "No external Java installation required!"
    echo ""
    echo "PLATFORM:"
    echo "  Auto-detected if not specified"
    echo "  linux/amd64    Linux x86_64 (Docker build)"
    echo "  linux/arm64    Linux ARM64 (Docker build, native ARM64 runner)"
    echo "  darwin/arm64   macOS ARM64 Apple Silicon (native build on macOS)"
    echo "  windows/amd64  Windows x86_64 (native build on Windows)"
    echo ""
    echo "PYTHON_VERSION:"
    echo "  Python version for wheel (default: 3.12); used by Linux (Docker) builds only"
    echo "  Examples: 3.10, 3.11, 3.12, 3.13, 3.14"
    echo ""
    echo "JAR_LIB_DIR (optional):"
    echo "  Directory containing ArcadeDB JARs to embed"
    echo "  If omitted, JARs are pulled from arcadedata/arcadedb:<version>"
    echo ""
    echo "Build Methods:"
    echo "  Native: macOS/Windows build on matching native host architecture"
    echo "  Docker: Linux uses Docker for manylinux compliance"
    echo ""
    echo "Examples:"
    echo "  $0                                    # Build for current platform with Python 3.12"
    echo "  $0 linux/amd64                        # Build for Linux x86_64 with Python 3.12 (Docker)"
    echo "  $0 linux/amd64 3.11                   # Build for Linux x86_64 with Python 3.11 (Docker)"
    echo "  $0 linux/amd64 3.12 /path/to/jars     # Build using JARs from /path/to/jars"
    echo "  $0 darwin/arm64                       # Build for macOS ARM64 (native; uses the first Python with a working build module)"
    echo ""
    echo "Package features:"
    echo "  ✅ Bundled platform-specific JRE (no Java required)"
    echo "  ✅ Optimized JAR selection (see scripts/jar_exclusions.txt)"
    echo "  ✅ Multi-platform support (4 platforms)"
    echo "  📦 Size varies by platform/version; see CI summaries for current numbers"
    echo ""
}

normalize_arch() {
    case "$1" in
        x86_64 | amd64)
            echo "amd64"
            ;;
        aarch64 | arm64)
            echo "arm64"
            ;;
        *)
            echo "$1"
            ;;
    esac
}

# Check for help flag
if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    print_header
    print_usage
    exit 0
fi

print_header

# Auto-detect platform if not specified
if [[ -z "$PLATFORM" ]]; then
    echo -e "${CYAN}🔍 Auto-detecting platform...${NC}"
    OS="$(uname -s)"
    ARCH="$(uname -m)"

    case "${OS}" in
        Linux*)
            PLATFORM_OS="linux"
            ;;
        Darwin*)
            PLATFORM_OS="darwin"
            ;;
        MINGW* | MSYS* | CYGWIN*)
            PLATFORM_OS="windows"
            ;;
        *)
            echo -e "${RED}❌ Unsupported OS: ${OS}${NC}"
            exit 1
            ;;
    esac

    case "${ARCH}" in
        x86_64 | amd64)
            PLATFORM_ARCH="amd64"
            ;;
        aarch64 | arm64)
            PLATFORM_ARCH="arm64"
            ;;
        *)
            echo -e "${RED}❌ Unsupported architecture: ${ARCH}${NC}"
            exit 1
            ;;
    esac

    PLATFORM="${PLATFORM_OS}/${PLATFORM_ARCH}"
    echo -e "${CYAN}✅ Detected platform: ${YELLOW}${PLATFORM}${NC}"
    echo ""
fi

# Auto-detect Docker tag from pom.xml
echo -e "${CYAN}🔍 Detecting version from pom.xml...${NC}"
DOCKER_TAG=$(python3 "$SCRIPT_DIR/extract_version.py" --format=docker)
echo -e "${CYAN}📌 Docker tag: ${YELLOW}${DOCKER_TAG}${NC}"
echo ""

# Select jar source: explicit directory when provided; otherwise pull from ArcadeDB image
LOCAL_JARS_DIR="$PY_BINDINGS_DIR/local-jars/lib"
USE_LOCAL_JARS_ARG=""
LOCAL_JARS_HASH_ARG=""
JAR_SOURCE_DESC="ArcadeDB image"
mkdir -p "$LOCAL_JARS_DIR"

if [[ -n "$JAR_LIB_DIR" ]]; then
    if [[ ! -d "$JAR_LIB_DIR" ]]; then
        echo -e "${RED}❌ JAR_LIB_DIR not found: ${JAR_LIB_DIR}${NC}"
        exit 1
    fi

    if ! find "$JAR_LIB_DIR" -maxdepth 1 -name "*.jar" -print -quit | grep -q .; then
        echo -e "${RED}❌ No JAR files found in: ${JAR_LIB_DIR}${NC}"
        exit 1
    fi

    JAR_LIB_DIR_REAL=$(realpath "$JAR_LIB_DIR")
    LOCAL_JARS_REAL=$(realpath "$LOCAL_JARS_DIR")

    if [[ "$JAR_LIB_DIR_REAL" == "$LOCAL_JARS_REAL" ]]; then
        echo -e "${CYAN}📦 Using pre-staged JARs in: ${YELLOW}${JAR_LIB_DIR}${NC}"
        JAR_SOURCE_DESC="${JAR_LIB_DIR} (pre-staged)"
    else
        echo -e "${CYAN}📦 Using provided JAR directory: ${YELLOW}${JAR_LIB_DIR}${NC}"
        rm -rf "$LOCAL_JARS_DIR"
        mkdir -p "$LOCAL_JARS_DIR"
        cp -a "$JAR_LIB_DIR"/*.jar "$LOCAL_JARS_DIR"/
        JAR_SOURCE_DESC="${JAR_LIB_DIR} (staged into local-jars)"
    fi

    USE_LOCAL_JARS_ARG="--build-arg USE_LOCAL_JARS=1"
    LOCAL_JARS_HASH=$(find "$LOCAL_JARS_DIR" -maxdepth 1 -name "*.jar" -type f -print0 | sort -z | xargs -0 sha256sum | sha256sum | awk '{print $1}')
    LOCAL_JARS_HASH_ARG="--build-arg LOCAL_JARS_HASH=${LOCAL_JARS_HASH}"
else
    echo -e "${CYAN}📦 Using JARs from ArcadeDB image (no JAR_LIB_DIR provided)${NC}"
fi

# Determine build method: native or Docker
# Use native build if we're already on the target platform
CURRENT_OS="$(uname -s)"
CURRENT_ARCH="$(uname -m)"
CURRENT_ARCH_NORMALIZED="$(normalize_arch "$CURRENT_ARCH")"
TARGET_OS="${PLATFORM%%/*}"
TARGET_ARCH="${PLATFORM##*/}"

USE_NATIVE=false
if [[ "$TARGET_OS" == "darwin" ]]; then
    if [[ "$CURRENT_OS" != "Darwin" ]]; then
        echo -e "${RED}❌ ${PLATFORM} builds require a native macOS host${NC}"
        echo -e "${YELLOW}💡 jlink can only create a macOS JRE when run on macOS${NC}"
        exit 1
    fi
    if [[ "$CURRENT_ARCH_NORMALIZED" != "$TARGET_ARCH" ]]; then
        echo -e "${RED}❌ ${PLATFORM} builds require a matching native macOS architecture${NC}"
        echo -e "${YELLOW}💡 Host architecture: ${CURRENT_ARCH_NORMALIZED}; target architecture: ${TARGET_ARCH}${NC}"
        exit 1
    fi
    USE_NATIVE=true
elif [[ "$TARGET_OS" == "windows" ]]; then
    if [[ "$CURRENT_OS" != MINGW* && "$CURRENT_OS" != MSYS* && "$CURRENT_OS" != CYGWIN* ]]; then
        echo -e "${RED}❌ ${PLATFORM} builds require a native Windows host${NC}"
        echo -e "${YELLOW}💡 jlink can only create a Windows JRE when run on Windows${NC}"
        exit 1
    fi
    if [[ "$CURRENT_ARCH_NORMALIZED" != "$TARGET_ARCH" ]]; then
        echo -e "${RED}❌ ${PLATFORM} builds require a matching native Windows architecture${NC}"
        echo -e "${YELLOW}💡 Host architecture: ${CURRENT_ARCH_NORMALIZED}; target architecture: ${TARGET_ARCH}${NC}"
        exit 1
    fi
    USE_NATIVE=true
fi

BUILD_METHOD="Docker"
if [[ "$USE_NATIVE" == true ]]; then
    BUILD_METHOD="Native"
fi

# Check requirements based on build method
if [[ "$USE_NATIVE" == false ]]; then
    # Docker build
    if ! command -v docker &> /dev/null; then
        echo -e "${RED}❌ Docker is not installed or not in PATH${NC}"
        echo -e "${YELLOW}💡 Please install Docker to build the Python bindings${NC}"
        exit 1
    fi
else
    # Native build - check for Java (needed to BUILD the bundled JRE)
    if ! command -v java &> /dev/null; then
        echo -e "${RED}❌ Java is not installed${NC}"
        echo -e "${YELLOW}💡 Please install Java 25+ JDK to BUILD the package (creates bundled JRE)${NC}"
        exit 1
    fi
    if ! command -v jlink &> /dev/null; then
        echo -e "${RED}❌ jlink not found${NC}"
        echo -e "${YELLOW}💡 Please install a full JDK 25+ (jlink creates the bundled JRE)${NC}"
        exit 1
    fi
fi

echo -e "${CYAN}📋 Build Configuration:${NC}"
echo -e "   Package: ${YELLOW}arcadedb-embedded${NC}"
echo -e "   Platform: ${YELLOW}${PLATFORM}${NC}"
echo -e "   Python Version: ${YELLOW}${PYTHON_VERSION}${NC}"
echo -e "   JAR Source: ${YELLOW}${JAR_SOURCE_DESC}${NC}"
echo -e "   JRE: ${YELLOW}Bundled (end users need no Java)${NC}"
echo -e "   Build Method: ${YELLOW}${BUILD_METHOD}${NC}"
echo ""

# Package configuration
PACKAGE_NAME="arcadedb-embedded"
DESCRIPTION="ArcadeDB embedded multi-model database with bundled JRE - no Java installation required"

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Building: ${YELLOW}${PACKAGE_NAME}${NC}"
echo -e "${BLUE}Platform: ${YELLOW}${PLATFORM}${NC}"
echo -e "${BLUE}Method: ${YELLOW}${BUILD_METHOD}${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

if [[ "$USE_NATIVE" == true ]]; then
    # Native build
    echo -e "${YELLOW} Building natively on ${PLATFORM}...${NC}"
    "$SCRIPT_DIR/build-native.sh" "$PLATFORM" "$PACKAGE_NAME" "$DESCRIPTION" "$DOCKER_TAG" "${BUILD_VERSION:-}"
else
    # Docker build
    echo -e "${YELLOW} 🐳 Building in Docker...${NC}"

    # Check if BUILD_VERSION is set (from CI/CD)
    BUILD_VERSION_ARG=""
    if [ -n "${BUILD_VERSION:-}" ]; then
        echo -e "${CYAN}📌 Using specified version: ${YELLOW}${BUILD_VERSION}${NC}"
        BUILD_VERSION_ARG="--build-arg BUILD_VERSION=$BUILD_VERSION"
    fi

    # Convert platform format: linux/amd64 -> linux-x64, linux/arm64 -> linux-arm64, etc.
    TARGET_PLATFORM=$(echo "$PLATFORM" | sed 's|/|-|' | sed 's/amd64/x64/')
    echo -e "${CYAN}🎯 Target platform: ${YELLOW}${PLATFORM}${NC}"
    echo -e "${CYAN}🎯 JRE platform: ${YELLOW}${TARGET_PLATFORM}${NC}"
    echo ""

    # Determine Docker build platform (always Linux for cross-compilation)
    # We build ON linux/amd64 or linux/arm64, but FOR any target platform
    DOCKER_PLATFORM="${PLATFORM}"

    if [[ -z "$DOCKER_TAG" ]]; then
        echo -e "${RED}❌ Missing ArcadeDB Docker tag${NC}"
        echo -e "${YELLOW}💡 Pass a version so Docker builds remain reproducible${NC}"
        exit 1
    fi

    # Build Docker image
    echo -e "${CYAN}📦 Building Docker image...${NC}"

    docker build \
        --pull \
        --platform "$DOCKER_PLATFORM" \
        --build-arg PYTHON_VERSION="$PYTHON_VERSION" \
        --build-arg PACKAGE_NAME="$PACKAGE_NAME" \
        --build-arg PACKAGE_DESCRIPTION="$DESCRIPTION" \
        --build-arg ARCADEDB_TAG="$DOCKER_TAG" \
        --build-arg TARGET_PLATFORM="$TARGET_PLATFORM" \
        $USE_LOCAL_JARS_ARG \
        $LOCAL_JARS_HASH_ARG \
        $BUILD_VERSION_ARG \
        --target export \
        -t arcadedb-python-package-export \
        -f "$SCRIPT_DIR/Dockerfile.build" \
        ../..

    # Run tests
    echo -e "${CYAN}🧪 Running tests in Docker...${NC}"
    docker build \
        --platform "$DOCKER_PLATFORM" \
        --build-arg PYTHON_VERSION="$PYTHON_VERSION" \
        --build-arg PACKAGE_NAME="$PACKAGE_NAME" \
        --build-arg PACKAGE_DESCRIPTION="$DESCRIPTION" \
        --build-arg ARCADEDB_TAG="$DOCKER_TAG" \
        --build-arg TARGET_PLATFORM="$TARGET_PLATFORM" \
        $USE_LOCAL_JARS_ARG \
        $LOCAL_JARS_HASH_ARG \
        $BUILD_VERSION_ARG \
        --target tester \
        -t arcadedb-python-package \
        -f "$SCRIPT_DIR/Dockerfile.build" \
        ../..

    # Create dist directory if it doesn't exist
    mkdir -p dist

    # Record what was in dist/ BEFORE extracting. The check below used to be
    # `ls dist/*.whl`, which asks "is there a wheel here?" when the question is
    # "did THIS build produce one?". Nothing cleans dist/, so a wheel left by
    # any earlier build satisfied that test: an export stage that yielded
    # nothing still printed the success banner and exited 0, handing back a
    # stale wheel that looks like a fresh one. build-native.sh already avoids
    # this by clearing the directory first; the Docker path had no equivalent.
    # Compare identities rather than clearing, so deliberately kept wheels for
    # other platforms survive a build.
    WHEELS_BEFORE=$(ls -1 dist/*.whl 2> /dev/null | sort || true)

    # Extract the wheel from the export container
    echo -e "${CYAN}📋 Extracting wheel file...${NC}"
    CONTAINER_ID=$(docker create arcadedb-python-package-export)
    docker cp ${CONTAINER_ID}:/build/dist/. ./dist/
    docker rm ${CONTAINER_ID}

    # Verify THIS build produced a wheel, not merely that dist/ contains one
    WHEELS_AFTER=$(ls -1 dist/*.whl 2> /dev/null | sort || true)
    NEW_WHEELS=$(comm -13 <(echo "$WHEELS_BEFORE") <(echo "$WHEELS_AFTER") || true)
    if [[ -n "$NEW_WHEELS" ]]; then
        echo -e "${GREEN}✅ Wheel file created successfully!${NC}"
    elif [[ -n "$WHEELS_AFTER" ]]; then
        # Same filename as before: only trust it if the export actually rewrote
        # it. A rebuild of the same version legitimately lands here.
        NEWEST=$(ls -t dist/*.whl | head -n1)
        if [[ "$NEWEST" -nt "$BUILD_START_MARKER" ]]; then
            echo -e "${GREEN}✅ Wheel file created successfully!${NC}"
        elif [[ -n "$JAR_LIB_DIR" ]]; then
            # MTIME IS A HEURISTIC; THE SHA256 BELOW IS THE FACT.
            #
            # `docker cp` preserves the timestamp from inside the container, so a
            # run whose layers all cache-hit copies out a wheel stamped when the
            # layer was first built. The wheel is correct and current, and the
            # -nt test calls it stale. That failed a pinned-pair build whose
            # wheel carried exactly the right engine commit.
            #
            # When JAR_LIB_DIR is set, the next step compares the SHA256 of the
            # integration jar INSIDE the wheel against the one on disk and exits
            # 1 on mismatch. That is a content check and strictly stronger, so
            # let it be the gate rather than failing here on a file date.
            echo -e "${YELLOW}⚠️  No wheel newer than this run; docker layers likely all cached.${NC}"
            echo -e "${YELLOW}   Deferring to the SHA256 jar verification below, which is authoritative.${NC}"
        else
            echo -e "${RED}❌ No wheel was produced by this build${NC}"
            echo -e "${YELLOW}💡 dist/ still holds only wheels older than this run:${NC}"
            ls -lh dist/*.whl
            exit 1
        fi
    else
        echo -e "${RED}❌ Failed to extract wheel file${NC}"
        exit 1
    fi

    if [[ -n "$JAR_LIB_DIR" ]]; then
        echo -e "${CYAN}🔎 Verifying embedded local integration JAR...${NC}"
        ARCADEDB_VERSION="$DOCKER_TAG" BUILD_VERSION="${BUILD_VERSION:-}" NEW_WHEELS="${NEW_WHEELS:-}" python3 - << 'PY'
import hashlib
import os
import re
import sys
import zipfile
from pathlib import Path

ARCADEDB_VERSION = os.environ["ARCADEDB_VERSION"]
# NOT sorted()[-1]: that is a LEXICOGRAPHIC sort over filenames, where
# "26.9.1" ranks above "26.10.1" because '9' > '1'. On 2026-09-19 that made
# this check open September's 26.9.1 wheel, look for October's
# arcadedb-integration-26.10.1-SNAPSHOT.jar inside it, and fail a wheel that
# was in fact correct and complete. Nor the newest by mtime: `docker cp` keeps
# the timestamp from inside the container, so a fully cached run's wheel can be
# older than one left in dist/ by another build. Take the wheel this run added
# (NEW_WHEELS), else the newest wheel of the version this run asked for
# (BUILD_VERSION when set, as the wheel's own version follows it), and assert
# the version, so picking the wrong file fails loudly.
_want = (os.environ.get("BUILD_VERSION") or ARCADEDB_VERSION).replace("-SNAPSHOT", "").replace("-", ".")


def _is_wanted(wheel_path):
    # The wheel's version field, matched whole: 26.10.1 is 26.10.1 or its dev, post, or pre-release, never 26.10.10.
    parts = wheel_path.name.split("-")
    return len(parts) > 1 and re.fullmatch(re.escape(_want) + r"(\.dev\d+|\.post\d+|(a|b|rc)\d+)?", parts[1]) is not None


_new = [Path(p) for p in os.environ.get("NEW_WHEELS", "").split() if p.endswith(".whl")]
wheels = _new or sorted((p for p in Path("dist").glob("arcadedb_embedded-*.whl") if _is_wanted(p)),
                        key=lambda p: p.stat().st_mtime)
if not wheels:
    print(f"❌ no arcadedb_embedded-{_want} wheel in dist/", file=sys.stderr)
    sys.exit(1)
wheel = wheels[-1]
if not _is_wanted(wheel):
    print(f"❌ newest wheel is {wheel.name}, which is not the "
          f"{ARCADEDB_VERSION} build this run produced", file=sys.stderr)
    sys.exit(1)
local_jar_name = f"arcadedb-integration-{ARCADEDB_VERSION}.jar"
local_jar = Path(f"local-jars/lib/{local_jar_name}")

if not local_jar.exists():
    print(f"❌ Local integration JAR not found: {local_jar}", file=sys.stderr)
    sys.exit(1)

with zipfile.ZipFile(wheel) as zf:
    matches = [name for name in zf.namelist() if name.endswith(local_jar_name)]
    if not matches:
        print(f"❌ Embedded integration JAR ({local_jar_name}) not found in wheel", file=sys.stderr)
        sys.exit(1)
    wheel_bytes = zf.read(matches[0])

local_bytes = local_jar.read_bytes()
wheel_hash = hashlib.sha256(wheel_bytes).hexdigest()
local_hash = hashlib.sha256(local_bytes).hexdigest()

print(f"   wheel integration SHA256: {wheel_hash}")
print(f"   local integration SHA256: {local_hash}")

if wheel_bytes != local_bytes:
    print("❌ Wheel embedded integration JAR does not match local-jars/lib", file=sys.stderr)
    sys.exit(1)

print("✅ Embedded integration JAR matches local-jars/lib")
PY
    fi
fi

echo ""

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
# SUPERSEDED WHEELS ARE DELETED, NOT KEPT. A wheel left in dist/ from an
# earlier build was read as evidence of the engine inside a newer one
# (2026-09-15: a 26.10.1.dev0 label beside a 26.6.1 jar, from a build days
# apart), which is exactly the confusion a stale artifact produces. Only the
# wheel this build wrote survives for its python/platform tag; wheels for
# other tags are someone else's build and are left alone.
# Which wheel is "this build's": the one this run added (NEW_WHEELS, Docker path), not the newest by mtime. `docker cp`
# keeps the container's timestamp, so after a fully cached run `ls -t` can rank an older build's wheel of ANOTHER
# version first, and this loop would then delete the wheel just produced as "superseded" (CodeRabbit on upstream #9294).
if [[ -n "${NEW_WHEELS:-}" ]]; then
    NEWEST_WHEEL=$(echo "$NEW_WHEELS" | head -n1)
else
    NEWEST_WHEEL=$(ls -t dist/*.whl | head -n1)
fi
NEWEST_TAG=$(basename "$NEWEST_WHEEL" | sed -E 's/^[^-]+-[^-]+-//')
for old in dist/*.whl; do
    if [[ "$old" != "$NEWEST_WHEEL" && "$(basename "$old" | sed -E 's/^[^-]+-[^-]+-//')" == "$NEWEST_TAG" ]]; then
        rm -f "$old"
        echo -e "${YELLOW}🗑  Removed superseded wheel $(basename "$old")${NC}"
    fi
done

# THE WHEEL SIZE POLICY: a wheel at or over 100 MB means a mistake (a whole distribution in JAR_LIB_DIR, an optional jar,
# a stale artifact), and an embedded database should not ship a huge wheel anyway. Fail it here, before anything tests
# or publishes it. The release workflow checks the same limit again before the upload.
SIZE_PY=$(command -v python3 || command -v python || true)
if [[ -z "$SIZE_PY" ]]; then
    echo -e "${YELLOW}⚠️  No python found: the wheel size check was skipped${NC}"
else
    "$SIZE_PY" "$SCRIPT_DIR/verify_wheel_size.py" "$NEWEST_WHEEL" || exit 1
fi

echo -e "${GREEN}🎉 Build completed successfully!${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo -e "${CYAN}📦 Built package:${NC}"
if [ -d "dist" ]; then
    ls -lh dist/*.whl 2> /dev/null | awk '{print "   " $9 " (" $5 ")"}'
fi

echo ""

# Refresh the repo-root uv dev environment so `uv run pytest` immediately uses
# the wheel that was just built. Skipped in CI, which installs wheels itself.
if [ -z "${CI:-}" ] && command -v uv > /dev/null 2>&1; then
    REPO_ROOT=$(git rev-parse --show-toplevel 2> /dev/null || true)
    if [ -n "$REPO_ROOT" ] && [ -f "$REPO_ROOT/pyproject.toml" ]; then
        echo -e "${CYAN}🔄 Refreshing uv dev environment at repo root...${NC}"
        (
            cd "$REPO_ROOT" &&
                uv lock --upgrade-package arcadedb-embedded &&
                uv sync --reinstall-package arcadedb-embedded
        )
    fi
fi

echo ""
echo -e "${BLUE}💡 Next steps:${NC}"
echo -e "   🧪 Run tests (from the repository root or bindings/python):"
echo -e "      ${YELLOW}uv run pytest${NC}"
echo ""
echo -e "   📤 Releases publish through the release workflow (docs/development/release.md);"
echo -e "      do not upload wheels by hand"
echo ""
