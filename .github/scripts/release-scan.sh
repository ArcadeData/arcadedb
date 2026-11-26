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
# Pre-release vulnerability gate. Builds the release artifacts exactly as the release does, then scans every SBOM and
# the JVM image of each platform with grype. The gate fails only on a CRITICAL vulnerability that has a fix available
# and is not marked not_affected in vex/arcadedb.openvex.json: a release must not ship a critical hole that an upgrade
# would close, but an unfixable or already-assessed one must not block it forever.
#
# Every scan runs even when an earlier one fails, so one run reports everything that is wrong; the exit status is
# non-zero if any gate failed. JSON reports land in package/target/scan/ and the full all-severity tables go to
# $GITHUB_STEP_SUMMARY when it is set.
#
# Usage: release-scan.sh <version>
#   env SCAN_BUILDER    buildx builder used for the OCI export (default arcadedb-scan, created when absent: the default
#                       docker driver cannot export an OCI archive)
#       SCAN_PLATFORMS  comma-separated platforms to scan (e.g. linux/amd64). Default: package/pom.xml's
#                       `docker-maven-plugin.platform` as the release's `-Pdocker` build resolves it on this machine.
#                       Its `multiplatform` profile activates on any unix host, so on the Linux release runner that is
#                       linux/amd64,linux/arm64: exactly the platforms mvn-release.yml pushes, no more and no fewer.

set -uo pipefail

if [[ $# -ne 1 ]]; then
    echo "Usage: release-scan.sh <version>" >&2
    exit 2
fi

readonly VERSION="$1"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly ROOT
readonly SCAN_DIR="${ROOT}/package/target/scan"
readonly VEX="${ROOT}/vex/arcadedb.openvex.json"
readonly BUILDER="${SCAN_BUILDER:-arcadedb-scan}"

cd "${ROOT}" || exit 1

# The platforms the release publishes, read from the property the image build uses rather than listed here, so a
# platform added to or dropped from the release is scanned (or not) without touching this script.
platforms="${SCAN_PLATFORMS:-}"
if [[ -z "${platforms}" ]]; then
    platforms="$(./mvnw -q -N -Pdocker -f package/pom.xml help:evaluate -Dexpression=docker-maven-plugin.platform -DforceStdout)" || {
        echo "::error::cannot read docker-maven-plugin.platform from package/pom.xml" >&2
        exit 1
    }
fi
PLATFORMS=()
IFS=',' read -r -a entries <<<"${platforms}"
for p in "${entries[@]}"; do
    p="${p//[[:space:]]/}"
    [[ -z "${p}" ]] && continue
    if [[ ! "${p}" =~ ^linux/[a-z0-9]+$ ]]; then
        echo "::error::unexpected image platform '${p}' (from '${platforms}')" >&2
        exit 1
    fi
    PLATFORMS+=("${p#linux/}")
done
if [[ ${#PLATFORMS[@]} -eq 0 ]]; then
    echo "::error::no image platform to scan (got '${platforms}')" >&2
    exit 1
fi
readonly PLATFORMS
echo "== Image platforms: ${PLATFORMS[*]}"
mkdir -p "${SCAN_DIR}"
FAILED=0

fail() {
    echo "::error::$*" >&2
    FAILED=1
}

# The build needs network (cyclonedx makeBom skips itself offline) and python3 (merge-sbom.py).
echo "== Building release artifacts for ${VERSION}"
if ! ./mvnw -Pdeploy -DskipTests --batch-mode --errors --show-version -pl package -am package; then
    echo "::error::the release build failed" >&2
    exit 1
fi

# The expected set is explicit: a glob would pass with one distribution silently missing.
SBOMS=("${ROOT}/package/target/sbom/full/arcadedb-${VERSION}.cdx.json" "${ROOT}/package/target/sbom/base/arcadedb-${VERSION}-base.cdx.json")
for sbom in "${SBOMS[@]}"; do
    if [[ ! -f "${sbom}" ]]; then
        echo "::error::expected SBOM ${sbom} is missing, the build did not produce it" >&2
        exit 1
    fi
done

echo "== Checking SBOM fields"
python3 .github/scripts/check-sbom-fields.py "${SBOMS[@]}" || fail "an SBOM misses required fields"

# scan <label> <grype source> : the gate and the report share one run, so the table in the summary is what gated.
scan() {
    local label="$1" source="$2" table rc
    table="${SCAN_DIR}/${label}.txt"
    echo "== Scanning ${label}"
    grype "${source}" --vex "${VEX}" --only-fixed --fail-on critical \
        -o "json=${SCAN_DIR}/${label}.json" -o "table=${table}"
    rc=$?
    cat "${table}" 2> /dev/null
    if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
        {
            echo "### ${label} (grype exit ${rc})"
            echo '```'
            cat "${table}"
            echo '```'
        } >> "${GITHUB_STEP_SUMMARY}"
    fi
    if ! jq -e . "${SCAN_DIR}/${label}.json" > /dev/null 2>&1; then
        fail "grype failed to scan ${source} (exit ${rc}, no valid JSON report)"
    elif [[ ${rc} -ne 0 ]]; then
        fail "${label}: grype exit ${rc}, a critical vulnerability with a fix is not covered by the VEX document"
    fi
}

for sbom in "${SBOMS[@]}"; do
    scan "sbom-$(basename "$(dirname "${sbom}")")" "sbom:${sbom}"
done

# The Dockerfile copies ./arcadedb-*, so the context is the distribution directory the `full` assembly lays out. The
# Dockerfile itself stays in src/main/docker (the docker profile only copies it for the push), hence -f.
CONTEXT="${ROOT}/package/target/arcadedb-${VERSION}.dir"
if [[ ! -d "${CONTEXT}" ]]; then
    fail "image context ${CONTEXT} is missing"
else
    if ! docker buildx inspect "${BUILDER}" > /dev/null 2>&1; then
        docker buildx create --name "${BUILDER}" --driver docker-container > /dev/null || fail "cannot create buildx builder ${BUILDER}"
    fi
    for arch in "${PLATFORMS[@]}"; do
        tar="${SCAN_DIR}/image-${arch}.tar"
        echo "== Building the JVM image for linux/${arch}"
        if docker buildx build --builder "${BUILDER}" --platform "linux/${arch}" \
            -f "${ROOT}/package/src/main/docker/Dockerfile" \
            --output "type=oci,dest=${tar}" "${CONTEXT}"; then
            scan "image-${arch}" "oci-archive:${tar}"
        else
            fail "the linux/${arch} image did not build"
        fi
    done
fi

if [[ ${FAILED} -ne 0 ]]; then
    echo "Release scan FAILED, see the errors above" >&2
    exit 1
fi
echo "Release scan passed"
