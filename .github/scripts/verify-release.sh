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
# Verifies a published release end to end, the way a consumer would: every asset against the signed SHA256SUMS, the
# cosign and gpg signatures of SHA256SUMS, the build provenance of every archive and contract file, and the signatures,
# SBOM attestations and SLSA build provenance of the Docker images. Each signature must come from one of the four release workflows of this
# repository, on main or on a release tag: other workflows in the repository can also mint OIDC certificates
# (load-tests, benchmarks), and a pattern accepting any workflow would accept theirs.
#
# Every check runs even when an earlier one failed, so one run lists everything that is wrong; it exits 1 if any did.
#
# Usage: verify-release.sh <tag>
#   env GITHUB_REPOSITORY    owner/repo of the release and of the signer identity (default ArcadeData/arcadedb), so a
#                            fork verifies against its own identity
#       DOCKER_NAMESPACE     Docker Hub namespace of the JVM and native images (default arcadedata); a fork that
#                            rehearses a release sets it to its scratch namespace, as its workflows push there
#       NATIVE_ARCHES        per-arch native image tags (default "amd64 arm64"), as native-image.yml pushes them
#       GPG_PUBLIC_KEY_FILE  ASCII-armored public key to check SHA256SUMS.asc against, in a throwaway keyring; a
#                            relative path is relative to the caller's directory. Unset: the caller's own keyring (a
#                            consumer who imported the ArcadeDB release key)
#       RELEASE_GPG_FINGERPRINT  REQUIRED: fingerprint of the release signing key. A valid signature by any other key
#                            in the keyring is a failure (case and spaces ignored; the signing subkey's or the primary
#                            key's fingerprint both match, as gpg reports both)

set -uo pipefail

if [[ $# -ne 1 ]]; then
    echo "Usage: verify-release.sh <tag>" >&2
    exit 2
fi

readonly TAG="$1"
if [[ ! "${TAG}" =~ ^[0-9][0-9A-Za-z._+-]*$ ]]; then
    echo "::error::'${TAG}' is not a release tag" >&2
    exit 2
fi
readonly REPO="${GITHUB_REPOSITORY:-ArcadeData/arcadedb}"
if [[ ! "${REPO}" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]]; then
    echo "::error::'${REPO}' is not an owner/repo" >&2
    exit 2
fi
readonly NS="${DOCKER_NAMESPACE:-arcadedata}"
# Resolved now: the checks below run inside the download directory.
GPG_KEY_FILE="${GPG_PUBLIC_KEY_FILE:-}"
if [[ -n "${GPG_KEY_FILE}" && "${GPG_KEY_FILE}" != /* ]]; then
    GPG_KEY_FILE="${PWD}/${GPG_KEY_FILE}"
fi
readonly GPG_KEY_FILE
read -r -a ARCHES <<<"${NATIVE_ARCHES:-amd64 arm64}"
readonly ARCHES

readonly ISSUER="https://token.actions.githubusercontent.com"
# The repository name goes into a regular expression: its dots must match only dots.
readonly IDENTITY="^https://github\.com/${REPO//./\\.}/\.github/workflows/(mvn-release|native-image|release-assets|publish-contract)\.yml@refs/(heads/main|tags/[0-9][^/]*)$"

normalize_fpr() {
    # tr, not ${f^^}: consumers run this with macOS's /bin/bash 3.2.
    local f="${1// /}"
    echo "${f}" | tr '[:lower:]' '[:upper:]'
}

failures=()
fail() {
    failures+=("$*")
    echo "::error::$*" >&2
}

DIR="$(mktemp -d)"
readonly DIR
trap 'rm -rf "${DIR}"' EXIT

echo "== Verifying release ${TAG} of ${REPO}"
echo "   signer identity ${IDENTITY}"

mkdir -p "${DIR}/assets"
if ! gh release download "${TAG}" --repo "${REPO}" --dir "${DIR}/assets"; then
    fail "could not download the assets of release ${TAG}"
fi
cd "${DIR}/assets" || exit 1

# --- the asset set -------------------------------------------------------------------------------------------------
required=(
    "arcadedb-${TAG}.tar.gz" "arcadedb-${TAG}.tar.gz.sha256"
    "arcadedb-${TAG}-base.tar.gz" "arcadedb-${TAG}-base.tar.gz.sha256"
    "arcadedb-${TAG}.cdx.json" "arcadedb-${TAG}.spdx.json"
    "arcadedb-${TAG}-base.cdx.json" "arcadedb-${TAG}-base.spdx.json"
    "arcadedb-image-${TAG}.cdx.json" "arcadedb-image-${TAG}.spdx.json"
    "arcadedb-${TAG}.openvex.json"
    "arcadedb-openapi-${TAG}.json" "arcadedb-openapi-${TAG}.json.sha256"
    "arcadedb-server-${TAG}.proto" "arcadedb-server-${TAG}.proto.sha256"
    SHA256SUMS SHA256SUMS.sigstore.json SHA256SUMS.asc
)
for arch in "${ARCHES[@]}"; do
    required+=("arcadedb-image-${TAG}-native-${arch}.cdx.json" "arcadedb-image-${TAG}-native-${arch}.spdx.json")
done
for name in "${required[@]}"; do
    [[ -f "${name}" ]] || fail "${name} is missing from the release"
done

archives=()
native=0
while IFS= read -r f; do
    archives+=("${f}")
    [[ "${f}" != "arcadedb-${TAG}.tar.gz" && "${f}" != "arcadedb-${TAG}-base.tar.gz" ]] && native=$((native + 1))
done < <(find . -maxdepth 1 -type f \( -name '*.tar.gz' -o -name '*.zip' \) -exec basename {} \; | LC_ALL=C sort)
[[ ${native} -gt 0 ]] || fail "no native archive on the release"

# --- SHA256SUMS ----------------------------------------------------------------------------------------------------
if [[ -f SHA256SUMS ]]; then
    sha256sum -c SHA256SUMS || fail "SHA256SUMS does not match the release assets"
    # An asset SHA256SUMS does not list is covered by no signature at all.
    while IFS= read -r f; do
        awk -v f="${f}" '$2 == f || $2 == "*" f { found = 1 } END { exit !found }' SHA256SUMS || fail "${f} is not covered by SHA256SUMS"
    done < <(find . -maxdepth 1 -type f ! -name 'SHA256SUMS*' -exec basename {} \; | LC_ALL=C sort)

    if [[ -f SHA256SUMS.sigstore.json ]]; then
        cosign verify-blob --bundle SHA256SUMS.sigstore.json \
            --certificate-identity-regexp "${IDENTITY}" --certificate-oidc-issuer "${ISSUER}" SHA256SUMS ||
            fail "the cosign signature of SHA256SUMS does not verify"
    fi

    # "Good signature" alone means signed by SOME key in the keyring; the fingerprint pins WHICH.
    expected_fpr="$(normalize_fpr "${RELEASE_GPG_FINGERPRINT:-}")"
    if [[ -z "${expected_fpr}" ]]; then
        fail "set the repository variable RELEASE_GPG_FINGERPRINT to the release signing key fingerprint"
    elif [[ -f SHA256SUMS.asc ]]; then
        gpg_cmd=(gpg --batch)
        if [[ -n "${GPG_KEY_FILE}" ]]; then
            keyring="$(mktemp -d "${DIR}/gnupg.XXXXXX")"
            chmod 700 "${keyring}"
            gpg_cmd=(env "GNUPGHOME=${keyring}" gpg --batch)
            "${gpg_cmd[@]}" --import "${GPG_KEY_FILE}" || fail "could not import the public key ${GPG_KEY_FILE}"
        fi
        if status="$("${gpg_cmd[@]}" --status-fd 1 --verify SHA256SUMS.asc SHA256SUMS)"; then
            # [GNUPG:] VALIDSIG <signing key fpr> ... <primary key fpr>
            read -r -a validsig <<<"$(grep -m1 '^\[GNUPG:\] VALIDSIG ' <<<"${status}")"
            signing_fpr="$(normalize_fpr "${validsig[2]:-}")"
            primary_fpr="$(normalize_fpr "${validsig[11]:-}")"
            if [[ "${signing_fpr}" != "${expected_fpr}" && "${primary_fpr}" != "${expected_fpr}" ]]; then
                fail "SHA256SUMS.asc is signed by ${signing_fpr:-an unknown key}, not by the release key ${expected_fpr}"
            fi
        else
            fail "the gpg signature of SHA256SUMS does not verify"
        fi
    fi
fi

# --- build provenance ----------------------------------------------------------------------------------------------
attest() {
    gh attestation verify "$1" --repo "${REPO}" --cert-identity-regex "${IDENTITY}" --cert-oidc-issuer "${ISSUER}" "${@:2}" >/dev/null
}
for f in "${archives[@]+"${archives[@]}"}" "arcadedb-openapi-${TAG}.json" "arcadedb-server-${TAG}.proto"; do
    [[ -f "${f}" ]] || continue
    attest "${f}" || fail "${f} has no verified build provenance"
done
# mvn-release.yml binds each JVM archive to the SBOM of its contents.
for f in "arcadedb-${TAG}.tar.gz" "arcadedb-${TAG}-base.tar.gz"; do
    [[ -f "${f}" ]] || continue
    attest "${f}" --predicate-type https://cyclonedx.org/bom || fail "${f} has no verified SBOM attestation"
done

# --- images --------------------------------------------------------------------------------------------------------
cosign_identity=(--certificate-identity-regexp "${IDENTITY}" --certificate-oidc-issuer "${ISSUER}")
signed=("${NS}/arcadedb:${TAG}" "${NS}/arcadedb:${TAG}-native")
with_sbom=("${NS}/arcadedb:${TAG}")
for arch in "${ARCHES[@]}"; do
    signed+=("${NS}/arcadedb:${TAG}-native-${arch}")
    with_sbom+=("${NS}/arcadedb:${TAG}-native-${arch}")
done
for image in "${signed[@]}"; do
    cosign verify "${cosign_identity[@]}" "${image}" >/dev/null || fail "${image} has no verified signature"
done
# The `-native` manifest list carries a signature only; the SBOMs and the SLSA build provenance are attested on the
# JVM image and the per-arch images it lists. The provenance is pushed to the registry under index.docker.io, the
# subject name the workflows give actions/attest-build-provenance; `gh attestation verify` defaults to its predicate.
for image in "${with_sbom[@]}"; do
    cosign verify-attestation --type cyclonedx "${cosign_identity[@]}" "${image}" >/dev/null ||
        fail "${image} has no verified CycloneDX SBOM attestation"
    attest "oci://index.docker.io/${image}" || fail "${image} has no verified build provenance"
done

echo
if [[ ${#failures[@]} -gt 0 ]]; then
    echo "Release ${TAG}: ${#failures[@]} check(s) failed:" >&2
    printf '  - %s\n' "${failures[@]}" >&2
    exit 1
fi
echo "Release ${TAG}: every check passed."
