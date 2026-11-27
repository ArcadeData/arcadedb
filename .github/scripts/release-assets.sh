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
# Completes a GitHub release with the JVM distributions, their SBOMs, the VEX document and a signed SHA256SUMS over
# every asset. Run by .github/workflows/release-assets.yml once Native Image and publish-contract have both put their
# own assets on the release, so the checksums cover the complete set.
#
#   1. finds the mvn-release.yml run of the release (see release_run_id) and downloads its `release-dist` and
#      `release-image-sbom` artifacts: the exact files Maven Central received and `attest` attested
#   2. puts each CI-built file on the release: absent -> uploaded; present with the same SHA-256 -> left alone;
#      present with a different SHA-256 -> fails before uploading anything. A different file under the release name
#      means someone replaced it by hand or a different build ran: its attestation would no longer match, and
#      overwriting it would hide that.
#   3. converts the two distribution SBOMs to SPDX 2.3 JSON (the native and JVM image SBOMs arrive in both formats)
#   4. uploads vex/arcadedb.openvex.json as arcadedb-<tag>.openvex.json
#   5. downloads every asset, writes SHA256SUMS (sorted by name, SHA256SUMS* excluded), signs it with cosign
#      (keyless, SHA256SUMS.sigstore.json) and gpg (SHA256SUMS.asc), and uploads the three with --clobber
#
# Derived files (the SPDX conversions, the VEX copy, SHA256SUMS*) are replaced on every run: they are not attested,
# and a recovery run (native-image.yml `refresh-release-assets`) must re-sign checksums over replaced assets.
#
# When the mvn-release run's `release-dist` artifact has EXPIRED (90 days retention), the CI-built files already on
# the release are used instead, so a late recovery can still refresh SHA256SUMS. Expired means: the download fails AND
# the run's artifact list either reports `release-dist` with `expired: true` or no longer lists it at all (GitHub stops
# returning expired artifacts through the API). Those files are trusted only after `gh attestation verify` proves each
# JVM archive and distribution SBOM was attested by mvn-release.yml on main; a missing or unverified one fails. A
# download failure while `release-dist` is still listed and not expired fails, as does any other failure to find the
# run or list its artifacts: falling back then would sign whatever the release happens to carry.
#
# On a workflow_run trigger (GITHUB_EVENT_NAME=workflow_run) it first checks that BOTH upstream workflows finished
# successfully for the tag and exits 0 otherwise: each of them triggers this script when it completes, and only the
# second may sign, or the checksums would miss the other's assets. An upstream still running is a notice (its own
# completion triggers the run that signs); an upstream that concluded in failure is a warning, because nothing will
# produce SHA256SUMS until it is fixed and re-run. A failed or unparseable `gh run list` is an error (exit 1), never
# "still running": if both upstreams already finished, no later completion would retry.
#
# Usage: release-assets.sh <tag> <workdir>
#   env GH_REPO         owner/repo of the release (set by the workflow; default GITHUB_REPOSITORY, then
#                       ArcadeData/arcadedb). Also the repository in the signer identity of the fallback check, and
#                       what decides whether a rehearsal run counts (only outside ArcadeData/arcadedb)
#       MVN_RELEASE_RUN_ID  when set, the mvn-release.yml run to take the files from instead of searching for it; it
#                       must pass the same checks as a run found by the search, or the script fails
#       GPG_PASSPHRASE  passphrase of the imported release signing key (may be empty)
#       RELEASE_GPG_FINGERPRINT  when set, the key SHA256SUMS.asc is made with (--local-user)
#       SYFT            syft binary (default: syft on PATH)
#       GITHUB_OUTPUT   when set, receives `completed=true` once SHA256SUMS is signed and uploaded (not on an early exit)

set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "Usage: release-assets.sh <tag> <workdir>" >&2
    exit 2
fi

readonly TAG="$1"
readonly WORKDIR="$2"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
readonly ROOT
readonly VEX="${ROOT}/vex/arcadedb.openvex.json"
readonly SYFT="${SYFT:-syft}"
readonly UPSTREAM_WORKFLOWS=(native-image.yml publish-contract.yml)
readonly REPO="${GH_REPO:-${GITHUB_REPOSITORY:-ArcadeData/arcadedb}}"
if [[ ! "${REPO}" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]]; then
    echo "::error::'${REPO}' is not an owner/repo" >&2
    exit 2
fi
# For the fallback's attestation check: only mvn-release.yml, dispatched on main, attests the JVM archives and the
# distribution SBOMs, so nothing else may vouch for them (narrower than verify-release.sh's four-workflow identity).
readonly ISSUER="https://token.actions.githubusercontent.com"
readonly IDENTITY="^https://github\.com/${REPO//./\\.}/\.github/workflows/mvn-release\.yml@refs/heads/main$"

# Release tags are versions (`26.12.1`); the signer identity accepts only refs/tags/[0-9][^/]*. Anything else is
# refused before it reaches a file name or a gh argument.
if [[ ! "${TAG}" =~ ^[0-9][0-9A-Za-z._+-]*$ ]]; then
    echo "::error::'${TAG}' is not a release tag" >&2
    exit 2
fi

sha256_of() {
    sha256sum "$1" | cut -d' ' -f1
}

# Succeeds when the latest `release`-event run of each upstream workflow for <tag> completed with success.
# For a release event, a run's head_branch is the release's tag, which is what --branch filters on.
upstream_finished() {
    local tag="$1" workflow run status conclusion
    for workflow in "${UPSTREAM_WORKFLOWS[@]}"; do
        run="$(gh run list --workflow "${workflow}" --event release --branch "${tag}" --limit 1 --json databaseId,status,conclusion)" || {
            echo "::error::could not list the ${workflow} runs for ${tag}" >&2
            exit 1
        }
        if ! jq -e 'type == "array"' <<<"${run}" >/dev/null 2>&1; then
            echo "::error::the ${workflow} run list for ${tag} is not a JSON array" >&2
            exit 1
        fi
        status="$(jq -r '.[0].status // "missing"' <<<"${run}")"
        conclusion="$(jq -r '.[0].conclusion // ""' <<<"${run}")"
        if [[ "${status}" == completed && "${conclusion}" != success ]]; then
            echo "::warning::${workflow} for ${tag} is ${status}/${conclusion}; SHA256SUMS was not produced. Fix it and re-run ${workflow} (its successful completion triggers this workflow), or dispatch release-assets.yml once its assets are on the release." >&2
            return 1
        fi
        if [[ "${status}" != completed ]]; then
            echo "::notice::${workflow} for ${tag} is ${status}; nothing uploaded. The run its successful completion triggers (or a re-run) completes the release." >&2
            return 1
        fi
    done
    return 0
}

# The run titles that count as the release of <tag>. A rehearsal (`release <tag> (rehearsal)`) counts only outside
# ArcadeData/arcadedb: mvn-release.yml refuses to rehearse there, and a fork rehearses the whole chain with its own tag.
release_titles() {
    local repo_lc
    repo_lc="$(tr '[:upper:]' '[:lower:]' <<<"${REPO}")"
    if [[ "${repo_lc}" == arcadedata/arcadedb ]]; then
        jq -cn --arg t "release $1" '[$t]'
    else
        jq -cn --arg t "release $1" '[$t, $t + " (rehearsal)"]'
    fi
}

# Succeeds when run <id> can supply the release files: its `attest` job concluded success (so `central` uploaded
# `release-dist` and attested it) and its `sign-image` job concluded success (so the image was pushed and signed, and
# `release-image-sbom` uploaded). Jobs, not artifact records: the jobs of a run stay listed after its artifacts expired
# and vanished from the API. The run's overall conclusion does not matter: a failure in `tag` after that (a merge
# conflict on the next-version commit) leaves a complete, published release. Any API failure fails the script rather
# than skipping the run.
run_usable() {
    local id="$1" jobs
    jobs="$(gh api "repos/${REPO}/actions/runs/${id}/jobs?per_page=100")" || {
        echo "::error::could not list the jobs of mvn-release run ${id}" >&2
        exit 1
    }
    jq -e 'any(.jobs[]; .name == "attest" and .conclusion == "success") and
        any(.jobs[]; .name == "sign-image" and .conclusion == "success")' <<<"${jobs}" >/dev/null
}

# Database id of the mvn-release.yml run that released <tag>: the latest run dispatched on main, titled as
# release_titles allows, that passes run_usable. A dispatch from another branch is not a release (its attestations
# carry a branch identity nobody accepts). With MVN_RELEASE_RUN_ID set, that run is checked the same way instead.
release_run_id() {
    local tag="$1" titles candidates id
    titles="$(release_titles "${tag}")"
    if [[ -n "${MVN_RELEASE_RUN_ID:-}" ]]; then
        if [[ ! "${MVN_RELEASE_RUN_ID}" =~ ^[0-9]+$ ]]; then
            echo "::error::mvn_release_run_id '${MVN_RELEASE_RUN_ID}' is not a run id" >&2
            exit 1
        fi
        local run
        run="$(gh api "repos/${REPO}/actions/runs/${MVN_RELEASE_RUN_ID}")" || {
            echo "::error::mvn-release run ${MVN_RELEASE_RUN_ID} not found" >&2
            exit 1
        }
        if ! jq -e --argjson titles "${titles}" \
            '.path == ".github/workflows/mvn-release.yml" and .head_branch == "main" and (.display_title as $d | any($titles[]; . == $d))' \
            <<<"${run}" >/dev/null; then
            echo "::error::run ${MVN_RELEASE_RUN_ID} is not an mvn-release.yml run on main titled 'release ${tag}'" >&2
            exit 1
        fi
        if ! run_usable "${MVN_RELEASE_RUN_ID}"; then
            echo "::error::mvn-release run ${MVN_RELEASE_RUN_ID} has no successful \`attest\` and \`sign-image\` jobs" >&2
            exit 1
        fi
        echo "${MVN_RELEASE_RUN_ID}"
        return
    fi
    candidates="$(gh run list --workflow mvn-release.yml --limit 200 --json databaseId,displayTitle,headBranch |
        jq -r --argjson titles "${titles}" '.[] | select(.headBranch == "main" and (.displayTitle as $d | any($titles[]; . == $d))) | .databaseId')"
    for id in ${candidates}; do
        if run_usable "${id}"; then
            echo "${id}"
            return
        fi
    done
}

# "true" when the run's release-dist artifact is gone: listed with `expired: true`, or not listed at all (GitHub no
# longer returns expired artifacts). "false" while it is listed and not expired. An API failure fails the script.
release_dist_expired() {
    local artifacts
    # Filtered by name, so the answer does not depend on which page of a long artifact list release-dist is on.
    artifacts="$(gh api "repos/${REPO}/actions/runs/$1/artifacts?name=release-dist&per_page=100")" || {
        echo "::error::could not list the artifacts of mvn-release run $1" >&2
        exit 1
    }
    jq -r 'all(.artifacts[]; .name != "release-dist" or .expired == true)' <<<"${artifacts}"
}

if [[ "${GITHUB_EVENT_NAME:-workflow_dispatch}" == workflow_run ]] && ! upstream_finished "${TAG}"; then
    exit 0
fi

mkdir -p "${WORKDIR}"
WORK="$(mktemp -d "${WORKDIR}/release-assets.XXXXXX")"
readonly WORK

release_assets() {
    gh release view "${TAG}" --json assets | jq -r '.assets[].name'
}

on_release=$'\n'"$(release_assets)"$'\n'
is_on_release() {
    [[ "${on_release}" == *$'\n'"$1"$'\n'* ]]
}

# Published name -> path inside the downloaded artifacts.
readonly CI_BUILT=(
    "arcadedb-${TAG}.tar.gz=dist/arcadedb-${TAG}.tar.gz"
    "arcadedb-${TAG}.tar.gz.sha256=dist/arcadedb-${TAG}.tar.gz.sha256"
    "arcadedb-${TAG}-base.tar.gz=dist/arcadedb-${TAG}-base.tar.gz"
    "arcadedb-${TAG}-base.tar.gz.sha256=dist/arcadedb-${TAG}-base.tar.gz.sha256"
    "arcadedb-${TAG}.cdx.json=dist/sbom/full/arcadedb-${TAG}.cdx.json"
    "arcadedb-${TAG}-base.cdx.json=dist/sbom/base/arcadedb-${TAG}-base.cdx.json"
    "arcadedb-image-${TAG}.cdx.json=image/arcadedb-image-${TAG}.cdx.json"
    "arcadedb-image-${TAG}.spdx.json=image/arcadedb-image-${TAG}.spdx.json"
)

# --- 1. the CI-built files ---------------------------------------------------------------------------------------
SRC="${WORK}/src"
mkdir -p "${SRC}"
run_id="$(release_run_id "${TAG}")"
if [[ -z "${run_id}" ]]; then
    echo "::error::no mvn-release.yml run on main titled 'release ${TAG}' with successful attest and sign-image jobs" >&2
    exit 1
fi
from_ci=0
if gh run download "${run_id}" --name release-dist --dir "${WORK}/ci/dist" &&
    gh run download "${run_id}" --name release-image-sbom --dir "${WORK}/ci/image"; then
    from_ci=1
    echo "[assets] CI-built files from mvn-release run ${run_id}"
else
    # set -e: an API failure inside the substitution fails this assignment, and with it the script.
    expired="$(release_dist_expired "${run_id}")"
    if [[ "${expired}" != true ]]; then
        echo "::error::could not download the artifacts of mvn-release run ${run_id}, and they have not expired" >&2
        exit 1
    fi
    echo "::warning::the artifacts of mvn-release run ${run_id} have expired; using the CI-built files already on release ${TAG}, once their attestations verify" >&2
fi

missing=()
differs=()
to_upload=()
for entry in "${CI_BUILT[@]}"; do
    name="${entry%%=*}"
    if [[ ${from_ci} -eq 1 ]]; then
        built="${WORK}/ci/${entry#*=}"
        if [[ ! -f "${built}" ]]; then
            missing+=("${name} (not in the artifacts of run ${run_id})")
            continue
        fi
        cp "${built}" "${SRC}/${name}"
        if is_on_release "${name}"; then
            mkdir -p "${WORK}/remote"
            gh release download "${TAG}" --pattern "${name}" --dir "${WORK}/remote" --clobber
            if [[ "$(sha256_of "${WORK}/remote/${name}")" == "$(sha256_of "${SRC}/${name}")" ]]; then
                echo "[assets] ${name}: already on the release, same SHA-256"
            else
                differs+=("${name}")
            fi
        else
            to_upload+=("${SRC}/${name}")
        fi
    elif is_on_release "${name}"; then
        gh release download "${TAG}" --pattern "${name}" --dir "${SRC}" --clobber
    else
        missing+=("${name}")
    fi
done

for name in "${differs[@]+"${differs[@]}"}"; do
    echo "::error::${name} on release ${TAG} differs from the CI-built file" >&2
done
for name in "${missing[@]+"${missing[@]}"}"; do
    echo "::error::${name} is missing: neither on release ${TAG} nor in a usable mvn-release run" >&2
done
if [[ ${#differs[@]} -gt 0 || ${#missing[@]} -gt 0 ]]; then
    exit 1
fi

# Files taken from the release instead of from CI: nothing vouches for them but their attestations.
if [[ ${from_ci} -eq 0 ]]; then
    unverified=0
    for name in "arcadedb-${TAG}.tar.gz" "arcadedb-${TAG}-base.tar.gz" "arcadedb-${TAG}.cdx.json" "arcadedb-${TAG}-base.cdx.json"; do
        if ! gh attestation verify "${SRC}/${name}" --repo "${REPO}" --cert-identity-regex "${IDENTITY}" \
            --cert-oidc-issuer "${ISSUER}" >/dev/null; then
            echo "::error::${name} on release ${TAG} has no verified build provenance; not signing it" >&2
            unverified=1
        fi
    done
    [[ ${unverified} -eq 0 ]] || exit 1
fi

# --- 2. upload what the release does not carry yet ------------------------------------------------------------------
if [[ ${#to_upload[@]} -gt 0 ]]; then
    echo "[assets] uploading ${#to_upload[@]} CI-built file(s)"
    gh release upload "${TAG}" "${to_upload[@]}"
fi

# --- 3. + 4. derived files ------------------------------------------------------------------------------------------
DERIVED="${WORK}/derived"
mkdir -p "${DERIVED}"
for sbom in "arcadedb-${TAG}" "arcadedb-${TAG}-base"; do
    "${SYFT}" convert "${SRC}/${sbom}.cdx.json" -o "spdx-json@2.3=${DERIVED}/${sbom}.spdx.json"
done
cp "${VEX}" "${DERIVED}/arcadedb-${TAG}.openvex.json"
gh release upload "${TAG}" \
    "${DERIVED}/arcadedb-${TAG}.spdx.json" "${DERIVED}/arcadedb-${TAG}-base.spdx.json" \
    "${DERIVED}/arcadedb-${TAG}.openvex.json" --clobber

# --- 5. SHA256SUMS over every asset, signed ------------------------------------------------------------------------
ALL="${WORK}/all"
SUMS="${WORK}/sums"
mkdir -p "${ALL}" "${SUMS}"
gh release download "${TAG}" --dir "${ALL}"
(
    cd "${ALL}"
    find . -maxdepth 1 -type f ! -name 'SHA256SUMS*' -exec basename {} \; | LC_ALL=C sort | while IFS= read -r f; do
        sha256sum "${f}"
    done
) >"${SUMS}/SHA256SUMS"
if [[ ! -s "${SUMS}/SHA256SUMS" ]]; then
    echo "::error::release ${TAG} has no assets to checksum" >&2
    exit 1
fi
echo "[assets] SHA256SUMS covers $(wc -l <"${SUMS}/SHA256SUMS" | tr -d ' ') asset(s)"

cosign sign-blob --yes --bundle "${SUMS}/SHA256SUMS.sigstore.json" "${SUMS}/SHA256SUMS"
signer=()
if [[ -n "${RELEASE_GPG_FINGERPRINT:-}" ]]; then
    signer=(--local-user "${RELEASE_GPG_FINGERPRINT// /}")
fi
gpg --batch --yes --pinentry-mode loopback --passphrase-fd 0 "${signer[@]+"${signer[@]}"}" --armor --detach-sign \
    --output "${SUMS}/SHA256SUMS.asc" "${SUMS}/SHA256SUMS" <<<"${GPG_PASSPHRASE:-}"

gh release upload "${TAG}" "${SUMS}/SHA256SUMS" "${SUMS}/SHA256SUMS.sigstore.json" "${SUMS}/SHA256SUMS.asc" --clobber
echo "[assets] release ${TAG} complete"
if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    echo "completed=true" >>"${GITHUB_OUTPUT}"
fi
