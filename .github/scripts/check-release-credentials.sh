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
# Checks, without publishing or changing anything, that the credentials a release needs work, so a wrong passphrase
# or a read-only token fails before the release builds instead of in the middle of it. A fork rehearsal never uses
# the real credentials, so this is the only test they get before the release itself.
#
#   - the GPG key in GPG_PRIVATE_KEY signs with GPG_PASSPHRASE, the signature verifies, and the key that made it is
#     RELEASE_GPG_FINGERPRINT (the key Maven signs the jars with, and release-assets.yml signs SHA256SUMS with);
#   - that key does not expire within EXPIRY_FAIL_DAYS (fail) or EXPIRY_WARN_DAYS (warning);
#   - the public key is on keyserver.ubuntu.com or keys.openpgp.org, where Maven Central looks it up (warning when
#     only one has it);
#   - the Maven Central Portal token is accepted, and the release version is not published yet;
#     these two are skipped with SKIP_CENTRAL=true, for a fork rehearsal that never publishes;
#   - the Docker Hub token is granted `push` on DOCKER_REPOSITORY;
#   - the release tag does not exist yet in GITHUB_REPOSITORY.
#
# Every check runs even after a failure, so one run lists everything that is wrong.
#
# Usage:
#   check-release-credentials.sh <release-version>
#
# Environment:
#   GPG_PRIVATE_KEY, GPG_PASSPHRASE   REQUIRED: the MAVEN_GPG_PRIVATE_KEY / MAVEN_GPG_PASSPHRASE secrets
#   RELEASE_GPG_FINGERPRINT           REQUIRED: the expected signing key (primary or signing subkey; case and spaces
#                                     ignored)
#   CENTRAL_USERNAME, CENTRAL_PASSWORD  REQUIRED unless SKIP_CENTRAL=true: the Central Portal user token
#   CENTRAL_NAMESPACE, CENTRAL_ARTIFACT  component probed on Central. Default com.arcadedb / arcadedb-engine
#   DOCKER_USERNAME, DOCKER_PASSWORD  REQUIRED: the Docker Hub account the release pushes with
#   DOCKER_REPOSITORY                 image repository the release pushes. Default arcadedata/arcadedb
#   GITHUB_REPOSITORY                 owner/repo of the release tag. Default ArcadeData/arcadedb
#   GITHUB_TOKEN                      optional, for the tag lookup
#   EXPIRY_FAIL_DAYS, EXPIRY_WARN_DAYS  defaults 30 / 90
#
# Exit status: 0 all checks passed (warnings allowed), 1 at least one check failed, 2 usage error.

set -uo pipefail

if [[ $# -ne 1 || -z $1 ]]; then
    echo "Usage: $0 <release-version>" >&2
    exit 2
fi
readonly VERSION="$1"

readonly CENTRAL_NAMESPACE="${CENTRAL_NAMESPACE:-com.arcadedb}"
readonly CENTRAL_ARTIFACT="${CENTRAL_ARTIFACT:-arcadedb-engine}"
readonly DOCKER_REPOSITORY="${DOCKER_REPOSITORY:-arcadedata/arcadedb}"
readonly REPOSITORY="${GITHUB_REPOSITORY:-ArcadeData/arcadedb}"
readonly EXPIRY_FAIL_DAYS="${EXPIRY_FAIL_DAYS:-30}"
readonly EXPIRY_WARN_DAYS="${EXPIRY_WARN_DAYS:-90}"

failures=0
warnings=0

ok() { echo "ok      - $1"; }
warn() {
    warnings=$((warnings + 1))
    echo "::warning::$1"
}
fail() {
    failures=$((failures + 1))
    echo "::error::$1"
}

work="$(mktemp -d)"
trap 'gpgconf --homedir "$work/gnupg" --kill gpg-agent >/dev/null 2>&1; rm -rf "$work"' EXIT
chmod 700 "$work"

# GET <url> into <out>, sending the headers in the files that follow (so no credential shows on a command line).
# Prints the HTTP status, 000 when the request did not complete.
http_get() {
    local url="$1" out="$2"
    shift 2
    local args=() h
    for h in "$@"; do
        args+=(-H "@$h")
    done
    curl -sS --max-time 30 -o "$out" -w '%{http_code}' ${args[@]+"${args[@]}"} "$url" 2>"$work/curl.err" || true
}

# Writes `Authorization: <scheme> base64(user:password)` to a private file and prints its path.
auth_header() {
    local scheme="$1" user="$2" password="$3" file
    file="$(mktemp "$work/header.XXXXXX")"
    printf 'Authorization: %s %s\n' "$scheme" "$(printf '%s:%s' "$user" "$password" | base64 | tr -d '\n')" >"$file"
    echo "$file"
}

normalize_fpr() { tr -d ' \t' <<<"$1" | tr '[:lower:]' '[:upper:]'; }

# --- GPG -------------------------------------------------------------------------------------------------------------

expected_fpr="$(normalize_fpr "${RELEASE_GPG_FINGERPRINT:-}")"
signed_by=""
if [[ -z $expected_fpr ]]; then
    fail "RELEASE_GPG_FINGERPRINT is not set"
fi
if [[ -z ${GPG_PRIVATE_KEY:-} ]]; then
    fail "the GPG private key (MAVEN_GPG_PRIVATE_KEY) is empty"
else
    export GNUPGHOME="$work/gnupg"
    mkdir -m 700 "$GNUPGHOME"
    if ! gpg --batch --import <<<"$GPG_PRIVATE_KEY" >"$work/import.log" 2>&1; then
        fail "the GPG private key (MAVEN_GPG_PRIVATE_KEY) does not import: $(tail -1 "$work/import.log")"
    else
        echo "release ${VERSION}" >"$work/probe.txt"
        # No --local-user: maven-gpg-plugin signs with the default key too, so this signs with the key the jars get.
        if ! printf '%s' "${GPG_PASSPHRASE:-}" | gpg --batch --pinentry-mode loopback --passphrase-fd 0 \
            --detach-sign --output "$work/probe.sig" "$work/probe.txt" >"$work/sign.log" 2>&1; then
            fail "GPG signing failed, MAVEN_GPG_PASSPHRASE is probably not the passphrase of MAVEN_GPG_PRIVATE_KEY: $(tail -1 "$work/sign.log")"
        else
            status="$(gpg --batch --status-fd 1 --verify "$work/probe.sig" "$work/probe.txt" 2>/dev/null)"
            validsig="$(grep '^\[GNUPG:\] VALIDSIG ' <<<"$status" | head -1)"
            if [[ -z $validsig ]]; then
                fail "the test signature does not verify"
            else
                signed_by="$(awk '{print $3}' <<<"$validsig")"
                primary="$(awk '{print $12}' <<<"$validsig")"
                if [[ -n $expected_fpr && $signed_by != "$expected_fpr" && $primary != "$expected_fpr" ]]; then
                    fail "MAVEN_GPG_PRIVATE_KEY signs with ${primary}, but RELEASE_GPG_FINGERPRINT is ${expected_fpr}: update them together"
                else
                    ok "MAVEN_GPG_PRIVATE_KEY and MAVEN_GPG_PASSPHRASE sign as ${primary}"
                fi
            fi
        fi
    fi
fi

if [[ -n $signed_by ]]; then
    # Expiry of the key that made the signature: the record whose fpr line is the signing key's.
    # Prints the expiry epoch, `never`, or nothing when the key is not found (a failure, never "does not expire").
    expires="$(gpg --batch --with-colons --list-keys "$signed_by" 2>/dev/null |
        awk -F: -v fpr="$signed_by" '$1 == "pub" || $1 == "sub" { until = ($7 == "" ? "never" : $7) }
            $1 == "fpr" && $10 == fpr { print until; exit }')"
    if [[ -z $expires ]]; then
        fail "could not read the expiry of the signing key ${signed_by}"
    elif [[ $expires == never ]]; then
        ok "the signing key does not expire"
    else
        days=$(((expires - $(date +%s)) / 86400))
        if ((days < EXPIRY_FAIL_DAYS)); then
            fail "the signing key expires in ${days} days: extend it (gpg --quick-set-expire), republish it and update MAVEN_GPG_PRIVATE_KEY"
        elif ((days < EXPIRY_WARN_DAYS)); then
            warn "the signing key expires in ${days} days: extend it (gpg --quick-set-expire), republish it and update MAVEN_GPG_PRIVATE_KEY"
        else
            ok "the signing key expires in ${days} days"
        fi
    fi
fi

# Maven Central is the only consumer that looks the key up on a keyserver, so a rehearsal (SKIP_CENTRAL=true, signing
# with a throwaway test key) skips this with it.
if [[ ${SKIP_CENTRAL:-false} == true ]]; then
    echo "skipped - keyservers (SKIP_CENTRAL=true)"
elif [[ -n $expected_fpr ]]; then
    found=()
    missing=()
    code="$(http_get "https://keyserver.ubuntu.com/pks/lookup?op=get&options=mr&search=0x${expected_fpr}" "$work/ubuntu.key")"
    if [[ $code == 200 ]]; then found+=(keyserver.ubuntu.com); else missing+=(keyserver.ubuntu.com); fi
    code="$(http_get "https://keys.openpgp.org/vks/v1/by-fingerprint/${expected_fpr}" "$work/openpgp.key")"
    if [[ $code == 200 ]]; then found+=(keys.openpgp.org); else missing+=(keys.openpgp.org); fi
    if [[ ${#found[@]} -eq 0 ]]; then
        fail "the public key ${expected_fpr} is on neither keyserver.ubuntu.com nor keys.openpgp.org: Maven Central will reject the signatures"
    elif [[ ${#missing[@]} -gt 0 ]]; then
        warn "the public key ${expected_fpr} is on ${found[*]} but not on ${missing[*]}"
    else
        ok "the public key is on ${found[*]}"
    fi
fi

# --- Maven Central ---------------------------------------------------------------------------------------------------

if [[ ${SKIP_CENTRAL:-false} == true ]]; then
    echo "skipped - Maven Central (SKIP_CENTRAL=true)"
elif [[ -z ${CENTRAL_USERNAME:-} || -z ${CENTRAL_PASSWORD:-} ]]; then
    fail "the Maven Central credentials (MAVEN_CENTRAL_USERNAME / MAVEN_CENTRAL_PASSWORD) are empty"
else
    header="$(auth_header Bearer "$CENTRAL_USERNAME" "$CENTRAL_PASSWORD")"
    code="$(http_get "https://central.sonatype.com/api/v1/publisher/published?namespace=${CENTRAL_NAMESPACE}&name=${CENTRAL_ARTIFACT}&version=${VERSION}" \
        "$work/central.json" "$header")"
    case "$code" in
    200)
        if [[ "$(jq -r '.published' "$work/central.json" 2>/dev/null)" == true ]]; then
            fail "${CENTRAL_NAMESPACE}:${CENTRAL_ARTIFACT}:${VERSION} is already published on Maven Central: pick another version"
        else
            ok "the Maven Central token is accepted and ${VERSION} is not published yet"
        fi
        ;;
    401 | 403) fail "Maven Central rejects MAVEN_CENTRAL_USERNAME / MAVEN_CENTRAL_PASSWORD (HTTP ${code}): generate a new Portal user token" ;;
    *) fail "Maven Central answered HTTP ${code} to the credential check: $(head -c 300 "$work/central.json" 2>/dev/null) $(cat "$work/curl.err" 2>/dev/null)" ;;
    esac
fi

# --- Docker Hub ------------------------------------------------------------------------------------------------------

if [[ -z ${DOCKER_USERNAME:-} || -z ${DOCKER_PASSWORD:-} ]]; then
    fail "the Docker Hub credentials (DOCKER_USERNAME / DOCKER_PASSWORD) are empty"
else
    header="$(auth_header Basic "$DOCKER_USERNAME" "$DOCKER_PASSWORD")"
    # The registry grants the subset of the requested actions the token holds: a read-only token gets a token with
    # `pull` only, which is the failure this is here to catch.
    code="$(http_get "https://auth.docker.io/token?service=registry.docker.io&scope=repository:${DOCKER_REPOSITORY}:push,pull" \
        "$work/docker.json" "$header")"
    if [[ $code == 401 || $code == 403 ]]; then
        fail "Docker Hub rejects DOCKER_USERNAME / DOCKER_PASSWORD (HTTP ${code})"
    elif [[ $code != 200 ]]; then
        fail "Docker Hub answered HTTP ${code} to the credential check: $(cat "$work/curl.err" 2>/dev/null)"
    else
        payload="$(jq -r '.token // .access_token // empty' "$work/docker.json" 2>/dev/null | cut -d. -f2 | tr '_-' '/+')"
        while ((${#payload} % 4)); do payload+="="; done
        actions="$(base64 -d <<<"$payload" 2>/dev/null |
            jq -r --arg repo "$DOCKER_REPOSITORY" '[.access[]? | select(.name == $repo) | .actions[]] | join(",")' 2>/dev/null)"
        if [[ ",${actions}," == *",push,"* ]]; then
            ok "the Docker Hub token can push to ${DOCKER_REPOSITORY}"
        else
            fail "the Docker Hub token of ${DOCKER_USERNAME} cannot push to ${DOCKER_REPOSITORY} (granted: ${actions:-nothing}): use a token with Read & Write scope"
        fi
    fi
fi

# --- GitHub tag ------------------------------------------------------------------------------------------------------

headers=()
if [[ -n ${GITHUB_TOKEN:-} ]]; then
    headers+=("$work/github.header")
    printf 'Authorization: Bearer %s\n' "$GITHUB_TOKEN" >"$work/github.header"
fi
code="$(http_get "https://api.github.com/repos/${REPOSITORY}/git/ref/tags/${VERSION}" "$work/tag.json" ${headers[@]+"${headers[@]}"})"
case "$code" in
404) ok "the tag ${VERSION} does not exist yet in ${REPOSITORY}" ;;
200) fail "the tag ${VERSION} already exists in ${REPOSITORY}: the release would publish to Central and then fail to tag" ;;
*) fail "GitHub answered HTTP ${code} to the tag lookup of ${VERSION}" ;;
esac

echo
if ((failures > 0)); then
    echo "${failures} check(s) failed, ${warnings} warning(s)"
    exit 1
fi
echo "all checks passed, ${warnings} warning(s)"
