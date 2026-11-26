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
# Applies the release version to every file that carries it. Run from the repository root.
#
# mvn-release.yml runs this in every job that builds or commits the release: `preflight` scans the tree it produces,
# `central` deploys it, `docker` builds the image from it, and `tag` commits and tags it. Keeping the edits in one
# place is what guarantees the tag points at exactly the sources that went to Maven Central.
#
# server.json is the manifest the MCP registry publishes this server from. It is not a Maven artifact, so
# versions:set never reaches it and it silently drifted six releases behind HEAD (#6838). It names the last
# RELEASE, so the next-development step of the release workflow deliberately leaves it alone rather than pinning a
# -SNAPSHOT container tag that was never pushed.

set -euo pipefail

if [ $# -ne 1 ] || [ -z "$1" ]; then
  echo "usage: $0 <release-version>" >&2
  exit 1
fi

readonly VERSION="$1"

./mvnw versions:set "-DnewVersion=${VERSION}" -DgenerateBackupPoms=false --no-transfer-progress

jq --arg version "${VERSION}" '.version = $version' studio/package.json > studio/package.json.tmp
mv studio/package.json.tmp studio/package.json

# The lockfile carries the version twice. Before the release workflow was split, `npm install` in the deploy build
# rewrote it and `git commit -am` picked that up; the job that commits the release no longer runs npm.
jq --arg version "${VERSION}" '.version = $version | .packages[""].version = $version' \
  studio/package-lock.json > studio/package-lock.json.tmp
mv studio/package-lock.json.tmp studio/package-lock.json

jq --arg version "${VERSION}" \
  '.version = $version | .packages |= map(.identifier |= sub(":[^:]+$"; ":" + $version))' \
  server.json > server.json.tmp
mv server.json.tmp server.json
