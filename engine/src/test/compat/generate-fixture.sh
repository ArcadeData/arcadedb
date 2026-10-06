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
# Regenerates the backward-compatibility fixture written by a RELEASED ArcadeDB engine (#9265).
#
#   engine/src/test/compat/generate-fixture.sh <release-version> [extra maven args...]
#   e.g. engine/src/test/compat/generate-fixture.sh 26.9.1
#
# Writes engine/src/test/resources/compat/db-<release-version>.zip. See README.md next to this script.
set -euo pipefail

if [ $# -lt 1 ]; then
  echo "Usage: $0 <release-version> [extra maven args...]" >&2
  exit 1
fi

VERSION="$1"
shift

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd -P)"
OUTPUT="$SCRIPT_DIR/../resources/compat/db-$VERSION.zip"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# A throwaway project whose only dependency is the released engine, so Maven resolves that release's own
# transitive classpath (never the working tree's).
cat > "$WORK/pom.xml" <<EOF
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>com.arcadedb.compat</groupId>
  <artifactId>fixture-generator</artifactId>
  <version>1</version>
  <packaging>pom</packaging>
  <dependencies>
    <dependency>
      <groupId>com.arcadedb</groupId>
      <artifactId>arcadedb-engine</artifactId>
      <version>$VERSION</version>
    </dependency>
  </dependencies>
</project>
EOF

mvn -q -f "$WORK/pom.xml" "$@" dependency:build-classpath -Dmdep.outputFile="$WORK/classpath.txt" -Dmdep.includeScope=runtime

java --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
  -cp "$(cat "$WORK/classpath.txt")" \
  "$SCRIPT_DIR/BackwardCompatFixtureGenerator.java" "$VERSION" "$OUTPUT"
