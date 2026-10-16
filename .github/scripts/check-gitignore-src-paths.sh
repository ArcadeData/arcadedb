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
# Fails when an ignore pattern hides a directory under a source tree (#10162).
#
# The tracked-file check of gitignore-hygiene.yml proves that no pattern contradicts what is already committed. It
# cannot see the loss case: a new file under an ignored package with no tracked sibling is skipped by `git add`, never
# enters the index, and nothing reports it (#9981 hid com/arcadedb/log that way). This check is computed from the
# PATTERNS instead, so it holds for a file nobody has written yet: every pattern that names a directory by itself
# (`log`, `out`, `**/replication/`) is probed as a package folder of every source tree its .gitignore governs, the root
# file governing all of them. A pattern that names a path INTO a source tree (`engine/src/main/java/foo/`) is probed at
# the path it names, its wildcards filled in, since one written after the re-including rule wins over it. The other
# patterns with a path or a wildcard in them are anchored outside the source trees or are file patterns, and are not
# probed.
#
# Fix a failure with the `!**/src/**/` rule at the end of the .gitignore that holds the pattern (`!src/**/` in a
# module's own file), which re-includes every directory of the source trees at once, or by anchoring the pattern.
#
# Usage: .github/scripts/check-gitignore-src-paths.sh   (from anywhere inside the repository)

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

# Names that are output wherever they appear, source trees included, and so are meant to stay hidden there. One per line,
# with the reason: a new entry is a decision that the name can never be a source folder, not a way to silence the check
DELIBERATE='__pycache__	the Python bytecode cache, written next to every module it compiles'

# The patterns that name build output written INSIDE a source tree, restated after the re-including rule on purpose. The
# same rule as above: a new entry is a decision that the path is output, with the reason
DELIBERATE_PATHS='/grpc/src/generated/	the gRPC stubs generated into the source tree
/doc/src/main/asciidoc/*/.asciidoctor/	the Asciidoctor cache
/doc/src/main/asciidoc/*/var/	the Asciidoctor working files
**/src/**/*.egg-info/	the Python packaging metadata, written next to the package
**/src/**/__pycache__/	the Python bytecode cache
src/**/*.egg-info/	the Python packaging metadata, written next to the package
src/**/__pycache__/	the Python bytecode cache
src/main/resources/static/dist/	the vendor libraries webpack copies into Studio
src/main/resources/static/swagger-ui/	the Swagger UI webpack copies into Studio
src/arcadedb_embedded/jre/	the JRE bindings/python/scripts/build-native.sh bundles
src/arcadedb_embedded/jars/	the jars bindings/python/scripts/build-native.sh bundles
src/arcadedb_embedded/_version.py	the version module bindings/python/scripts/write_version.py writes
bindings/python/src/arcadedb_embedded/_version.py	the version module bindings/python/scripts/write_version.py writes'

probes="$(mktemp)"
trap 'rm -f "$probes"' EXIT

# The source trees: the leading path of every tracked file up to its first "src" segment
roots="$(git ls-files | awk -F'/src/' 'NF > 1 { print $1 "/src" }' | sort -u)"

while IFS= read -r ignore_file; do
  dir="$(dirname "$ignore_file")"
  prefix=""
  [ "$dir" = "." ] || prefix="$dir/"

  # The patterns that name a path into a source tree, probed where they point: a leading / dropped, every wildcard filled in
  sed -e 's/[[:space:]]*$//' -e '/^#/d' -e '/^$/d' -e '/^!/d' "$ignore_file" | { grep -E '(^|/)src/' || true; } \
    | sed -e 's#^/##' -e 's#\*\*#x#g' -e 's#[*?]#x#g' -e 's#/$##' \
    | while IFS= read -r path; do printf '%s%s/Probe.java\n' "$prefix" "$path"; done

  # The names a pattern hides wherever they appear: no comment, blank or negation, a leading **/ and a trailing / dropped,
  # and nothing left with a path separator, a wildcard or an escape in it
  names="$(sed -e 's/[[:space:]]*$//' -e '/^#/d' -e '/^$/d' -e '/^!/d' -e 's#^\*\*/##' -e 's#/$##' "$ignore_file" \
    | grep -v '[][/*?\\]' | grep -vxF -f <(cut -f1 <<< "$DELIBERATE") | sort -u || true)"
  [ -n "$names" ] || continue

  while IFS= read -r root; do
    # a module's own .gitignore governs only the source trees below it
    if [ "$dir" != "." ]; then
      case "$root" in
        "$dir"/*) ;;
        *) continue ;;
      esac
    fi
    while IFS= read -r name; do
      printf '%s/main/probe/%s/Probe.java\n' "$root" "$name"
    done <<< "$names"
  done <<< "$roots"
done < <(git ls-files -- '.gitignore' '*/.gitignore') > "$probes"

# --no-index: the probed paths are not tracked, and the answer must not depend on what is. Exit status 1 means "nothing
# matched", which is the pass case, so test the output, not the status. A hit by a deliberate path pattern is expected
hidden="$({ git check-ignore --no-index --verbose --stdin < "$probes" || true; } \
  | awk -F'\t' 'NR == FNR { allowed[$1] = 1; next }
      { source = $1; sub(/^[^:]*:[0-9]+:/, "", source); if (!(source in allowed)) print }' <(printf '%s\n' "$DELIBERATE_PATHS") -)"
if [ -n "$hidden" ]; then
  echo "::error::An ignore pattern hides a directory under a source tree (responsible line, then the probed path):"
  printf '%s\n' "$hidden"
  echo "Re-include the source trees with '!**/src/**/' at the end of that .gitignore ('!src/**/' in a module's own file), or anchor the pattern."
  exit 1
fi

echo "No ignore pattern hides a directory under any of the $(grep -c . <<< "$roots") source trees ($(grep -c . "$probes") probes)."
