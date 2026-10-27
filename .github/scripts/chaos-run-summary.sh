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
# Renders the summary.json files of an HA chaos run (written by ChaosReport#summary) as Markdown
# for the workflow's run page.
#
# Every leaf of the report is printed, nested ones by their dotted path (ledger.acked, ...), rather
# than a hand-picked list of keys: the run page used to stop at seven fields, so the reformat
# counters, the late commits and the whole ledger never reached it (#10603). A key added to the
# report now appears on the page by existing in the report.
#
# Usage:
#   chaos-run-summary.sh <summary.json>...   (Markdown on stdout; missing files are skipped)

set -euo pipefail

# Values sit in a double-backtick code span so a single backtick in a message cannot break it, and
# line breaks are folded so a multi-line message stays on its own bullet.
# shellcheck disable=SC2016
program='
def cell: tostring | gsub("[\r\n]+"; " ") | "`` \(.) ``";
"### HA chaos: \(.result // "UNKNOWN")",
"",
( del(.result)
  | paths(((type != "object") and (type != "array")) or (length == 0)) as $p
  | "- \($p | map(tostring) | join(".")): \(getpath($p) | cell)" ),
""
'

found=0
for f in "$@"; do
    [ -f "$f" ] || continue
    found=1
    # The step runs under if: always(), so one unreadable report must not hide the ones after it.
    if ! out="$(jq -r "$program" "$f" 2>/dev/null)"; then
        out="### HA chaos: unreadable summary \`$f\`"
    fi
    printf '%s\n\n' "$out"
done
if [ "$found" = 0 ]; then
    echo "### HA chaos: no summary written (build or setup failed)"
fi
