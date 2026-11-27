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
# Validates the OpenVEX document the release gate trusts. A not_affected statement without a justification and an
# impact statement would silence a critical CVE with no recorded reason, so it fails here, before it can be merged.
# Each statement needs a vulnerability name, a non-empty products array of objects with an @id, and a status among
# not_affected, affected and under_investigation. `fixed` is refused: grype suppresses a match on a `fixed` statement
# too, which would exempt a fixable critical with no justification (release-scan.sh also passes grype only the
# not_affected statements). An empty statements array is valid.
#
# Usage: check-vex.sh <openvex.json> [<gate-vex-out.json>]
#   With a second argument, a valid document is also written there with only its not_affected statements: the copy
#   release-scan.sh hands grype, so no other status can ever suppress a match.

set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
    echo "Usage: check-vex.sh <openvex.json> [<gate-vex-out.json>]" >&2
    exit 2
fi

if ! jq -e '
    type == "object"
    and .["@context"] == "https://openvex.dev/ns/v0.2.0"
    and ((.author // "") | type == "string" and length > 0)
    and ((.timestamp // "") | type == "string" and length > 0)
    and (.version | type == "number")
    and (.statements | type == "array")
' "$1" >/dev/null 2>&1; then
    echo "$1: not an OpenVEX v0.2.0 document: needs @context https://openvex.dev/ns/v0.2.0, author, timestamp, version and a statements array" >&2
    exit 1
fi

bad="$(jq -r '
    def nonempty_string: type == "string" and length > 0;
    .statements | to_entries[]
    | .key as $k | .value as $s
    | if ($s | type) != "object" then "statement \($k): not an object"
      else
        (($s.vulnerability.name? // null) as $name
         | if ($name | nonempty_string | not) then "statement \($k): needs vulnerability.name" else empty end),
        (if (($s.products | type) != "array") or (($s.products | length) == 0)
            or any($s.products[]; (type != "object") or ((.["@id"] // null) | nonempty_string | not))
         then "statement \($k) (\($s.vulnerability.name? // "unknown")): needs a non-empty products array, each with an @id"
         else empty end),
        (if ($s.status | IN("not_affected", "affected", "under_investigation") | not)
         then "statement \($k) (\($s.vulnerability.name? // "unknown")): status \($s.status | tojson) is not one of not_affected, affected, under_investigation"
         else empty end),
        (if ((($s.justification // "") | type) != "string") or ((($s.impact_statement // "") | type) != "string")
         then "statement \($k) (\($s.vulnerability.name? // "unknown")): justification and impact_statement must be strings"
         else empty end)
      end
' "$1")"

if [[ -z $bad ]]; then
    bad="$(jq -r '
    .statements | to_entries[]
    | select(.value.status == "not_affected")
    | select(((.value.justification // "") == "") or ((.value.impact_statement // "") == ""))
    | "statement \(.key) (\(.value.vulnerability.name // "unknown")): not_affected needs both justification and impact_statement"
' "$1")"
fi

if [[ -n $bad ]]; then
    echo "$bad" >&2
    exit 1
fi
if [[ $# -eq 2 ]]; then
    if ! jq '.statements |= map(select(.status == "not_affected"))' "$1" >"$2"; then
        echo "$2: cannot write the not_affected-only copy" >&2
        exit 1
    fi
fi
echo "$1: OK"
