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
#
# Usage: check-vex.sh <openvex.json>

set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "Usage: check-vex.sh <openvex.json>" >&2
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
    .statements | to_entries[]
    | select(.value.status == "not_affected")
    | select(((.value.justification // "") == "") or ((.value.impact_statement // "") == ""))
    | "statement \(.key) (\(.value.vulnerability.name // "unknown")): not_affected needs both justification and impact_statement"
' "$1")"

if [[ -n $bad ]]; then
    echo "$bad" >&2
    exit 1
fi
echo "$1: OK"
