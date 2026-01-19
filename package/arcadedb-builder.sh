#!/usr/bin/env bash
# Requires bash 4.0+ for associative arrays
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

set -euo pipefail

VERSION="1.0.0"
SCRIPT_NAME="$(basename "$0")"

# URLs
MAVEN_CENTRAL_BASE="https://repo1.maven.org/maven2/com/arcadedb"
GITHUB_RELEASES_BASE="https://github.com/arcadedata/arcadedb/releases/download"

# Module metadata
SHADED_MODULES="gremlin redisw mongodbw postgresw grpcw metrics"
REGULAR_MODULES="console studio graphql"

# Module descriptions for interactive menu
declare -A MODULE_DESCRIPTIONS=(
    [console]="Interactive database console"
    [gremlin]="Apache Tinkerpop Gremlin support"
    [studio]="Web-based administration interface"
    [redisw]="Redis wire protocol compatibility"
    [mongodbw]="MongoDB wire protocol compatibility"
    [postgresw]="PostgreSQL wire protocol compatibility"
    [grpcw]="gRPC wire protocol support"
    [graphql]="GraphQL API support"
    [metrics]="Prometheus metrics integration"
)

# Default values
ARCADEDB_VERSION=""
SELECTED_MODULES=""
OUTPUT_NAME=""
OUTPUT_DIR="$(pwd)"
DOCKER_TAG=""
SKIP_DOCKER=false
DOCKERFILE_ONLY=false
KEEP_TEMP=false
DRY_RUN=false
VERBOSE=false
QUIET=false

# Temp directory
TEMP_DIR=""

echo "ArcadeDB Modular Distribution Builder v${VERSION}"
echo "================================================"
echo ""
