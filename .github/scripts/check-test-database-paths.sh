#!/usr/bin/env bash
#
# Fails when a test constructs a DatabaseFactory on a repo-relative "databases/" path.
#
# Such a database is written outside target/, so `mvn clean` cannot remove it. If a run dies
# before its teardown (kill -9, surefire fork timeout, OOM), the leftover directory makes every
# subsequent run of that class fail with "Database ... already exists", and only a manual
# `rm -rf` recovers. Tests must place their database under target/ - extend TestHelper, which
# does that plus pre-test cleanup, or pass an explicit target/-rooted path.
#
set -euo pipefail

cd "$(dirname "$0")/../.."

# Matches: new DatabaseFactory("databases/...  and  new DatabaseFactory("./databases/...
PATTERN='new[[:space:]]+DatabaseFactory\([[:space:]]*"\.?/?databases/'

violations=$(grep -rEln --include='*.java' "${PATTERN}" -- */src/test/java || true)

if [ -n "${violations}" ]; then
  echo "ERROR: test sources construct a DatabaseFactory on a repo-relative 'databases/' path."
  echo "These databases live outside target/, so 'mvn clean' cannot remove them and a leftover"
  echo "directory from a crashed run permanently wedges the test class."
  echo
  echo "Fix: extend com.arcadedb.TestHelper, or root the path under target/databases/."
  echo
  echo "Offending files:"
  echo "${violations}" | sed 's/^/  - /'
  exit 1
fi

echo "OK: no test constructs a DatabaseFactory on a repo-relative 'databases/' path."
