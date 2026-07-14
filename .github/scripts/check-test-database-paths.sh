#!/usr/bin/env bash
#
# Fails when a test would put its database on a repo-relative "databases/" path.
#
# Such a database is written outside target/, so `mvn clean` cannot remove it. If a run dies
# before its teardown (kill -9, surefire fork timeout, OOM), the leftover directory makes every
# subsequent run of that class fail with "Database ... already exists", and only a manual
# `rm -rf` recovers. Tests must place their database under target/ - extend TestHelper, which
# does that plus pre-test cleanup, or pass an explicit target/-rooted path.
#
# Two complementary checks:
#
#   static  - grep test sources for repo-relative "databases/" string literals. Catches the path
#             whether it is inlined in the constructor or held in a String constant/local.
#             It matches the literal anywhere in the file, so a "databases/..." string inside a
#             comment or a log message trips it too. That is deliberate: a false alarm you can
#             silence by rewording beats a leak that wedges someone's test class next month.
#
#   runtime - after a test run, assert no module has grown a top-level databases/ directory.
#             This is the backstop: a server test derives its path from SERVER_ROOT_PATH rather
#             than a literal, so no grep can see it - only the resulting directory can.
#
# Usage:
#   check-test-database-paths.sh            # static only (cheap, safe to run pre-build)
#   check-test-database-paths.sh --runtime  # runtime only (run AFTER the test phase)
#   check-test-database-paths.sh --all      # both
#
set -euo pipefail

cd "$(dirname "$0")/../.."

mode="${1:---static}"
status=0

# A string literal that *starts* at a repo-relative databases/ dir: "databases/, "./databases/, "../databases/
#
# Deliberately does NOT match "/databases/, which is the suffix half of the correct server idiom
# `new DatabaseFactory(rootPath + "/databases/" + name)`. Whether that one lands outside target/
# depends on SERVER_ROOT_PATH, which no grep can resolve - the runtime check below covers it.
LITERAL='"(\.\.?/)?databases/'

run_static() {
  local violations
  violations=$(grep -rEl --include='*.java' "${LITERAL}" -- */src/test/java || true)

  if [[ -n "${violations}" ]]; then
    echo "ERROR: test sources reference a repo-relative 'databases/' path."
    echo "These databases live outside target/, so 'mvn clean' cannot remove them and a leftover"
    echo "directory from a crashed run permanently wedges the test class."
    echo
    echo "Fix: extend com.arcadedb.TestHelper, or root the path under target/databases/."
    echo
    echo "Offending files:"
    grep -rEn --include='*.java' "${LITERAL}" -- */src/test/java | sed 's/^/  /'
    status=1
  else
    echo "OK (static): no test source references a repo-relative 'databases/' path."
  fi
}

run_runtime() {
  local leaked
  # <module>/databases is a leak; anything under a target/ dir is the goal state, not a leak.
  leaked=$(find . -maxdepth 2 -type d -name databases -not -path './.git/*' -not -path '*/target/*' || true)

  if [[ -n "${leaked}" ]]; then
    echo "ERROR: the test run left a repo-relative 'databases/' directory behind."
    echo "The database was written outside target/, so 'mvn clean' cannot remove it."
    echo
    echo "A server test most likely started ArcadeDBServer without setting SERVER_ROOT_PATH, which"
    echo "defaults the database directory to ./databases. Set it to ./target, as the other server"
    echo "tests do:  GlobalConfiguration.SERVER_ROOT_PATH.setValue(\"./target\");"
    echo
    echo "Leaked directories:"
    while IFS= read -r dir; do
      echo "  - ${dir}"
    done <<< "${leaked}"
    status=1
  else
    echo "OK (runtime): the test run left no repo-relative 'databases/' directory behind."
  fi
}

case "${mode}" in
  --static)  run_static ;;
  --runtime) run_runtime ;;
  --all)     run_static; run_runtime ;;
  *) echo "usage: $0 [--static|--runtime|--all]" >&2; exit 2 ;;
esac

exit "${status}"
