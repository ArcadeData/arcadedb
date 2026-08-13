#!/usr/bin/env bash
# tools/backport/list-missing.sh <window-start-ref> <window-end-tag> [java17-ref]
#
# Prints, in chronological order, the commits reachable from <window-end-tag>
# but not from <window-start-ref> whose patch-id git cannot find an
# equivalent for anywhere in <java17-ref>'s history since the fork point.
# Excludes chore(deps)/build(deps) commits (reconciled separately, see Task 8).
# This is a WORKLIST, not ground truth: git cherry over-reports "missing" for
# commits that were ported with JDK17 adaptations (different patch-id). Treat
# every line as "try cherry-picking this"; an empty diff after conflict
# resolution means it's already present -- `git cherry-pick --skip` it.
set -euo pipefail

START="$1"
END="$2"
JAVA17="${3:-java17}"

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

git cherry "$JAVA17" "$END" "$START" > "$TMP/cherry.txt"
grep '^+' "$TMP/cherry.txt" | awk '{print $2}' | sort > "$TMP/missing.txt"

git rev-list --reverse "$START..$END" | while read -r h; do
  if grep -qx "$h" "$TMP/missing.txt"; then
    subj=$(git log -1 --format='%s' "$h")
    case "$subj" in
      chore\(*deps*\)*|build\(deps*\)*) continue ;;
    esac
    printf '%s\t%s\n' "$h" "$subj"
  fi
done
