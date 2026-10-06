# tools/backport/list-missing.sh

Generates the cherry-pick worklist for one main→java17 release window.
Uses `git cherry` patch-id comparison, which over-reports "missing" for any
commit whose java17 port required JDK17 adaptation (the diff no longer
matches). Don't trust the list as exhaustive proof of what's missing --
trust `git cherry-pick`'s own conflict/empty-diff behavior when you actually
run it. Re-run this after finishing a window to confirm the list is empty
(module dependency-commit lines expected to remain, they're intentionally
excluded -- see Task 8 in docs/superpowers/plans/2026-08-13-backport-main-to-java17-26.8.1.md).

## Window 26.8.1 -> 26.9.1 (2026-09)

Unlike the 25.5.1-26.5.1 windows, this one is a near-total gap: `git cherry`
reported 1223 missing and only 7 already present, and the 7 are the CI-fix
cherry-picks that were applied to java17 ahead of the backport. Treat the
worklist for this window as close to ground truth. See
docs/superpowers/plans/2026-09-04-backport-main-to-java17-26.9.1.md.

## Window 26.9.1 -> 26.10.1 (2026-10)

3100 commits (481 merges), worklist 2488 after dropping `chore(deps)`/`build(deps)`.
Driven take-theirs (`git cherry-pick -x`, conflicts resolved to the incoming side,
`.github/workflows/mvn-release.yml` and the root pom's deploy section kept untouched),
then ONE reconcile commit that aligns every non-build file with the 3-way merge
`git merge-tree --write-tree --merge-base=26.9.1 <java17 head> 26.10.1`. That target
is upstream's end state plus the java17 adaptations, and it repairs the files that
out-of-order sibling-branch cherry-picks had silently reverted (59 in this window).
Files that conflict in the 3-way merge take the tag's content and are re-adapted
compile-first: under `--release 17` every Java 18+ construct is a javac error, except
two runtime differences found by the tests (Float/Double.toString shortest decimal,
JDK 19) and a javac 17 crash on a diamond anonymous class inside lambda inference.
