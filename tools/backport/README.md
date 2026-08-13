# tools/backport/list-missing.sh

Generates the cherry-pick worklist for one main→java17 release window.
Uses `git cherry` patch-id comparison, which over-reports "missing" for any
commit whose java17 port required JDK17 adaptation (the diff no longer
matches). Don't trust the list as exhaustive proof of what's missing --
trust `git cherry-pick`'s own conflict/empty-diff behavior when you actually
run it. Re-run this after finishing a window to confirm the list is empty
(module dependency-commit lines expected to remain, they're intentionally
excluded -- see Task 8 in docs/superpowers/plans/2026-08-13-backport-main-to-java17-26.8.1.md).
