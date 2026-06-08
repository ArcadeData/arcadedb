# Review notes for #4514 PR (cycle 2, head 14ac5c1)

## Applied
- Claude #3: replaced `Integer.MIN_VALUE` sentinel with `-1` (node IDs are non-negative).
- Claude #4: eliminated the `compactOffsets` allocation and `System.arraycopy` by capturing
  `offsets[u]` into local read bounds and overwriting `offsets[u]` in the same pass. No extra
  allocation; same O(merged-edges) complexity.
- Claude #2 (partial): trimmed the verbose comment block and removed the inline `TODO`.

## Declined (with rationale)
- Claude #1: "remove the `docs/4514-*.md` analysis file". Declined. A per-issue tracking doc under
  `docs/` is an established repository convention (see `docs/4334-*.md`, `docs/3971-*.md`, and many
  others) and is an explicit deliverable of the resolve-issue workflow (its Phase 6 updates and
  commits this file with PR + review history). Removing it would diverge from precedent rather than
  follow it. Kept.
- Claude #2 (TODO-as-GitHub-issue): not filing a new GitHub issue unprompted. The sequential-vs-
  parallel de-dup observation is minor and the inline TODO was removed, so nothing is lost.
- Gemini cycle 1: no actionable items (approving summary only).
