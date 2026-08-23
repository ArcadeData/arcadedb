# PR #6606 review notes - cycle 1 (head 1ce4323)

Review: https://github.com/ArcadeData/arcadedb/pull/6606#issuecomment-5384910307 (claude[bot])

Overall verdict: approve modulo minor suggestions. Categorized below.

## Applied (actionable & clear)

- **Test coverage gap - precedence case.** The two original tests each set only one of
  `InsertChunk.database` / `InsertOptions.database`. Added a third test,
  `insertStreamMustPreferChunkDatabaseOverOptionsDatabaseWhenBothAreSet`, that sets both to
  different values (`InsertOptions.database` deliberately invalid, rejected by
  `validateDatabaseName`) and asserts the insert still succeeds, locking in that
  `InsertChunk.database` wins. This is the item the review itself called out as the "mainly"
  actionable one.

## Skipped (nitpick / optional, with rationale)

- **Credentials asymmetry** (`InsertChunk.credentials` still unread, only `InsertOptions.credentials`
  is used). The review itself notes this is already called out as a known follow-up in
  `docs/6597-grpc-insertstream-database.md`. Issue #6597 only reports the `database` field
  defect; expanding this PR's scope to `credentials` was not requested and risks changing
  auth-relevant behavior beyond what was reviewed for this fix. Left as a documented follow-up
  rather than filing a new GitHub issue unattended (out of scope for an unattended review
  cycle to create new tracked work).
- **Silent override logging.** Suggestion to add a `FINE`/`DEBUG` log line when
  `InsertChunk.database` and `InsertOptions.database` are both set and differ. Reasonable but
  purely cosmetic/observability, not correctness-affecting, and the reviewer's own summary
  ("approve modulo the minor suggestions above, mainly the precedence test case") does not treat
  it as blocking. Skipped to keep this fix minimal per the bug-workflow TDD approach; can be
  added in a follow-up if a real troubleshooting need arises.
