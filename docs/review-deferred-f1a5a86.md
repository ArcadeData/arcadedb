# Review notes for HEAD f1a5a86 (PR #4459, cycle 1)

## Skipped with justification

### gemini-code-assist - 3 inline comments: add try-finally + DETACH DELETE cleanup

- `matchWithParameterPropertyFilter` (line 1726)
- `vlpMatchWithParameters` (line 1794)
- `vlpMatchWithParametersInTransaction` (line 1824)

**Rationale for skipping:** `BaseGraphServerTest.beginTest()` (`@BeforeEach`) calls `deleteDatabaseFolders()` + `prepareDatabase()`, and `endTest()` (`@AfterEach`) drops the database (`dropDatabasesAtTheEnd()` returns true). The database is recreated fresh for every test method, so there is no cross-test pollution and manual cleanup is unnecessary. All pre-existing tests in `BoltProtocolIT` (e.g. `createAndMatchVertex`, `multipleStatements`, `transaction`) follow the same no-cleanup convention; adding `try-finally` cleanup would break consistency with the file. The claude bot review independently confirmed this: "the tests are properly isolated without needing manual cleanup."

## Applied (claude bot, advisory)

1. `matchByIdParameter`: added `assertThat(allNodes.hasNext()).isFalse()` after consuming the first record to make the single-result invariant explicit.
2. Both VLP tests: replaced the bare `.next()` on the parent-id query with a stored `Result` + `assertThat(parentResult.hasNext()).isTrue()` for clearer failure diagnostics.
3. Both VLP tests: added a comment documenting the dependency on a clean per-test database for the unscoped MATCH.
4. Both VLP tests: added a comment clarifying that `*0..` is intentional (mirrors the exact query from issue #4452) and that the zero-hop case is filtered out by the target label.
