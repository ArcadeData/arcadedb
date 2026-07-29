# PR #5560 - Schema dictionary spans multiple pages

## Symptom

`com.arcadedb.engine.Dictionary` held every type name and property name in page 0 alone, so a database was
capped at whatever fitted there: `pageSize - 8 (page header) - 4 (legacy counter)` = 327,668 bytes,
measured at 48,396 short names. Past that, `CREATE PROPERTY` and inserting a document with a new field
name failed permanently with `DatabaseMetadataException: No space left in dictionary file`, and there was
no way to grow. Entries are never reclaimed, so a schemaless workload with dynamic field names walked
steadily toward the wall.

## Fix

Names roll over to a new page. Every page, page 0 included, carries the same 4-byte legacy counter, so a
dictionary written before this change is exactly a dictionary of one page and loads with no special case:
no migration, no rewrite, no format flag needed to read an existing database.

The invariant everything follows from: **an id is the ordinal of the name in page order**, and that id is
written inside records on disk. The layout is therefore strictly append-only across pages. Names go only to
the last page, and a page left behind is never revisited even when it still has room; filling a gap would
renumber every name after it and silently repoint every record that referenced them.

Three details carry the correctness:

- `addItemToPage` **reads** the tail page before deciding where to write. Enlisting it as modifiable and
  then finding it full would bump its version, rewrite it at commit for nothing, and false-conflict with
  concurrent transactions (the page is the conflict unit, see `engine/CLAUDE.md`).
- `reload()` walks `Math.max(1, pageCount.get())` pages and reads them through `PageManager`, never through
  `TransactionContext.getPage()`. Both halves speak durable state: `getTotalPages()` would count pages a
  rolling-back transaction is discarding, and `TransactionContext.getPage()` resolves `modifiedPages` first,
  so the rollback reload would rebuild the dictionary from exactly the content being thrown away. That
  second half was a defect predating this change, since `updateName` always rewrote page 0 inside the
  caller's transaction.
- `updateName` re-lays the dictionary out from page 0 in the same order, so no id moves, and empties the
  pages the new content no longer reaches. `reload()` walks every committed page, so a stale tail page
  would otherwise re-add its old names.

## Page size and format version

`DEF_PAGE_SIZE` drops from 327,680 to 65,536 for new dictionaries. That size existed only to make the
single available page hold as many names as possible; now that pages roll over, a new name dirties and
eventually flushes one page, so a smaller page is 5x less write amplification per name. Existing databases
keep the page size they were created with, read back from the file name.

One consequence: on a new database a single identifier caps at `pageSize - 12` = ~65Kb, against ~327Kb
before. Only identifiers ever enter the dictionary (the `create=true` callers: type names and property
names); a string value is only ever looked up with `create=false`, so user data is never subject to this.

`CURRENT_VERSION` goes to 1, with a guard refusing a dictionary written by a newer ArcadeDB rather than
misreading it as this layout.

### Why the version is not bumped when an existing database first rolls over

The version is stamped at creation, so a `v0` database that later grows past page 0 keeps saying `v0` on
disk. Bumping it on first rollover was considered and rejected:

- The version is part of the **file name** (`dictionary.0.65536.v1.dict`). Changing it at runtime means
  renaming a live component file. `PaginatedComponent.rename` can only do that behind a full flush barrier
  (`waitAllPagesOfDatabaseAreFlushed`), and doing it from inside an append transaction would race
  replication, which ships pages by file id, and leave follower file names diverged from the leader's.
- It would protect nothing. `ComponentFactory` passes the parsed version straight through without
  validating it, so a build old enough to be at risk opens a `v1` file just as happily as a `v0` one. The
  guard added here only helps a *newer* format meeting *this* reader.

So the version records what wrote the file, and the operational signal that a database has left single-page
territory is the INFO line logged when page 1 is created.

### How to tell whether a database has rolled over

The log line is emitted once, when it happens, so it is no help for a database you are looking at after the
fact. The durable answer is the file size: the dictionary is one page per `pageSize` bytes, and `pageSize`
is in the file name.

```
$ ls -l <database>/dictionary.*.dict
-rw-r--r--  1 arcadedb  arcadedb  131072  dictionary.0.65536.v1.dict
```

131072 / 65536 = 2 pages, so this database has rolled over and cannot be read by a build without multi-page
support. A file of exactly `pageSize` bytes (or less) is still single-page and remains downgradable. This is
worth checking before rolling a cluster back to an older build.

## Upgrade notes

**Single node.** Nothing to do. An existing database keeps its page size and its `v0` file name, and gains
rollover on the next write that needs it.

**Downgrade.** Once a database has grown past page 0 it can no longer be opened by an older ArcadeDB, which
reads page 0 only and reports `Dictionary item with id N is not valid`. Loud, not silent. Such a database
could not have existed before this change, since the write would have been refused. A new database that has
never rolled over stays readable by an older build, so downgradability is lost exactly when the database
starts depending on the feature, not before.

**Cluster: upgrade followers before, or together with, the leader.** Dictionary pages replicate as raw
pages through `TransactionManager.applyChanges`. A new-version leader that rolls the dictionary over ships
page 1 and beyond to its followers; a follower still running a build without multi-page support writes the
page but reloads only page 0, so its in-RAM dictionary is missing those names and every record referencing
them fails with `Dictionary item with id N is not valid`. Upgrading followers first avoids the window
entirely.

## Tests

`engine/src/test/java/com/arcadedb/engine/DictionaryMultiPageTest.java`

- `namesRollOverToNewPagesInsteadOfBeingRefused` - past one page it keeps working, ids resolve both ways
- `idsSurviveAReopen` - the mapping is identical after a reopen
- `pageZeroKeepsTheLegacySinglePageLayout` - page 0 still reads with the pre-multi-page algorithm, which is
  the backward-compatibility guarantee
- `aNameTooBigForAnEmptyPageIsRejectedAndLeavesTheDictionaryUsable`
- `documentsWithPropertyNamesSpanningPagesReadBack` - the serializer round trip across pages
- `updateNameRewritesEveryPage` / `updateNameGrowingBeyondTheExistingPagesAddsOne` - shrink and grow
- `aRolledBackTransactionThatGrewTheDictionaryLeavesItIntact` - the case that exposed the dirty-page reload
- `aDictionaryFromANewerFormatIsRefusedInsteadOfMisread` - the version guard through the real load path
- `aReplicatedNewDictionaryPageIsVisibleAfterApplyChanges` - the follower apply path

`engine/src/test/java/com/arcadedb/schema/DictionaryLimitsTest.java` - filling well past one page keeps
working, entries are never reclaimed, both directions survive a reload.
