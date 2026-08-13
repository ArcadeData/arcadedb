# Engine CLAUDE.md

Guidance for working under `engine/`. This file records things the code does not tell you, or actively misleads you about. It is not a tour of the module - use `Glob` and the package names for that.

## Concurrency: the conflict unit is the PAGE, and two commit-time merges take it back to the record

`PageManager.checkPageVersion()` compares the transaction's page version against `getMostRecentVersionOfPage()`. Any other committed transaction that touched that page raises `ConcurrentModificationException`, **whether or not the records overlap** - `LocalBucket.findAvailableSpace` deliberately packs unrelated records into the same page, and `EdgeLinkedList.add` makes two edges into one supernode touch the same chunk even though appends commute.

Two mechanisms undo that false verdict at commit time, both leader/embedded-only and both running while the file's commit lock is held (see `TransactionContext.commit1stPhase`):

- **`GRAPH_EDGE_APPEND_MERGE`** replays this transaction's in-chunk edge appends on the newer committed edge-list page.
- **`TX_PAGE_SLOT_MERGE`** (#5381, #5279, #5569) replays this transaction's per-slot record writes on the newer committed bucket page: inserts into free slots, updates that stayed **inside** the page - an overwrite of the same size or smaller, or a growth the page could host by shifting the records that follow (`LocalBucket.growRecordInPage`, shared with the replay so both grow a record identically) - and the delete of a plain in-place record (`rebaseRecordDeleteOnPage`), which only zeroes its own slot-table entry. `LocalBucket` also reserves the insert slot per in-flight transaction, so concurrent inserts into one page get different slots (hence different RIDs) instead of all being handed the same one.

Since #6129 the merge also covers the shapes a record takes **after it outgrows its page**, each tracked with its own slot *kind* (`TransactionContext.SLOT_KIND_*`) so the replay checks the committed marker and not only the bytes: the CONTENT record behind a placeholder pointer (in place or growing on its own page), the HEAD chunk of a multi-page record, and the spill that turns a plain record into that head chunk (its images differ in shape - plain record in, chunk out - and the replay re-establishes the room the spill used: the slot's own footprint, the free tail of the page when it is the last record, and otherwise the same in-page shift the write used, see the placeholder section below).

The head chunk needs one extra guarantee the pre-image check cannot give: it is the only part of a multi-page record that lives on the page being replayed, so a concurrent transaction that rewrote the record only PAST that chunk would be silently dropped. `LocalBucket.chunkChainTailFingerprint` closes that - a 64-bit FNV-1a of the chain past the head, taken when the record is taken for update (`TransactionContext.addUpdatedRecord`, the same moment the record's page is pinned) and again at commit before the chain is rewritten. Different fingerprints mean the page falls back to the plain retry it took before #6129.

**Which mechanism protects the tail depends on the isolation level, and only one of them is the fingerprint.** Under `READ_COMMITTED` (the default) `getPage` does not cache, so a transaction's continuation-chunk pages are loaded fresh under the commit lock and can never fail a version check - the fingerprint is the only thing standing between a tail-only concurrent change and a silently dropped write. Measured, by forcing `isChunkChainTailUnchanged` to true: exactly one test goes red, `Issue6129ChunkedSlotMergeTest.aTailOnlyChangeToTheSameChunkedRecordStillConflicts`, and it goes red by *committing*. Under `REPEATABLE_READ` the same caching that makes both fingerprint walks agree - they read the transaction's own cached pages - is what makes the chain pages stale in the modified set, so the ordinary page-version check refuses them instead. Two levels, two mechanisms, one guarantee; the second is pinned by `aTailOnlyChangeToTheSameChunkedRecordStillConflictsUnderRepeatableRead`, which passes with the fingerprint disabled.

**Know what that trades.** For a multi-page record, "two transactions writing the same record always conflict" used to hold absolutely, for the blunt reason that the head chunk's page was poisoned no matter what. It now holds up to a fingerprint collision: ~2^-64 per pair of concurrent updates to one such record whose head chunks are byte-identical. Deliberate collisions are not a threat model here - both colliding tails are content of the same record, so producing one needs write access to it, and whoever has that can overwrite it by committing last anyway. An installation that wants the absolute form back turns `arcadedb.txPageSlotMerge` off and gets unconditional poisoning, at the cost of the false conflicts the whole mechanism exists to remove.

What still raises a `ConcurrentModificationException`: two transactions writing the **same** record (by design - the byte-for-byte pre-image check catches it and the application must reload), a slot turning into (or being rebuilt from) a placeholder POINTER, the continuation chunks of a multi-page record, and any page its owner poisoned with one of those.

The delete is tracked with a `null` final image in `TransactionContext.SlotRebaseBuffer.finalBody`, which is why `deleteRecordInternal` decides where to poison only **after** reading the record's marker: every non-plain shape (placeholder pointer or content, chunk chain, corrupted slot, and the error fallback) poisons its own page. A slot the transaction both inserted and deleted is skipped at replay - it never existed on the committed page, so checking for a pre-image would invent a conflict.

**A record that outgrows its page keeps its slot, and that is what makes the shapes above replayable.** The head chunk's footprint on the page is fixed when the record spills and is never grown again (`updateMultiPageRecord` only rewrites inside it), and a placeholder content record is an ordinary record on its own page. The continuation chunks are the part that stays out: they are written through inline record-table writes that no slot image accounts for, so `writeMultiPageRecord`/`updateMultiPageRecord` poison every page they land on - including the head chunk's own page when the chain comes back to it.

**Only the pages a transaction PINNED can conflict, and a record update pins exactly one of them.** A record update is deferred: `save()` puts the record in `updatedRecords` and `addUpdatedRecord` pins its own page (`fetchPageInTransaction`) right then, but the write itself happens in `commit1stPhase`, after the file's commit lock is taken. Every other page it needs - a placeholder's content page, the continuation chunks of a chain - is therefore loaded *fresh under the lock*, at the newest committed version, so no page-version check can ever fire on it. That is why the merge does not have to fear false conflicts there - and why it cannot rely on MVCC to catch a *true* one either, which is what `chunkChainTailFingerprint` is for. It also means a concurrent update of the same PLACEHOLDER-backed record goes undetected today: both writers load the content page fresh and the second one wins silently, with the pointer page untouched by either.

`ConcurrentModificationException extends NeedRetryException`, so retry is the designed response. **Retry safety is asymmetric:** on retry an INSERT gets a fresh slot and RID from `findAvailableSpace`, so nothing is overwritten and retrying is safe. For UPDATE, blind retry can overwrite a concurrent write - re-read first.

**Retry does not always run.** The server auto-retry in `DatabaseAbstractHandler` only fires when the request is an atomic single-request command. A client-managed explicit transaction over `RemoteDatabase` spans several HTTP calls, so the commit is not retried and the raw exception reaches the client. If you are reproducing a CME report, establish which of the two shapes the reporter is using before theorizing.

RID is a physical position (`pageNumber * maxRecordsInPage + slot`), which is why the engine tracks pages rather than records in the first place.

## A record that outgrows its page becomes CHUNKS; the placeholder is what is left of the mechanism that came before

The placeholder - an 8-byte pointer in the record's slot and the content on another page - is the pre-chunk spill mechanism. The chunk chain (#332) took over every case it used to serve except one, and `LocalBucket` has exactly one site that still writes a placeholder pointer: the spill branch of `updateRecordInternal`, when the slot cannot reach `MINIMUM_SPACE_FOR_FIRST_CHUNK` (14 bytes - marker, chunk size, next-chunk pointer, one byte of content).

Since #6149 that branch first asks the page for the few bytes it is missing, through the same in-page shift a growth uses (`shiftFollowingRecordsRight`, shared by `growRecordInPage`, the spill, and the commit-time replay of both). Reaching the spill means the page could not host the record's FULL new size, which says nothing about whether it can spare the five-odd bytes between a 9-byte slot and a chunk header - and it usually can. **Only the missing bytes are taken**: a head chunk's footprint is fixed for the life of the record (`updateMultiPageRecord` only ever rewrites inside it), so a bigger bite would lock page space away from every other record for good.

What is left for the placeholder is a page with **no free tail at all**. No insert can produce one - `getAvailableSpaceInPage` keeps `SPARE_SPACE_FOR_GROWTH` in hand - but a spill can, because the head chunk of a *last* record takes the whole tail. That is how the tests that still need a placeholder build one (`sealFirstPage`, in `Issue6149PlaceholderPrefersChunksTest`, `Issue6129ChunkedSlotMergeTest` and `Issue5279ConcurrentUpdateTest`); filling a page with inserts no longer does it.

The readers stay: existing databases are full of placeholders, and roughly ten branches across scan, check, delete, update, compaction and space accounting resolve them. Only the writer stopped producing new ones - and an existing placeholder converts to a chunk chain by itself the first time its content record can no longer absorb an update, because that rebuild goes through the very same spill branch.

## LSM index reads must walk newest to oldest at every level

`LSMTreeIndexAbstract.lookupInPageAndAddInResultset` resolves deletions with a **forward-poisoning** `deletedRIDs` set: a tombstone adds its RID, and a live entry is skipped only if its RID is *already* in the set. A tombstone therefore suppresses only what is encountered **after** it in the walk.

So every enclosing loop of the read path must feed pages newest first, and they all do: `LSMTreeIndexMutable.searchInNonCompactedIndex` counts down from `totalPages - 1`, `LSMTreeIndexCompacted.searchInCompactedIndex` walks descending page numbers, values within a page are walked in reverse, and the trailing preceding-leaf read (holding the oldest chunks) correctly runs last.

**If you change any iteration order in the LSM read path, check it against this invariant.** Getting it wrong does not throw. It silently drops live records that were re-added after a tombstone, surfacing only as "the index returns fewer rows than a plain scan".

Reproducing any tombstone-ordering bug requires the remove and the re-add to be in **separate transactions**: within one transaction `TransactionIndexContext` collapses a REMOVE followed by an ADD on the same (key, RID) into a single ADD, so no tombstone is ever written.

## `count(*)` and `count()` take different paths

`count(*)` matches `SelectExecutionPlanner.isCountStar()` and routes to `CountFromTypeStep`, which sums `LocalBucket.count()`. That returns a **cached counter** (`cachedRecordCount`, persisted to `<db>/statistics.json`) plus the transaction delta, not a scan. `count()` and `count(field)` full-scan and are always accurate.

This is deliberate - it keeps `count(*)` O(1). But it means the two forms can disagree if the counter is ever wrong, and it makes `count(*)` the wrong tool when you are verifying correctness in a test. Use `count()` or `count(@rid)` when you need ground truth.

The counter is recomputed only when it reads -1, which happens on a fresh open with no statistics entry and after an unclean shutdown. That recompute path takes the bucket file lock so it cannot interleave with a commit's delta fold.

## Explicit locks must cover every index component file

`LOCK TYPE` / `LOCK BUCKET` collection in `LocalTransactionExplicitLock` iterates `IndexInternal.getFileIds()` (**plural**), matching what `TransactionIndexContext.addFilesToLock` adds to the must-be-locked set. Singular `getFileId()` returns only the mutable component.

This matters as soon as any index has been compacted: an LSM index with a compacted sub-index, or a vector index with its companion graph file, has two or more files. Locking only the mutable makes the commit-time coverage check throw a plain `TransactionException` - which, not being a `NeedRetryException`, `COMMIT RETRY` cannot absorb. Keep the two collections symmetric.

## Compaction threshold is cached at construction

`LSMTreeIndexMutable.minPagesToScheduleACompaction` is read from `INDEX_COMPACTION_MIN_PAGES_SCHEDULE` in the constructor. Changing that config at runtime does **not** affect an existing mutable; only a new one created by `splitIndex()` picks it up.

To disable auto-compaction reliably mid-test: set the config, `database.async().waitCompletion()`, then force `scheduleCompaction()` + `compact()` on each LSM index (this creates a fresh mutable that reads the new value), then `waitCompletion()` again.

## `countEntries()` is O(1) only on `HASH`

On an LSM index it is a full ascending cursor walk. It counts LIVE entries - the value that survives a `next()`, not the call - so tombstones are excluded whether or not a compaction has purged them (#5601). Getting that wrong is easy in this codebase: `LSMTreeIndexCursor.hasNext()` is optimistic, so `next()` answers `null` at the tail of a scan whose remaining keys are all dead, and any `while (hasNext()) { next(); ++n; }` over-counts.

On a dense `LSM_VECTOR`, `countEntries()` is a lock-free popcount over the location index's presence bits - one word per 128 vector ids since #5588 laid the locations out in primitive arrays, where it used to stream a `ConcurrentHashMap` filtering deleted entries. The bounded backend that made concurrent callers serialize on the `locations` monitor was removed with the eviction it existed for (#5559), so the count is unconditionally the live-vector count. On a sparse `LSM_SPARSE_VECTOR` it returns **postings**, not records: a 2-record fixture with 2 dimensions each reports 4. A full-text index counts one entry per analyzed token and a geospatial one per covering cell. `TypeIndex.countEntries()` sums over buckets and inherits whichever applies.

Never call it on a query path. If you need an "is there more?" signal, derive it from the result window or the candidate budget you already have.

## Lock timeouts default to 5 seconds

`EXPLICIT_LOCK_TIMEOUT` and `COMMIT_LOCK_TIMEOUT` both default to 5000 ms (`TX_RETRIES` defaults to 3). When N threads serialize through one type or bucket lock and each held transaction does a WAL commit, per-thread wait grows as `(N-1) * per_tx_time`. On a slow 2-vCPU CI runner that crosses 5 s and `acquireLock` throws, which is a common source of intermittent failures in contention tests.

Both are `SCOPE.DATABASE`, so a test can raise them per-database without polluting the suite. If you do, comment why - a raised timeout turns a future real deadlock into a long hang instead of a fast failure.

## WAL: `currentPageSize` is the content marker, not the physical size

`WALFile` writes `newPage.getContentSize()` into the field read back as `currentPageSize`, while the modified range is validated against `getPhysicalSize()` in `MutablePage.updateModifiedRange`. A valid WAL entry can therefore legitimately have `changesTo >= currentPageSize`, for example when an append extends past the current content marker.

Do not add WAL header validation that assumes `changesTo < currentPageSize`. It passes review and breaks recovery tests. Safe per-page checks are `deltaSize > 0` and `changesFrom >= 0`; memory bounds for delta allocation come from the outer segment-size file check.
