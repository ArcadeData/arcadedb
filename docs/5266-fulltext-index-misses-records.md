# 5266 - FULL_TEXT index misses records

## Status

Issue #5266 as reported (fresh `CREATE INDEX` over 204k pre-existing records, no writes in between)
**could not be reproduced** on `main`. This change fixes a *different*, confirmed index-correctness
defect found while investigating, which produces the same user-visible symptom. It does not close
#5266.

## What was ruled out for #5266

A harness built the FULL_TEXT index over pre-existing records and compared, for every token, the index
result against a plain scan, and `countEntries()` against the expected posting count. No loss on `main`
in any of: 204k records / 917k postings / 158k-token vocabulary; vertex type with ~350-byte records;
`CREATE INDEX` inside an outer transaction (the server/HTTP path); page size 1024 and 65536;
`indexCompactionRAM` = 1MB (forcing multi-chunk, multi-series compaction); four successive compaction
rounds; database reopen; a token present in every record (posting list spanning many leaves).

The reporter is on 26.7.2. Several compacted-LSM read fixes landed after that tag (#5210, #5214, #4943),
and 26.7.2 demonstrably behaves differently: the same workload with a small index page size throws
`UnsupportedOperationException: Root index page overflow` there, and not on `main`. #5266 may already be
fixed; the reporter has been asked for their dataset or a reproducer, and to retest on `main`.

## Symptom (the defect fixed here)

A value removed from a key and then added back under that same key **permanently disappears** from the
index, while a plain scan still returns the record. Easiest to hit with a heavily duplicated full-text
token, because a common token's posting list is what overflows a compacted leaf page.

## Root cause

`LSMTreeIndexAbstract.lookupInPageAndAddInResultset` resolves deletions with a **forward-poisoning**
`deletedRIDs` set: a tombstone only suppresses RIDs encountered *after* it. The whole reader therefore
walks newest to oldest - mutable pages (`LSMTreeIndexMutable:358`), compacted series
(`LSMTreeIndexCompacted:530`), and values within a page (`LSMTreeIndexAbstract:663`).

The one exception was the per-key leaf-page loop at `LSMTreeIndexCompacted:580`. A key whose values
overflow one leaf is written oldest-chunk-first onto ascending pages, each with its own root entry.
That loop iterated the root entries in order, i.e. **oldest chunk first**, so a tombstone sitting in an
early chunk poisoned `deletedRIDs` and then suppressed the live re-add sitting in a later chunk.

The remove and the re-add must land in separate transactions to be reachable: within one transaction a
REMOVE followed by an ADD on the same (key, rid) collapses to a single ADD in the transaction overlay,
so no tombstone is written at all.

## Fix

- `LSMTreeIndexCompacted:580` - iterate the key's leaf pages newest-first, matching every other level of
  the reader. The preceding-leaf read (which holds the oldest chunks) already ran last and stays last.
- `LSMTreeIndexMutable.onAfterLoad()` - propagate `storeTermFrequency` to the compacted sub-index when it
  is re-attached. The flag is not persisted in the page header, so a sub-index materialised by the
  component factory defaults to `false`; a mismatch desynchronises the posting value stream (the
  tf/docLength varints are not consumed) and silently decodes the rest of the entry as garbage.

## Tests

`engine/src/test/java/com/arcadedb/index/CompactedTombstoneLeafOrderTest.java` - 3000 documents share the
token `data` (1024-byte pages, so its posting list provably spans many compacted leaves); a third of them
have the token removed in one transaction and added back in the next; after compaction the index must
return all 3000. Fails without the fix (1000 documents missing - exactly the ones touched), passes with it.

Regression run: 551 tests / 90 classes across the index, LSM, compaction and full-text suites - 0 failures.
