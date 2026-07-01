# Issue #4825 — `RaftLogEntryCodec.decodeSchemaEntry` swallows truncated/corrupt WAL section as "old version"

## Problem

`decodeSchemaEntry` wraps the entire embedded-WAL section in `catch (IOException ignored) {}`
("treat as empty"). This means a SCHEMA_ENTRY whose WAL section is present but truncated or
misaligned silently decodes with `walEntries = emptyList()` (or a partial list), with no error
surfaced. The follower then applies a schema change with missing index/WAL pages, producing the
"Cannot find indexes ..." class of failures.

The original intent of the catch was backward compatibility: log entries produced by nodes that
predate the embedded-WAL section end the stream cleanly right after the `filesToRemove` map, so the
first read of the section (the WAL count) hits EOF. That single case is legitimate. But the broad
`IOException` catch also swallows EOF/IO failures that occur *after* the WAL count has been read,
i.e. real truncation/corruption of a present section.

The sibling TimeSeries sealed-blob section (issue #4382) already catches only `EOFException`.

## Root cause

`RaftLogEntryCodec.decodeSchemaEntry` (ha-raft module):
- The whole WAL read loop is inside one `try { ... } catch (final IOException ignored) {}` block.
  Any failure mid-section is treated identically to an absent section.

## Fix

Scope the backward-compatibility tolerance to the **absence** of the section only:
- Read the WAL count in its own `try`; an `EOFException` there (and only there) means the section is
  absent (older entry) and is decoded as empty.
- Once the WAL count has been read, the section is present: the per-entry reads run outside any
  swallowing catch, so a truncated/misaligned section propagates as an `IOException`, which
  `decode()` wraps into an `IllegalStateException` ("Failed to decode Raft log entry").

The forward/backward-compatible trailing sealed-blob section is unchanged (still `EOFException`-only).
The `decode()` trailing-byte check remains disabled for SCHEMA_ENTRY because that type legitimately
carries optional self-describing trailing sections that newer nodes may append.

## Tests (TDD)

Added to `RaftLogEntryCodecTest`:
- `decodeSchemaEntryWithTruncatedWalPayloadThrows` — encodes a SCHEMA_ENTRY with an embedded WAL
  entry, truncates the bytes a few bytes into the compressed WAL payload (count + length prefixes
  fully read), asserts `decode()` throws (fails before the fix: silently returns empty WAL entries).
- `decodeSchemaEntryTruncatedInWalLengthPrefixThrows` — truncates 2 bytes into the first WAL entry's
  length prefix to lock the boundary semantics (count read, then truncation propagates).
- `decodeLegacySchemaEntryWithoutWalSectionDecodesEmpty` — hand-crafts a legacy SCHEMA_ENTRY that
  ends right after `filesToRemove` (no WAL section); asserts it still decodes with empty WAL/blob
  lists (backward compatibility preserved).

Truncation offsets are computed explicitly from the wire format (not as a fraction of the entry
size) so they stay anchored if the encoder or compression ratio changes.

## Verification

- `mvn -pl ha-raft test -Dtest=RaftLogEntryCodecTest` — 33/33 pass.
- `mvn -pl ha-raft test -Dtest='ArcadeStateMachine*Test,RaftLogEntry*Test'` — 74/74 pass.

## PR

https://github.com/ArcadeData/arcadedb/pull/4845

## Review cycles

- Cycle 1 (`a8f149f`): initial fix (WAL section only, boundary-only `try`/`EOFException`).
  - gemini-code-assist (high): use `dis.available() > 0` to detect the optional WAL section instead
    of a `try`/catch on the count, matching `decodeInstallDatabaseEntry`; also fixes a mid-`walCount`
    truncation hole (1-3 bytes left) that the catch would have swallowed. **Applied.**
  - claude (main): the sibling TimeSeries sealed-blob section has the identical latent bug (reads
    `blobCount` inside the same swallowing `catch (EOFException)`). **Applied** the analogous
    `available() > 0` guard there too. Minor: made the truncation-test offsets deterministic and
    added the length-prefix boundary test. **Applied.**
  - Result: both WAL and sealed-blob sections now use the `available() > 0` presence check; the
    `EOFException` import was removed (no longer referenced).
- Cycle 2 (`caa0457`): claude approved (LGTM, only cosmetic/optional items); gemini-code-assist did
  not re-review within the 15-minute per-iteration window (it reviewed cycle 1, and all of its
  feedback was already applied). No code changes were required in this cycle.
  - Verified claude's non-blocking consideration about a "poison" (now-throwing) committed entry:
    `RaftLogEntryCodec.decode()` is called in `ArcadeStateMachine.applyTransaction` *before*
    `applyWithRetry`, so a decode `IllegalStateException` is not retried. It falls into the final
    `catch (Throwable)`, which logs SEVERE, triggers the critical halt + server stop (snapshot
    recovery on restart), and leaves `lastAppliedIndex` untouched. No infinite apply-retry loop is
    introduced; this matches the existing fail-loud handling for unknown entry types (#4798).
  - Addressed the stale test name in the PR description (the committed tests are
    `decodeSchemaEntryWithTruncatedWalPayloadThrows` and `decodeSchemaEntryTruncatedInWalLengthPrefixThrows`).

## Known limitation (accepted trade-off)

The fix narrows but cannot fully eliminate silent truncation: a stream cut *exactly* at a section
boundary (e.g. the whole sealed-blob section lost after a fully-intact WAL section) is
indistinguishable from "an older node never wrote that section", so it still decodes as absent. This
is inherent to the length-prefix-free, self-describing trailing-section format and is the accepted
cost of backward compatibility. A per-entry total-length prefix or whole-entry CRC would be required
to close it if stronger integrity is ever wanted.

## Final state

timeout (cycle 2): claude approved; gemini-code-assist did not re-review cycle 2 within 15 minutes.
All actionable review feedback (gemini cycle 1 + claude main point) was applied in cycle 1; the only
remaining items were cosmetic (PR-body test name, handled) and an optional documentation note (added
above). No outstanding actionable code feedback.
