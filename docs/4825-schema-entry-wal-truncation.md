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
- `decodeSchemaEntryWithTruncatedWalSectionThrows` — encodes a SCHEMA_ENTRY with an embedded WAL
  entry, truncates the bytes mid-WAL-section, asserts `decode()` throws (fails before the fix:
  silently returns empty WAL entries).
- `decodeLegacySchemaEntryWithoutWalSectionDecodesEmpty` — hand-crafts a legacy SCHEMA_ENTRY that
  ends right after `filesToRemove` (no WAL section); asserts it still decodes with empty WAL/blob
  lists (backward compatibility preserved).

## Verification

- `mvn -q -pl ha-raft -am test -Dtest=RaftLogEntryCodecTest`
