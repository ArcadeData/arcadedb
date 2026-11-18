/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Binary;
import com.arcadedb.engine.WALFile;
import java.nio.ByteBuffer;

/**
 * Reads the WAL transaction a committed Raft log entry carries: its id without decoding the pages, or the whole
 * transaction, and the error that names the entry when the payload cannot be decoded.
 * <p>
 * Split out of {@link ArcadeStateMachine}, which applies the transaction it reads.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class CommittedEntryDecoding {
  private CommittedEntryDecoding() {
  }

  /**
   * Reads a replicated WAL transaction's id without deserializing the pages behind it: the id is the
   * first field {@link #deserializeWalTransaction(byte[])} writes, so it is the leading 8 bytes of the
   * payload. It is the key the committing thread and the apply thread meet on (see {@link LocalCommit}),
   * read without materializing the pages behind it.
   */
  static long peekWalTransactionId(final byte[] walData) {
    if (walData == null || walData.length < Long.BYTES)
      throw new ReplicationException("Corrupted WAL transaction entry: truncated before the transaction id");
    return ByteBuffer.wrap(walData, 0, Long.BYTES).getLong();
  }

  /**
   * The WAL transaction id of a COMMITTED transaction entry, read the way the apply path has to read it.
   * <p>
   * {@link #peekWalTransactionId(byte[])} reports a payload too short to hold the id as a {@link ReplicationException}.
   * Its type is left alone here because its other call sites read the id for a log line rather than to decide
   * anything ({@code RaftReplicatedDatabase.localWalTxId} catches it and carries on with a sentinel). On the apply
   * path it was the wrong answer: {@code applyWithRetry} rethrows a
   * {@code ReplicationException} unchanged (it is the resync signal applyTxEntry raises on a WAL gap), so a committed
   * entry whose payload is truncated bypassed {@link EntryApplyRetry#handleUnexpectedApplyError} entirely - no quarantine, no targeted
   * snapshot resync, and the failed future Ratis swallows while advancing its own applied index. The corrupt entry was
   * therefore skipped on this node and nothing said so.
   * <p>
   * A truncated payload IS what {@link RaftLogEntryDecodeException} exists for (issue #7138): a committed entry of a
   * KNOWN type this version cannot read. Raised as one, it reaches {@code applyWithRetry}'s {@code RuntimeException}
   * branch, quarantines this one database and lets the leader resend it as a snapshot, exactly as a decode failure of
   * the envelope itself already does (issue #7495).
   */
  static long walTransactionIdOfCommittedEntry(final RaftLogEntryCodec.DecodedEntry decoded, final long entryIndex) {
    try {
      return peekWalTransactionId(decoded.walData());
    } catch (final RuntimeException e) {
      throw decodeFailure(decoded, entryIndex, "read the WAL transaction id of", e);
    }
  }

  /**
   * The WAL transaction a committed entry carries, decoded the way the apply path has to decode it: the same
   * reclassification {@link #walTransactionIdOfCommittedEntry} applies, for the same reason. {@code
   * deserializeWalTransaction} rejects a misaligned page count or delta range with a {@link ReplicationException}
   * (issue #4420), and on this path that exception type means "resync already in progress" to
   * {@code applyWithRetry}, which rethrows it without quarantining anything.
   * <p>
   * The catch is on {@code RuntimeException} rather than on {@code ReplicationException} alone because those
   * explicit checks are not the only way the decode can fail: a payload long enough to hold the transaction id
   * (8 bytes) but shorter than the header the decoder reads (24) runs out of buffer first and raises a
   * {@code BufferUnderflowException}. Both shapes are one thing - a committed entry this node cannot read - and
   * both now say so, rather than the diagnosis depending on how far into the payload the corruption happened to
   * start. Nothing but the decode of a {@code byte[]} runs inside the try, so the wider catch cannot capture an
   * unrelated failure.
   */
  static WALFile.WALTransaction walTransactionOfCommittedEntry(final RaftLogEntryCodec.DecodedEntry decoded,
      final long entryIndex) {
    return walTransactionOfCommittedEntry(decoded, decoded.walData(), entryIndex, RaftLogEntryType.TX_ENTRY, null);
  }

  /**
   * The same decode for a WAL payload a committed entry of any type carries, over an EXPLICIT {@code walData}
   * (issue #7695).
   * <p>
   * A {@code SCHEMA_ENTRY} can carry a whole batch of buffered WAL entries - the {@code recordFileChanges()} path
   * through {@code RaftReplicatedDatabase}'s schema WAL buffer - and {@code applySchemaEntry} decoded them with a
   * bare {@link #deserializeWalTransaction} call, which is the very bypass the one-payload form above exists to
   * close: a {@link ReplicationException} from a misaligned page count or delta range, or a
   * {@code BufferUnderflowException} from a payload shorter than the 24-byte header, left {@code applyWithRetry}
   * without ever reaching {@link EntryApplyRetry#handleUnexpectedApplyError}, so the database was not quarantined, no targeted
   * snapshot resync was triggered, and the corrupt entry was skipped on this node with nothing saying so.
   *
   * @param which names the failing payload within a multi-payload entry, so the log line points at one buffered
   *              WAL entry of the batch rather than at "the entry". Null for an entry that carries exactly one
   */
  static WALFile.WALTransaction walTransactionOfCommittedEntry(final RaftLogEntryCodec.DecodedEntry decoded,
      final byte[] walData, final long entryIndex, final RaftLogEntryType type, final String which) {
    try {
      return deserializeWalTransaction(walData);
    } catch (final RuntimeException e) {
      throw decodeFailure(decoded, entryIndex, "decode the WAL payload of", type, which, e);
    }
  }

  private static RaftLogEntryDecodeException decodeFailure(final RaftLogEntryCodec.DecodedEntry decoded,
      final long entryIndex, final String what, final RuntimeException cause) {
    return decodeFailure(decoded, entryIndex, what, RaftLogEntryType.TX_ENTRY, null, cause);
  }

  /**
   * The decode failure of a committed entry, typed as the entry that carried it (issue #7695). The type is what
   * routes the failure: {@link RaftLogEntryDecodeException} carries it to the per-database quarantine of issue
   * #7138 together with the database name, and a reader of the log needs to know WHICH kind of entry could not be
   * read - a transaction, or a DDL statement's buffered WAL - because the two say different things about what
   * the resync has to replace.
   */
  private static RaftLogEntryDecodeException decodeFailure(final RaftLogEntryCodec.DecodedEntry decoded,
      final long entryIndex, final String what, final RaftLogEntryType type, final String which,
      final RuntimeException cause) {
    return new RaftLogEntryDecodeException(
        "Cannot " + what + " the committed " + entryDescription(type) + " entry for database '"
            + decoded.databaseName() + "' at index " + entryIndex + (which == null ? "" : " (" + which + ")") + ": "
            + cause.getMessage(), type, decoded.databaseName(), cause);
  }

  /** How a decode failure names the entry it could not read. */
  private static String entryDescription(final RaftLogEntryType type) {
    return type == RaftLogEntryType.SCHEMA_ENTRY ? "schema" : "transaction";
  }

  /**
   * Returns true if this node was restarted and the current entry might not have been applied
   * to database files before the crash/shutdown.
   * <p>
   * After a crash/restart, Ratis replays committed log entries through the state machine.
   * If this node becomes the new leader before replay completes, the leader-skip optimization
  /**
   * Deserializes a WAL transaction from raw bytes using the WALFile binary format.
   * <p>
   * Format: txId (long), timestamp (long), segmentCount (int), segmentSize (int),
   * then for each page segment: fileId (int), pageNumber (int), changesFrom (int),
   * changesTo (int), currentPageVersion (int), currentPageSize (int),
   * delta bytes (changesTo - changesFrom + 1).
   * <p>
   * One page contributes one segment per disjoint modified interval (issue #5470), so the same page can appear
   * several times, consecutively and at the same target version; {@code TransactionManager.applyChanges} folds them
   * back into a single page image.
   */
  static WALFile.WALTransaction deserializeWalTransaction(final byte[] data) {
    final ByteBuffer buf = ByteBuffer.wrap(data);
    final WALFile.WALTransaction tx = new WALFile.WALTransaction();

    tx.txId = buf.getLong();
    tx.timestamp = buf.getLong();
    tx.forceApply = tx.txId < 0; // negative txId signals compaction page replication
    final int pageCount = buf.getInt();
    buf.getInt(); // segmentSize - not needed for deserialization

    // Reject a corrupted/misaligned entry instead of blowing up with a cryptic NegativeArraySizeException (issue #4420):
    // every WAL page occupies at least its 24-byte fixed header, so a page count exceeding the remaining bytes is corruption.
    if (pageCount < 0 || (long) pageCount * 6 * Integer.BYTES > buf.remaining())
      throw new ReplicationException("Corrupted WAL transaction entry: invalid page count " + pageCount);

    tx.pages = new WALFile.WALPage[pageCount];

    for (int i = 0; i < pageCount; i++) {
      final WALFile.WALPage page = new WALFile.WALPage();
      page.fileId = buf.getInt();
      page.pageNumber = buf.getInt();
      page.changesFrom = buf.getInt();
      page.changesTo = buf.getInt();
      page.currentPageVersion = buf.getInt();
      page.currentPageSize = buf.getInt();

      final int deltaSize = page.changesTo - page.changesFrom + 1;
      if (deltaSize <= 0 || page.changesFrom < 0 || deltaSize > buf.remaining())
        throw new ReplicationException("Corrupted WAL transaction entry: invalid delta range [" + page.changesFrom + ","
            + page.changesTo + "] for page " + page.fileId + ":" + page.pageNumber);
      final byte[] content = new byte[deltaSize];
      buf.get(content);
      page.currentContent = new Binary(content);

      tx.pages[i] = page;
    }

    return tx;
  }
}
