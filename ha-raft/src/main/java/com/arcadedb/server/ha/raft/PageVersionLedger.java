/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import com.arcadedb.exception.ConcurrentModificationException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The page versions the Raft log has assigned but this node has not applied yet, per database (issue #6965).
 * <p>
 * Every replicated transaction is validated on the node that originates it against that node's copy of the pages, and
 * ships each page's next version. Two nodes validating against the same base version therefore ship two entries that
 * both claim the same next version, and Raft happily orders both: the second one used to be applied through the
 * equal-version repair path of {@code TransactionManager.applyChanges}, splicing its delta over the first - a silent
 * lost update, and one that different nodes could resolve differently.
 * <p>
 * The ledger is the leader's answer. {@link ArcadeStateMachine#startTransaction} checks every page of an entry against
 * the version the log assigned last, under the database's ledger lock so two entries with the same base can never both
 * pass, and reserves the entry's versions; a mismatch refuses the entry before Ratis touches it, so the originator gets
 * a retryable {@link ConcurrentModificationException} instead of a fake acknowledgement, and no node ever sees a
 * conflicting entry in its log. {@link ArcadeStateMachine#preAppendTransaction} then confirms the reservation at the
 * point that fixes the log order, and the apply releases it: from then on the local copy of the page carries the
 * version itself. The ledger only ever holds the pages of in-flight entries, so its size is bounded by the replication
 * pipeline depth rather than by the database, and a page with no reservation is seeded from the local copy - correct
 * because Ratis makes a leader ready only after it has applied every entry that precedes its own term, and every entry
 * accepted since is either still reserved here or applied locally.
 * <p>
 * Two refinements keep the reservations honest. Ratis retries a request it could not append (a transient resource
 * limit, say) with the same client id and call id, so a reservation is remembered by the entry that made it and the
 * retry is accepted as the same entry rather than refused as a conflict. And a request Ratis dropped between the
 * reservation and the append leaves a reservation nothing will ever confirm or release: such a reservation, unconfirmed
 * past {@link #STALE_RESERVATION_MS}, is discarded when the next entry on the page is validated, so a dropped request
 * cannot fence a page off for the rest of the leadership.
 * <p>
 * The ledger is also visible to local validation through {@link com.arcadedb.engine.PageVersionReservations}, so a
 * transaction on the leader that touches a page an in-flight entry reserved fails its phase 1 with the same retryable
 * error, one Raft round trip earlier.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class PageVersionLedger {
  /**
   * How long an unconfirmed reservation is trusted. The confirmation follows the reservation on the same request
   * thread, straight into the log append, so anything older than this is a request Ratis dropped in between.
   */
  static final long STALE_RESERVATION_MS = 30_000L;

  private static final int WAL_TX_HEADER_SIZE   = 2 * Long.BYTES + 2 * Integer.BYTES;
  private static final int WAL_PAGE_HEADER_SIZE = 6 * Integer.BYTES;

  private final Map<String, DatabaseLedger> byDatabase = new ConcurrentHashMap<>();

  /** Resolves the version of a page on this node, {@code 0} for a page that does not exist yet. */
  interface LocalVersions {
    int versionOf(int fileId, int pageNumber) throws IOException;
  }

  /** Identity of a replicated request: the Raft client that submitted it and its call id, unique cluster-wide. */
  record EntryId(Object client, long callId) {
  }

  /**
   * The pages of a replicated WAL transaction with the version each is written at, decoded once from the entry's
   * bytes and reused by every step of the entry's life on the leader: validation, confirmation and release. Arrays of
   * primitives, one slot per page segment (a page split in several modified intervals appears once per segment, at
   * the same version).
   */
  record Pages(int[] fileIds, int[] pageNumbers, int[] versions) {
    int count() {
      return fileIds.length;
    }
  }

  private static final class Reservation {
    private final int     version;
    private final EntryId entry;
    private final long    reservedAtMs;
    private volatile boolean appended;

    private Reservation(final int version, final EntryId entry, final long reservedAtMs) {
      this.version = version;
      this.entry = entry;
      this.reservedAtMs = reservedAtMs;
    }
  }

  /** The reservations of one database; validation is a check-then-act, so it runs under the instance's monitor. */
  private static final class DatabaseLedger {
    private final ConcurrentHashMap<Long, Reservation> pages = new ConcurrentHashMap<>();
  }

  static long pageKey(final int fileId, final int pageNumber) {
    return ((long) fileId << 32) | (pageNumber & 0xFFFFFFFFL);
  }

  /**
   * Validates every page of a replicated WAL transaction against the versions the log assigned so far and, if all of
   * them follow the last assigned version, reserves the versions the transaction is about to take.
   *
   * @throws ConcurrentModificationException when at least one page was validated against a superseded version, or
   *                                         one that this node has not reached yet; nothing is reserved in that case
   */
  void validateAndReserve(final String databaseName, final Pages pages, final EntryId entry, final LocalVersions local)
      throws IOException {
    final DatabaseLedger ledger = byDatabase.computeIfAbsent(databaseName, k -> new DatabaseLedger());
    final long now = System.currentTimeMillis();
    synchronized (ledger) {
      // Validate everything first: an entry is refused as a whole, so no page of a refused entry may stay reserved.
      for (int i = 0; i < pages.count(); i++) {
        final int fileId = pages.fileIds()[i];
        final int pageNumber = pages.pageNumbers()[i];
        final int targetVersion = pages.versions()[i];
        final long key = pageKey(fileId, pageNumber);
        Reservation reserved = ledger.pages.get(key);
        if (reserved != null && !reserved.appended && !reserved.entry.equals(entry)
            && now - reserved.reservedAtMs > STALE_RESERVATION_MS) {
          // Reserved by a request Ratis never appended: nothing will confirm or release it, so it is dropped here.
          ledger.pages.remove(key, reserved);
          reserved = null;
        }
        final int expected;
        if (reserved == null)
          expected = local.versionOf(fileId, pageNumber);
        else if (reserved.entry.equals(entry))
          // The same request, retried by Ratis: it is validated against the base it reserved from.
          expected = reserved.version - 1;
        else
          expected = reserved.version;

        if (targetVersion != expected + 1)
          throw new ReplicatedPageConflictException(databaseName, fileId, pageNumber, targetVersion - 1, expected);
      }

      for (int i = 0; i < pages.count(); i++)
        ledger.pages.put(pageKey(pages.fileIds()[i], pages.pageNumbers()[i]), new Reservation(pages.versions()[i], entry, now));
    }
  }

  /** Marks the reservations of an entry as backed by the log: the entry has been appended at its final position. */
  void confirmAppended(final String databaseName, final Pages pages, final EntryId entry) {
    final DatabaseLedger ledger = byDatabase.get(databaseName);
    if (ledger == null)
      return;
    for (int i = 0; i < pages.count(); i++) {
      final Reservation reserved = ledger.pages.get(pageKey(pages.fileIds()[i], pages.pageNumbers()[i]));
      if (reserved != null && reserved.entry.equals(entry))
        reserved.appended = true;
    }
  }

  /**
   * Releases the reservations of an entry once it has been applied on this node. A page whose reservation moved on to
   * a later entry is left alone. The pages are decoded from the entry only when there is something to release, so a
   * follower, whose ledger is empty, never pays the decode.
   */
  void release(final String databaseName, final Pages pagesOrNull, final byte[] walData) {
    final DatabaseLedger ledger = byDatabase.get(databaseName);
    if (ledger == null || ledger.pages.isEmpty())
      return;
    final Pages pages = pagesOrNull != null ? pagesOrNull : parse(walData);
    for (int i = 0; i < pages.count(); i++) {
      final long key = pageKey(pages.fileIds()[i], pages.pageNumbers()[i]);
      final Reservation reserved = ledger.pages.get(key);
      if (reserved != null && reserved.version == pages.versions()[i])
        ledger.pages.remove(key, reserved);
    }
  }

  /**
   * @return the version reserved for the page by an entry not yet applied here, or {@code -1} when there is none
   */
  int reservedVersion(final String databaseName, final int fileId, final int pageNumber) {
    final DatabaseLedger ledger = byDatabase.get(databaseName);
    if (ledger == null || ledger.pages.isEmpty())
      return -1;
    final Reservation reserved = ledger.pages.get(pageKey(fileId, pageNumber));
    return reserved != null ? reserved.version : -1;
  }

  /** Number of pages currently reserved for the database (diagnostics and tests). */
  int reservedPages(final String databaseName) {
    final DatabaseLedger ledger = byDatabase.get(databaseName);
    return ledger != null ? ledger.pages.size() : 0;
  }

  void clear(final String databaseName) {
    byDatabase.remove(databaseName);
  }

  void clearAll() {
    byDatabase.clear();
  }

  /**
   * Decodes the page headers of a replicated WAL transaction without materializing the deltas. Same layout as
   * {@link ArcadeStateMachine#deserializeWalTransaction(byte[])}: txId, timestamp, page count, segment size, then per
   * page the file id, page number, delta range, target version, page size and the delta bytes.
   *
   * @throws ReplicationException when the bytes do not describe a well-formed transaction
   */
  static Pages parse(final byte[] walData) {
    if (walData == null || walData.length < WAL_TX_HEADER_SIZE)
      throw new ReplicationException("Corrupted WAL transaction entry: truncated header");

    final ByteBuffer buf = ByteBuffer.wrap(walData);
    buf.position(2 * Long.BYTES);
    final int pageCount = buf.getInt();
    buf.getInt(); // segmentSize
    if (pageCount < 0 || (long) pageCount * WAL_PAGE_HEADER_SIZE > buf.remaining())
      throw new ReplicationException("Corrupted WAL transaction entry: invalid page count " + pageCount);

    final int[] fileIds = new int[pageCount];
    final int[] pageNumbers = new int[pageCount];
    final int[] versions = new int[pageCount];
    for (int i = 0; i < pageCount; i++) {
      fileIds[i] = buf.getInt();
      pageNumbers[i] = buf.getInt();
      final int changesFrom = buf.getInt();
      final int changesTo = buf.getInt();
      versions[i] = buf.getInt();
      buf.getInt(); // currentPageSize
      final int deltaSize = changesTo - changesFrom + 1;
      if (deltaSize <= 0 || changesFrom < 0 || deltaSize > buf.remaining())
        throw new ReplicationException("Corrupted WAL transaction entry: invalid delta range [" + changesFrom + "," + changesTo
            + "] for page " + fileIds[i] + ":" + pageNumbers[i]);
      buf.position(buf.position() + deltaSize);
    }
    return new Pages(fileIds, pageNumbers, versions);
  }
}
