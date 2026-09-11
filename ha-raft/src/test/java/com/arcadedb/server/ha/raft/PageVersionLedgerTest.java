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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The leader-side page-version ledger of issue #6965: every replicated transaction must follow, page by page, the
 * version the log assigned last, whether that version is still reserved by an in-flight entry or already visible on
 * the local copy of the page.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PageVersionLedgerTest {
  private static final String DB = "ledger";

  /** A page id and the version the entry writes it at. */
  private record Page(int fileId, int pageNumber, int targetVersion) {
  }

  private final PageVersionLedger    ledger = new PageVersionLedger();
  private final Map<Long, Integer>   local  = new HashMap<>();
  private final PageVersionLedger.LocalVersions localVersions = (fileId, pageNumber) ->
      local.getOrDefault(PageVersionLedger.pageKey(fileId, pageNumber), 0);

  private static PageVersionLedger.EntryId entry(final long callId) {
    return new PageVersionLedger.EntryId("client", callId);
  }

  @Test
  void acceptsTheNextVersionAndRefusesTheSameBaseTwice() throws IOException {
    ledger.validateAndReserve(DB, PageVersionLedger.parse(wal(1, new Page(3, 0, 1))), entry(1), localVersions);
    assertThat(ledger.reservedVersion(DB, 3, 0)).isEqualTo(1);

    // A second entry validated against the same base (version 0) must be refused: the log already gave version 1 away.
    assertThatThrownBy(() -> ledger.validateAndReserve(DB, PageVersionLedger.parse(wal(2, new Page(3, 0, 1))), entry(2), localVersions))
        .isInstanceOf(ConcurrentModificationException.class)
        .hasMessageContaining("3/0")
        .hasMessageContaining("version 0")
        .hasMessageContaining("version 1");

    // An entry that chains on the reserved version (its originator applied the first entry) is fine.
    ledger.validateAndReserve(DB, PageVersionLedger.parse(wal(3, new Page(3, 0, 2))), entry(3), localVersions);
    assertThat(ledger.reservedVersion(DB, 3, 0)).isEqualTo(2);
  }

  @Test
  void refusesAnEntryAheadOfTheLog() {
    local.put(PageVersionLedger.pageKey(3, 0), 5);
    assertThatThrownBy(() -> ledger.validateAndReserve(DB, PageVersionLedger.parse(wal(1, new Page(3, 0, 7))), entry(1), localVersions))
        .isInstanceOf(ConcurrentModificationException.class);
    assertThatThrownBy(() -> ledger.validateAndReserve(DB, PageVersionLedger.parse(wal(1, new Page(3, 0, 5))), entry(1), localVersions))
        .isInstanceOf(ConcurrentModificationException.class);
    assertThat(ledger.reservedPages(DB)).isZero();
  }

  @Test
  void seedsFromTheLocalCopyWhenNothingIsReserved() throws IOException {
    local.put(PageVersionLedger.pageKey(3, 0), 5);
    ledger.validateAndReserve(DB, PageVersionLedger.parse(wal(1, new Page(3, 0, 6))), entry(1), localVersions);
    assertThat(ledger.reservedVersion(DB, 3, 0)).isEqualTo(6);
  }

  @Test
  void aRefusedEntryReservesNothing() throws IOException {
    ledger.validateAndReserve(DB, PageVersionLedger.parse(wal(1, new Page(3, 0, 1))), entry(1), localVersions);
    // Page 4/0 is fine, page 3/0 conflicts: the entry is refused as a whole and 4/0 must not stay reserved.
    assertThatThrownBy(() -> ledger.validateAndReserve(DB, PageVersionLedger.parse(wal(2, new Page(4, 0, 1), new Page(3, 0, 1))), entry(2), localVersions))
        .isInstanceOf(ConcurrentModificationException.class);
    assertThat(ledger.reservedVersion(DB, 4, 0)).isEqualTo(-1);
    assertThat(ledger.reservedPages(DB)).isEqualTo(1);
  }

  @Test
  void releaseDropsOnlyTheVersionTheEntryReserved() throws IOException {
    final byte[] first = wal(1, new Page(3, 0, 1));
    final byte[] second = wal(2, new Page(3, 0, 2));
    ledger.validateAndReserve(DB, PageVersionLedger.parse(first), entry(1), localVersions);
    ledger.validateAndReserve(DB, PageVersionLedger.parse(second), entry(2), localVersions);

    // The first entry was applied: its reservation moved on to the second entry already, so nothing changes.
    ledger.release(DB, null, first);
    assertThat(ledger.reservedVersion(DB, 3, 0)).isEqualTo(2);

    ledger.release(DB, null, second);
    assertThat(ledger.reservedVersion(DB, 3, 0)).isEqualTo(-1);
    assertThat(ledger.reservedPages(DB)).isZero();

    // Back to the local copy as the seed (still 0 here: nothing was applied in this test).
    ledger.validateAndReserve(DB, PageVersionLedger.parse(wal(3, new Page(3, 0, 1))), entry(3), localVersions);
  }

  /** Ratis retries a request it could not append with the same client id and call id: that is not a conflict. */
  @Test
  void theSameRequestRetriedIsAcceptedAgain() throws IOException {
    final byte[] first = wal(1, new Page(3, 0, 1));
    ledger.validateAndReserve(DB, PageVersionLedger.parse(first), entry(1), localVersions);
    ledger.validateAndReserve(DB, PageVersionLedger.parse(first), entry(1), localVersions);
    assertThat(ledger.reservedVersion(DB, 3, 0)).isEqualTo(1);
    assertThat(ledger.reservedPages(DB)).isEqualTo(1);

    // Still one reservation, still one version given away: another entry on the same base is refused.
    assertThatThrownBy(() -> ledger.validateAndReserve(DB, PageVersionLedger.parse(wal(2, new Page(3, 0, 1))), entry(2), localVersions))
        .isInstanceOf(ConcurrentModificationException.class);
  }

  /**
   * A reservation nothing confirmed is a request Ratis dropped between the reservation and the append: past the
   * staleness bound it is discarded, so it cannot fence the page off for the rest of the leadership. A confirmed
   * one is backed by the log and stays until the entry is applied, however old.
   */
  @Test
  void anUnconfirmedReservationExpiresAConfirmedOneDoesNot() throws Exception {
    final byte[] dropped = wal(1, new Page(3, 0, 1));
    final byte[] appended = wal(2, new Page(4, 0, 1));
    ledger.validateAndReserve(DB, PageVersionLedger.parse(dropped), entry(1), localVersions);
    ledger.validateAndReserve(DB, PageVersionLedger.parse(appended), entry(2), localVersions);
    ledger.confirmAppended(DB, PageVersionLedger.parse(appended), entry(2));

    // Before the bound, both hold.
    assertThatThrownBy(() -> ledger.validateAndReserve(DB, PageVersionLedger.parse(wal(3, new Page(3, 0, 1))), entry(3), localVersions))
        .isInstanceOf(ConcurrentModificationException.class);

    backdate(DB, 3, 0, PageVersionLedger.STALE_RESERVATION_MS + 1);
    backdate(DB, 4, 0, PageVersionLedger.STALE_RESERVATION_MS + 1);

    // The dropped request's reservation is discarded and the page is seeded from the local copy again...
    ledger.validateAndReserve(DB, PageVersionLedger.parse(wal(3, new Page(3, 0, 1))), entry(3), localVersions);
    assertThat(ledger.reservedVersion(DB, 3, 0)).isEqualTo(1);
    // ... while the appended entry's reservation still refuses a stale base.
    assertThatThrownBy(() -> ledger.validateAndReserve(DB, PageVersionLedger.parse(wal(4, new Page(4, 0, 1))), entry(4), localVersions))
        .isInstanceOf(ConcurrentModificationException.class);
  }

  @Test
  void aPageSplitInSeveralSegmentsIsOnePage() throws IOException {
    // Two disjoint modified intervals of the same page ship as two consecutive segments at the same target version.
    ledger.validateAndReserve(DB, PageVersionLedger.parse(wal(1, new Page(3, 0, 1), new Page(3, 0, 1))), entry(1), localVersions);
    assertThat(ledger.reservedPages(DB)).isEqualTo(1);
    assertThat(ledger.reservedVersion(DB, 3, 0)).isEqualTo(1);
  }

  @Test
  void databasesAreIndependent() throws IOException {
    ledger.validateAndReserve(DB, PageVersionLedger.parse(wal(1, new Page(3, 0, 1))), entry(1), localVersions);
    ledger.validateAndReserve("other", PageVersionLedger.parse(wal(1, new Page(3, 0, 1))), entry(1), localVersions);
    assertThat(ledger.reservedVersion("other", 3, 0)).isEqualTo(1);

    ledger.clear("other");
    assertThat(ledger.reservedVersion("other", 3, 0)).isEqualTo(-1);
    assertThat(ledger.reservedVersion(DB, 3, 0)).isEqualTo(1);

    ledger.clearAll();
    assertThat(ledger.reservedVersion(DB, 3, 0)).isEqualTo(-1);
  }

  @Test
  void refusesACorruptedEntry() {
    assertThatThrownBy(() -> ledger.validateAndReserve(DB, PageVersionLedger.parse(new byte[3]), entry(1), localVersions))
        .isInstanceOf(ReplicationException.class);
    final byte[] tooManyPages = wal(1, new Page(3, 0, 1));
    ByteBuffer.wrap(tooManyPages).putInt(2 * Long.BYTES, 1_000_000);
    assertThatThrownBy(() -> ledger.validateAndReserve(DB, PageVersionLedger.parse(tooManyPages), entry(1), localVersions))
        .isInstanceOf(ReplicationException.class);
  }

  /** Ages a reservation, since the staleness bound is measured on the wall clock. */
  private void backdate(final String database, final int fileId, final int pageNumber, final long byMs) throws Exception {
    final Field byDatabase = PageVersionLedger.class.getDeclaredField("byDatabase");
    byDatabase.setAccessible(true);
    final Object databaseLedger = ((Map<?, ?>) byDatabase.get(ledger)).get(database);
    final Field pages = databaseLedger.getClass().getDeclaredField("pages");
    pages.setAccessible(true);
    final Object reservation = ((Map<?, ?>) pages.get(databaseLedger)).get(PageVersionLedger.pageKey(fileId, pageNumber));
    final Field reservedAtMs = reservation.getClass().getDeclaredField("reservedAtMs");
    reservedAtMs.setAccessible(true);
    reservedAtMs.setLong(reservation, reservedAtMs.getLong(reservation) - byMs);
  }

  /** The wire layout {@code ArcadeStateMachine.deserializeWalTransaction} reads, with a 4-byte delta per segment. */
  static byte[] wal(final long txId, final Page... pages) {
    final int deltaSize = 4;
    final ByteBuffer buf = ByteBuffer.allocate(2 * Long.BYTES + 2 * Integer.BYTES + pages.length * (6 * Integer.BYTES + deltaSize));
    buf.putLong(txId);
    buf.putLong(System.currentTimeMillis());
    buf.putInt(pages.length);
    buf.putInt(6 * Integer.BYTES + deltaSize);
    for (final Page page : pages) {
      buf.putInt(page.fileId());
      buf.putInt(page.pageNumber());
      buf.putInt(64);                    // changesFrom
      buf.putInt(64 + deltaSize - 1);    // changesTo
      buf.putInt(page.targetVersion());
      buf.putInt(1024);                  // currentPageSize
      buf.put(new byte[deltaSize]);
    }
    return buf.array();
  }
}
