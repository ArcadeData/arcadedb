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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

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
    assertThat(ledger.confirmAppended(DB, PageVersionLedger.parse(appended), entry(2))).isTrue();

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

    // The delayed request finally reaches its append: its page now belongs to entry 3, so it must not be appended.
    assertThat(ledger.confirmAppended(DB, PageVersionLedger.parse(dropped), entry(1)))
        .as("an entry whose reservation was taken over cannot be appended")
        .isFalse();
    assertThat(ledger.confirmAppended(DB, PageVersionLedger.parse(wal(3, new Page(3, 0, 1))), entry(3))).isTrue();
  }

  /**
   * The local copy of a page is read outside the ledger lock; a release in between (an entry applied) invalidates
   * that read, so the same base is refused once the applied entry moved the page on.
   */
  @Test
  void anEarlyLocalReadIsNotTrustedAcrossARelease() throws IOException {
    final byte[] first = wal(1, new Page(3, 0, 1));
    ledger.validateAndReserve(DB, PageVersionLedger.parse(first), entry(1), localVersions);
    ledger.confirmAppended(DB, PageVersionLedger.parse(first), entry(1));
    // The page must hold no reservation, or the early local read is skipped altogether.
    ledger.release(DB, null, first);

    // The apply lands DURING the early read: the local copy moves to 1 and the release counter is bumped.
    final PageVersionLedger.LocalVersions applyDuringRead = (fileId, pageNumber) -> {
      final int version = localVersions.versionOf(fileId, pageNumber);
      local.put(PageVersionLedger.pageKey(3, 0), 1);
      ledger.release(DB, null, first);
      return version;
    };
    // A second entry validated against version 0 reads the page before the apply lands and must still be refused:
    // its early read said 0, the lock-side check re-reads 1.
    assertThatThrownBy(() -> ledger.validateAndReserve(DB, PageVersionLedger.parse(wal(2, new Page(3, 0, 1))), entry(2), applyDuringRead))
        .isInstanceOf(ConcurrentModificationException.class);
    ledger.validateAndReserve(DB, PageVersionLedger.parse(wal(3, new Page(3, 0, 2))), entry(3), localVersions);
  }

  /**
   * The local phase-1 check on the leader sees the reservations too, and a transaction it refuses never reaches the
   * ledger's own cleanup: a dropped request's reservation must expire on that path as well, or a page only the leader
   * writes stays fenced until the next leadership change.
   */
  @Test
  void theLocalCheckIgnoresAnExpiredUnconfirmedReservation() throws Exception {
    final byte[] dropped = wal(1, new Page(3, 0, 1));
    final byte[] appended = wal(2, new Page(4, 0, 1));
    ledger.validateAndReserve(DB, PageVersionLedger.parse(dropped), entry(1), localVersions);
    ledger.validateAndReserve(DB, PageVersionLedger.parse(appended), entry(2), localVersions);
    ledger.confirmAppended(DB, PageVersionLedger.parse(appended), entry(2));
    assertThat(ledger.reservedVersion(DB, 3, 0)).isEqualTo(1);

    backdate(DB, 3, 0, PageVersionLedger.STALE_RESERVATION_MS + 1);
    backdate(DB, 4, 0, PageVersionLedger.STALE_RESERVATION_MS + 1);

    assertThat(ledger.reservedVersion(DB, 3, 0)).as("the dropped request's reservation is gone").isEqualTo(-1);
    assertThat(ledger.reservedPages(DB)).isEqualTo(1);
    assertThat(ledger.reservedVersion(DB, 4, 0)).as("the appended entry's reservation stays until applied").isEqualTo(1);
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

  /** The header walk and the full deserialization read the same layout: they must agree, page by page. */
  @Test
  void theHeaderWalkAgreesWithTheFullDeserialization() {
    final byte[] entry = wal(9, new Page(3, 0, 4), new Page(3, 0, 4), new Page(7, 12, 1));
    final PageVersionLedger.Pages pages = PageVersionLedger.parse(entry);
    final com.arcadedb.engine.WALFile.WALTransaction full = ArcadeStateMachine.deserializeWalTransaction(entry);
    assertThat(pages.count()).isEqualTo(full.pages.length);
    for (int i = 0; i < pages.count(); i++) {
      assertThat(pages.fileIds()[i]).isEqualTo(full.pages[i].fileId);
      assertThat(pages.pageNumbers()[i]).isEqualTo(full.pages[i].pageNumber);
      assertThat(pages.versions()[i]).isEqualTo(full.pages[i].currentPageVersion);
    }
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

  /**
   * A multi-page entry that lost one page to a later reservation is refused as a whole: none of its other pages may be
   * marked appended, or they would be exempt from the stale sweep and fenced until the next leadership change.
   */
  @Test
  void aPartiallyReplacedEntryIsRefusedWithoutMarkingAnything() throws Exception {
    final byte[] delayed = wal(1, new Page(3, 0, 1), new Page(4, 0, 1));
    ledger.validateAndReserve(DB, PageVersionLedger.parse(delayed), entry(1), localVersions);
    backdate(DB, 3, 0, PageVersionLedger.STALE_RESERVATION_MS + 1);
    backdate(DB, 4, 0, PageVersionLedger.STALE_RESERVATION_MS + 1);

    // A newcomer takes page 4/0 over; page 3/0 still belongs to the delayed entry.
    ledger.validateAndReserve(DB, PageVersionLedger.parse(wal(2, new Page(4, 0, 1))), entry(2), localVersions);
    assertThat(ledger.confirmAppended(DB, PageVersionLedger.parse(delayed), entry(1))).isFalse();

    // Page 3/0 was NOT marked appended by the refused confirmation: it is still stale and sweeps on the next touch.
    ledger.validateAndReserve(DB, PageVersionLedger.parse(wal(3, new Page(3, 0, 1))), entry(3), localVersions);
    assertThat(ledger.reservedVersion(DB, 3, 0)).isEqualTo(1);
    assertThat(ledger.reservedPages(DB)).isEqualTo(2);
  }

  /**
   * The confirmation of a delayed entry and the stale-replacement of its reservation by a concurrent validation are
   * atomic: exactly one of the two entries wins, never both (which would put two entries with the same target version
   * in the log), never neither.
   */
  @Test
  void confirmationAndStaleReplacementAreAtomic() throws Exception {
    final byte[] delayed = wal(1, new Page(3, 0, 1));
    final byte[] newcomer = wal(2, new Page(3, 0, 1));
    for (int round = 0; round < 500; round++) {
      final PageVersionLedger contended = new PageVersionLedger();
      contended.validateAndReserve(DB, PageVersionLedger.parse(delayed), entry(1), localVersions);
      backdate(contended, DB, 3, 0, PageVersionLedger.STALE_RESERVATION_MS + 1);

      final CountDownLatch start = new CountDownLatch(1);
      final boolean[] delayedConfirmed = new boolean[1];
      final boolean[] newcomerAccepted = new boolean[1];
      final Thread confirmer = Thread.ofPlatform().unstarted(() -> {
        awaitLatch(start);
        delayedConfirmed[0] = contended.confirmAppended(DB, PageVersionLedger.parse(delayed), entry(1));
      });
      final Thread validator = Thread.ofPlatform().unstarted(() -> {
        awaitLatch(start);
        try {
          contended.validateAndReserve(DB, PageVersionLedger.parse(newcomer), entry(2), localVersions);
          newcomerAccepted[0] = true;
        } catch (final ConcurrentModificationException | IOException refused) {
          newcomerAccepted[0] = false;
        }
      });
      confirmer.start();
      validator.start();
      start.countDown();
      confirmer.join();
      validator.join();

      assertThat(delayedConfirmed[0] && newcomerAccepted[0]).as("round %d: both entries cannot win the page", round).isFalse();
      assertThat(delayedConfirmed[0] || newcomerAccepted[0]).as("round %d: one of them must", round).isTrue();
    }
  }

  private static void awaitLatch(final CountDownLatch latch) {
    try {
      latch.await();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Issue #11921 review: a session commit run by publishOutsideTheLog waits for its file locks, which an ordinary commit of
   * the leader holds from its phase 1 until its own entry is validated and appended. Waiting for them with the validation
   * lock held made the two wait for each other until the file-lock timeout. The commit modelled here takes its file lock,
   * then checks a version (the engine's order in commit1stPhase); the ordinary commit holds the file lock and validates.
   */
  @Test
  void aPublicationWaitingForAFileLockDoesNotHoldTheValidationLock() throws Exception {
    ledger.validateAndReserve(DB, PageVersionLedger.parse(wal(1, new Page(3, 0, 1))), entry(1), localVersions);
    final ReentrantLock fileLock = new ReentrantLock();
    final CountDownLatch publicationStarted = new CountDownLatch(1);
    final CountDownLatch fileLockHeld = new CountDownLatch(1);
    final AtomicReference<Throwable> validationFailure = new AtomicReference<>();
    final Thread ordinaryCommit = new Thread(() -> {
      fileLock.lock();
      try {
        fileLockHeld.countDown();
        awaitLatch(publicationStarted);
        ledger.validateAndReserve(DB, PageVersionLedger.parse(wal(2, new Page(5, 0, 1))), entry(2), localVersions);
      } catch (final Throwable t) {
        validationFailure.set(t);
      } finally {
        fileLock.unlock();
      }
    }, "issue11921-ordinary-commit");
    ordinaryCommit.start();
    awaitLatch(fileLockHeld);

    final boolean published = ledger.publishOutsideTheLog(DB, () -> {
      publicationStarted.countDown();
      if (!fileLock.tryLock(3, TimeUnit.SECONDS))
        return false;
      try {
        ledger.reservedVersion(DB, 3, 0);
        return true;
      } finally {
        fileLock.unlock();
      }
    });
    ordinaryCommit.join(10_000);

    assertThat(published).as("the session commit must get the file lock the ordinary commit releases once validated").isTrue();
    assertThat(validationFailure.get()).isNull();
    assertThat(ledger.reservedVersion(DB, 5, 0)).isEqualTo(1);
  }

  /** Issue #11921: from its first version check to its return, a publication still excludes every validation of the database. */
  @Test
  void aValidationWaitsForAPublicationThatChecked() throws Exception {
    final CountDownLatch validated = new CountDownLatch(1);
    final Thread validation = new Thread(() -> {
      try {
        ledger.validateAndReserve(DB, PageVersionLedger.parse(wal(1, new Page(3, 0, 1))), entry(1), localVersions);
        validated.countDown();
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }, "issue11921-validation");

    final boolean queuedDuringPublication = ledger.publishOutsideTheLog(DB, () -> {
      ledger.reservedVersion(DB, 3, 0);
      validation.start();
      return awaitQueuedOnTheValidationLock(ledger, DB, validation);
    });

    assertThat(queuedDuringPublication).as("a validation must wait on the lock between a publication's check and its return").isTrue();
    assertThat(validated.await(10, TimeUnit.SECONDS)).isTrue();
    validation.join(10_000);
  }

  /** Issue #11921: a publication whose commit throws after its first check must release the validation lock. */
  @Test
  void aPublicationThatThrowsAfterItsCheckReleasesTheValidationLock() throws Exception {
    assertThatThrownBy(() -> ledger.publishOutsideTheLog(DB, () -> {
      ledger.reservedVersion(DB, 3, 0);
      throw new ConcurrentModificationException("commit refused after its check");
    })).isInstanceOf(ConcurrentModificationException.class);

    // Another thread validating the same database must not block: the lock was released on the way out.
    final CountDownLatch validated = new CountDownLatch(1);
    final Thread validation = new Thread(() -> {
      try {
        ledger.validateAndReserve(DB, PageVersionLedger.parse(wal(1, new Page(3, 0, 1))), entry(1), localVersions);
        validated.countDown();
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }, "issue11921-validation-after-throw");
    validation.setDaemon(true);
    validation.start();
    assertThat(validated.await(10, TimeUnit.SECONDS)).as("the validation lock leaked by a throwing publication").isTrue();
    validation.join(10_000);

    // The thread-local is restored too: a check outside any publication takes no lock, so a later validation is not blocked.
    ledger.reservedVersion(DB, 3, 0);
    final CountDownLatch validatedAgain = new CountDownLatch(1);
    final Thread second = new Thread(() -> {
      try {
        ledger.validateAndReserve(DB, PageVersionLedger.parse(wal(2, new Page(5, 0, 1))), entry(2), localVersions);
        validatedAgain.countDown();
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }, "issue11921-validation-after-restore");
    second.setDaemon(true);
    second.start();
    assertThat(validatedAgain.await(10, TimeUnit.SECONDS)).as("a check outside a publication must not take the lock").isTrue();
    second.join(10_000);
  }

  /** Issue #11921: an inner publication of a thread that already holds the lock must not release it for the outer one. */
  @Test
  void aNestedPublicationDoesNotReleaseTheLockOfTheEnclosingOne() throws Exception {
    final CountDownLatch validated = new CountDownLatch(1);
    final Thread validation = new Thread(() -> {
      try {
        ledger.validateAndReserve(DB, PageVersionLedger.parse(wal(1, new Page(3, 0, 1))), entry(1), localVersions);
        validated.countDown();
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }, "issue11921-validation-nested");
    validation.setDaemon(true);

    final boolean queuedAfterInner = ledger.publishOutsideTheLog(DB, () -> {
      ledger.reservedVersion(DB, 3, 0);
      ledger.publishOutsideTheLog(DB, () -> ledger.reservedVersion(DB, 4, 0));
      validation.start();
      return awaitQueuedOnTheValidationLock(ledger, DB, validation);
    });

    assertThat(queuedAfterInner).as("the inner publication released the enclosing publication's lock").isTrue();
    assertThat(validated.await(10, TimeUnit.SECONDS)).isTrue();
    validation.join(10_000);
  }

  /**
   * Issue #11921 review: a leadership change clears every ledger while a publication may hold one's lock. The clear used
   * to drop the ledger, so a validation after it built a new one with a free lock and reserved the version the publication
   * was about to write. The ledger is now kept and emptied under its lock, so the validation still waits.
   */
  @Test
  void aLeadershipClearDuringAPublicationDoesNotFreeTheValidationLock() throws Exception {
    final CountDownLatch validated = new CountDownLatch(1);
    final Thread validation = new Thread(() -> {
      try {
        ledger.validateAndReserve(DB, PageVersionLedger.parse(wal(1, new Page(3, 0, 1))), entry(1), localVersions);
        validated.countDown();
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }, "issue11921-validation-after-clear");
    validation.setDaemon(true);
    final Thread clear = new Thread(ledger::clearAll, "issue11921-leadership-clear");
    clear.setDaemon(true);

    final boolean queuedAfterClear = ledger.publishOutsideTheLog(DB, () -> {
      ledger.reservedVersion(DB, 3, 0);
      clear.start();
      // The clear either ran (the old removal) or waits for this publication: either way a validation must not get through
      clear.join(200);
      validation.start();
      return awaitQueuedOnTheValidationLock(ledger, DB, validation);
    });

    assertThat(queuedAfterClear).as("a validation after a leadership clear ran while a publication held the lock").isTrue();
    assertThat(validated.await(10, TimeUnit.SECONDS)).isTrue();
    validation.join(10_000);
    clear.join(10_000);
  }

  /** Issue #11921 review: a clear between a publication's start and its first check must not let that check skip the lock. */
  @Test
  void aLeadershipClearBeforeThePublicationsFirstCheckStillLetsItTakeTheLock() throws Exception {
    final CountDownLatch validated = new CountDownLatch(1);
    final Thread validation = new Thread(() -> {
      try {
        ledger.validateAndReserve(DB, PageVersionLedger.parse(wal(1, new Page(3, 0, 1))), entry(1), localVersions);
        validated.countDown();
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    }, "issue11921-validation-after-early-clear");
    validation.setDaemon(true);

    final boolean queued = ledger.publishOutsideTheLog(DB, () -> {
      ledger.clearAll();
      ledger.reservedVersion(DB, 3, 0);
      validation.start();
      return awaitQueuedOnTheValidationLock(ledger, DB, validation);
    });

    assertThat(queued).as("the publication's first check after a clear did not take the validation lock").isTrue();
    assertThat(validated.await(10, TimeUnit.SECONDS)).isTrue();
    validation.join(10_000);
  }

  /** A leadership clear still drops every reservation, of every database. */
  @Test
  void aLeadershipClearDropsEveryReservation() throws IOException {
    ledger.validateAndReserve(DB, PageVersionLedger.parse(wal(1, new Page(3, 0, 1))), entry(1), localVersions);
    ledger.validateAndReserve("other", PageVersionLedger.parse(wal(2, new Page(3, 0, 1))), entry(2), localVersions);

    ledger.clearAll();

    assertThat(ledger.reservedPages(DB)).isZero();
    assertThat(ledger.reservedPages("other")).isZero();
    assertThat(ledger.reservedVersion(DB, 3, 0)).isEqualTo(-1);
    // A fresh validation of the same base goes through: nothing reserved survived the clear
    ledger.validateAndReserve(DB, PageVersionLedger.parse(wal(3, new Page(3, 0, 1))), entry(3), localVersions);
    assertThat(ledger.reservedVersion(DB, 3, 0)).isEqualTo(1);
  }

  /**
   * Waits until {@code thread} is queued on the validation lock of {@code database}, the state a validation excluded by a
   * publication must reach. {@code false} once the thread ended without queueing, which is a validation that got through.
   */
  private static boolean awaitQueuedOnTheValidationLock(final PageVersionLedger ledger, final String database, final Thread thread)
      throws Exception {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (System.nanoTime() < deadline) {
      final ReentrantLock lock = validationLock(ledger, database);
      if (lock != null && lock.hasQueuedThread(thread))
        return true;
      if (thread.getState() == Thread.State.TERMINATED)
        return false;
      Thread.sleep(1);
    }
    return false;
  }

  private static ReentrantLock validationLock(final PageVersionLedger ledger, final String database) throws Exception {
    final Field byDatabase = PageVersionLedger.class.getDeclaredField("byDatabase");
    byDatabase.setAccessible(true);
    final Object databaseLedger = ((Map<?, ?>) byDatabase.get(ledger)).get(database);
    if (databaseLedger == null)
      return null;
    final Field lock = databaseLedger.getClass().getDeclaredField("lock");
    lock.setAccessible(true);
    return (ReentrantLock) lock.get(databaseLedger);
  }

  /** Ages a reservation, since the staleness bound is measured on the wall clock. */
  private void backdate(final String database, final int fileId, final int pageNumber, final long byMs) throws Exception {
    backdate(ledger, database, fileId, pageNumber, byMs);
  }

  private static void backdate(final PageVersionLedger ledger, final String database, final int fileId, final int pageNumber,
      final long byMs) throws Exception {
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
