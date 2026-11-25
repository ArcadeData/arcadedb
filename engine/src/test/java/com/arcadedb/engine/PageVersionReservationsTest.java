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
package com.arcadedb.engine;

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ConcurrentModificationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The engine-side half of issue #6965: a page version reserved by the replication layer counts as the most recent one
 * in the phase-1 check, a standalone database with no reservations installed is untouched, and the phase-2 bump never
 * consults the reservations.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PageVersionReservationsTest {
  @TempDir
  Path tempDir;

  private LocalDatabase db;
  private RID           rid;

  @BeforeEach
  void setUp() {
    db = (LocalDatabase) new DatabaseFactory(tempDir.resolve("reservations").toString()).create();
    db.getSchema().createDocumentType("Counter", 1);
    db.transaction(() -> rid = db.newDocument("Counter").set("value", 0L).save().getIdentity());
  }

  @AfterEach
  void tearDown() {
    if (db != null && db.isOpen())
      db.drop();
  }

  @Test
  void aReservedVersionRefusesThePhaseOneCheckUntilReleased() {
    assertThat(db.getPageVersionReservations()).as("a standalone database has none installed").isNull();

    final AtomicInteger consulted = new AtomicInteger();
    final PageId counterPage = new PageId(db, rid.getBucketId(), 0);
    db.setPageVersionReservations(pageId -> {
      consulted.incrementAndGet();
      return pageId.equals(counterPage) ? Integer.MAX_VALUE : -1;
    });

    assertThatThrownBy(() -> increment(0)).isInstanceOf(ConcurrentModificationException.class);
    assertThat(consulted.get()).as("the phase-1 check consulted the reservations").isGreaterThan(0);
    assertThat(db.lookupByRID(rid, true).asDocument().getLong("value")).isZero();

    // Released: the same transaction goes through, and the phase-2 bump validated against the local copy only.
    db.setPageVersionReservations(pageId -> -1);
    increment(0);
    assertThat(db.lookupByRID(rid, true).asDocument().getLong("value")).isEqualTo(1L);

    db.setPageVersionReservations(null);
    increment(0);
    assertThat(db.lookupByRID(rid, true).asDocument().getLong("value")).isEqualTo(2L);
  }

  /**
   * Issue #11921: the authority is told once per commit that the version checks are about to start, before the first
   * reservation is read, so a lock it takes there covers the check of every page, the first one included. A commit with
   * nothing to check does not call it.
   */
  @Test
  void theAuthorityIsToldOnceBeforeTheFirstVersionCheck() {
    final List<String> calls = new ArrayList<>();
    db.setPageVersionReservations(new PageVersionReservations() {
      @Override
      public int reservedVersion(final PageId pageId) {
        calls.add("reserved");
        return -1;
      }

      @Override
      public void beforeVersionChecks() {
        calls.add("before");
      }
    });

    increment(0);
    assertThat(calls).as("one call, ahead of every reservation read").isNotEmpty().startsWith("before");
    assertThat(calls.stream().filter("before"::equals).count()).isEqualTo(1);
    assertThat(calls).contains("reserved");

    calls.clear();
    db.transaction(() -> db.lookupByRID(rid, true));
    assertThat(calls).as("a commit that changed nothing checks no version").isEmpty();
  }

  /** Issue #11921: a failure raised there fails the commit like a refused version check, and nothing is published. */
  @Test
  void aRefusalBeforeTheVersionChecksFailsTheCommit() {
    db.setPageVersionReservations(new PageVersionReservations() {
      @Override
      public int reservedVersion(final PageId pageId) {
        return -1;
      }

      @Override
      public void beforeVersionChecks() {
        throw new ConcurrentModificationException("refused before the checks");
      }
    });

    assertThatThrownBy(() -> increment(0)).isInstanceOf(ConcurrentModificationException.class);
    db.setPageVersionReservations(null);
    assertThat(db.lookupByRID(rid, true).asDocument().getLong("value")).isZero();
    increment(0);
    assertThat(db.lookupByRID(rid, true).asDocument().getLong("value")).as("the file locks of the refused commit were released")
        .isEqualTo(1L);
  }

  /** Issue #12470: with no reservation installed (a standalone database, a replica) there is nothing to wait for. */
  @Test
  void awaitingAPageOfADatabaseWithoutReservationsAnswersAtOnce() throws Exception {
    assertThat(db.getPageVersionReservations()).isNull();

    assertThat(awaitApplied(60_000)).as("a hang here would run into the test's own timeout, not wait for the bound").isTrue();
  }

  /** Issue #12470: a reservation ahead of the local copy is waited for until the entry is applied, then the wait ends. */
  @Test
  void aReservationAheadOfTheLocalVersionIsWaitedForUntilItIsApplied() throws Exception {
    final PageId counterPage = counterPage();
    final AtomicInteger consultations = new AtomicInteger();
    db.setPageVersionReservations(pageId -> {
      assertThat(pageId).isEqualTo(counterPage);
      return consultations.incrementAndGet() <= 5 ? Integer.MAX_VALUE : -1;
    });

    assertThat(awaitApplied(60_000)).isTrue();
    assertThat(consultations.get()).as("polled until the reservation went away").isEqualTo(6);

    // And what the wait answered true for is what the phase-1 check accepts
    db.setPageVersionReservations(pageId -> -1);
    increment(0);
  }

  /** Issue #12470: a reservation that fences nothing (at or below the local version) is not worth a wait, as in the check. */
  @Test
  void aReservationAtOrBelowTheLocalVersionIsNotWaitedFor() throws Exception {
    final PageId counterPage = counterPage();
    final int localVersion = PageManager.INSTANCE.getMostRecentVersionOfPage(counterPage, pageSizeOf(counterPage));
    assertThat(localVersion).isPositive();
    final AtomicInteger consultations = new AtomicInteger();
    db.setPageVersionReservations(pageId -> {
      consultations.incrementAndGet();
      return localVersion;
    });

    assertThat(awaitApplied(60_000)).isTrue();
    assertThat(consultations.get()).as("answered by the first look").isEqualTo(1);
    // The same value does not refuse the check either
    increment(0);
  }

  /** Issue #12470: a request nothing will ever apply must not hold the caller for ever: the wait gives up and says so. */
  @Test
  void theWaitGivesUpOnAReservationThatIsNeverApplied() throws Exception {
    db.setPageVersionReservations(pageId -> Integer.MAX_VALUE);

    assertThat(awaitApplied(30)).isFalse();
    assertThat(awaitApplied(0)).as("no budget: one look, no sleep").isFalse();
  }

  /** Issue #12470: an interrupted wait ends at once, answers that it gave up and leaves the flag for the caller. */
  @Test
  void anInterruptedWaitEndsAndKeepsTheInterruptFlag() throws Exception {
    db.setPageVersionReservations(pageId -> Integer.MAX_VALUE);

    Thread.currentThread().interrupt();
    try {
      assertThat(awaitApplied(60_000)).isFalse();
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      // Clears the flag so the rest of the class does not run interrupted
      Thread.interrupted();
    }
  }

  private PageId counterPage() {
    return new PageId(db, rid.getBucketId(), 0);
  }

  private int pageSizeOf(final PageId pageId) {
    return ((LocalBucket) db.getSchema().getFileById(pageId.getFileId())).getPageSize();
  }

  private boolean awaitApplied(final long timeoutMs) throws Exception {
    final PageId counterPage = counterPage();
    return db.getPageManager().awaitReservedVersionApplied(counterPage, pageSizeOf(counterPage), timeoutMs);
  }

  private void increment(final int retries) {
    db.transaction(() -> {
      final Document counter = db.lookupByRID(rid, true).asDocument();
      counter.modify().set("value", counter.getLong("value") + 1).save();
    }, false, retries);
  }
}
