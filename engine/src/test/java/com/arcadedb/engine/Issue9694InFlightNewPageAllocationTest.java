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

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for issue #9694: a page a transaction has just created is private to it until it commits, but the
 * allocator of every other transaction could still pick it.
 * <p>
 * Writing a record on a new page advertises the page's free space in the bucket-wide free-space map, and a concurrent
 * transaction whose own page horizon is past that page number (because it created a later page itself, or because a
 * later page was committed first) reads it from the page manager as an empty page and claims its slot 0 - the very
 * slot the creator wrote first. Both commits can never succeed: whichever comes second fails with a
 * {@code ConcurrentModificationException} ("Slot rebase not possible on page ... slot 0" when the creator wins). With
 * four async workers on one bucket every batch creates its own page, so this happened on most batch commits and used
 * up {@code arcadedb.txRetries} on about 0.4% of the #7615 loads.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9694InFlightNewPageAllocationTest extends TestHelper {

  private static final String TYPE = "Issue9694Item";

  @Override
  protected void beginTest() {
    database.getSchema().buildDocumentType().withName(TYPE).withTotalBuckets(1).create();
  }

  @Test
  void aConcurrentTransactionDoesNotAllocateOnAPageAnotherTransactionCreated() throws Exception {
    final CountDownLatch creatorWrote = new CountDownLatch(1);
    final CountDownLatch otherCommitted = new CountDownLatch(1);
    final AtomicReference<Throwable> creatorFailure = new AtomicReference<>();
    final AtomicReference<Throwable> otherFailure = new AtomicReference<>();
    final AtomicReference<RID> creatorRid = new AtomicReference<>();
    final List<RID> otherRids = new CopyOnWriteArrayList<>();

    // The creator writes the bucket's first page and keeps its transaction open.
    final Thread creator = new Thread(() -> {
      try {
        database.begin();
        final MutableDocument doc = database.newDocument(TYPE).set("owner", "creator");
        doc.save();
        creatorRid.set(doc.getIdentity());
        creatorWrote.countDown();
        assertThat(otherCommitted.await(30, TimeUnit.SECONDS)).isTrue();
        database.commit();
      } catch (final Throwable e) {
        creatorFailure.set(e);
        creatorWrote.countDown();
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }
    });

    // The other transaction cannot see that page, so its first record opens a later one. Its second record used to be
    // allocated from the free-space map, where the creator's page is listed first and lies below its horizon.
    final Thread other = new Thread(() -> {
      try {
        assertThat(creatorWrote.await(30, TimeUnit.SECONDS)).isTrue();
        database.begin();
        for (int i = 0; i < 2; i++) {
          final MutableDocument doc = database.newDocument(TYPE).set("owner", "other").set("seq", i);
          doc.save();
          otherRids.add(doc.getIdentity());
        }
        database.commit();
      } catch (final Throwable e) {
        otherFailure.set(e);
      } finally {
        if (database.isTransactionActive())
          database.rollback();
        otherCommitted.countDown();
      }
    });

    creator.start();
    other.start();
    creator.join(60_000);
    other.join(60_000);
    assertThat(creator.isAlive()).as("the creator thread hung").isFalse();
    assertThat(other.isAlive()).as("the other thread hung").isFalse();

    assertThat(otherFailure.get()).as("the other transaction").isNull();
    assertThat(creatorFailure.get()).as("the creator transaction").isNull();

    final int maxRecordsInPage = ((LocalBucket) database.getSchema().getBucketById(creatorRid.get().getBucketId())).getMaxRecordsInPage();
    final long creatorPage = creatorRid.get().getPosition() / maxRecordsInPage;
    for (final RID rid : otherRids)
      assertThat(rid.getPosition() / maxRecordsInPage).as("record %s must not be on the creator's page", rid).isNotEqualTo(creatorPage);

    database.transaction(() -> {
      assertThat(scanCount()).isEqualTo(3);
      assertThat(database.lookupByRID(creatorRid.get(), true).asDocument().getString("owner")).isEqualTo("creator");
      for (final RID rid : otherRids)
        assertThat(database.lookupByRID(rid, true).asDocument().getString("owner")).isEqualTo("other");
    });
  }

  /**
   * A page stays its creator's only while the creator runs: a commit, a rollback and a commit that fails with a
   * conflict all give it back, and a page abandoned by a rollback is an empty page the next insert can fill.
   */
  @Test
  void aCreatedPageIsGivenBackWheneverItsTransactionEnds() throws Exception {
    final LocalBucket bucket = (LocalBucket) database.getSchema().getType(TYPE).getBuckets(false).getFirst();
    database.getSchema().createDocumentType("Issue9694Other");
    database.begin();
    final RID shared = database.newDocument("Issue9694Other").set("v", 0).save().getIdentity();
    database.commit();

    // ROLLBACK: the page the transaction created is never committed
    database.begin();
    database.newDocument(TYPE).set("owner", "rolledBack").save();
    assertThat(bucket.getInFlightNewPagesForTesting()).isEqualTo(1);
    database.rollback();
    assertThat(bucket.getInFlightNewPagesForTesting()).isZero();

    // FAILED COMMIT: a new page plus an update another transaction commits first
    database.begin();
    database.newDocument(TYPE).set("owner", "conflicted").save();
    database.lookupByRID(shared, true).asDocument().modify().set("v", 1).save();
    assertThat(bucket.getInFlightNewPagesForTesting()).isEqualTo(1);
    final Thread concurrent = new Thread(() -> database.transaction(
        () -> database.lookupByRID(shared, true).asDocument().modify().set("v", 2).save()));
    concurrent.start();
    concurrent.join(30_000);
    assertThat(concurrent.isAlive()).as("the concurrent updater hung").isFalse();
    assertThatThrownBy(database::commit).isInstanceOf(ConcurrentModificationException.class);
    assertThat(bucket.getInFlightNewPagesForTesting()).isZero();

    // COMMIT: the abandoned pages are free again, and a committed page is an ordinary one afterwards
    database.begin();
    final RID rid = database.newDocument(TYPE).set("owner", "committed").save().getIdentity();
    database.commit();
    assertThat(bucket.getInFlightNewPagesForTesting()).isZero();

    database.transaction(() -> {
      assertThat(scanCount()).isEqualTo(1);
      assertThat(database.lookupByRID(rid, true).asDocument().getString("owner")).isEqualTo("committed");
    });
  }

  /**
   * The second half of #9694: two transactions inserting into the same page reserve disjoint slots (#5279), and the
   * one that commits second has its inserts replayed on the committed page. When the first one held the LAST slot of
   * the record table, the committed page's record count reached {@code maxRecordsInPage}, and the replay measured the
   * page with the allocator's "can a new record go here?" walker, which gives up on such a page without measuring it.
   * The second transaction's insert - into a slot that is free, on a page with plenty of free bytes - was refused, and
   * the whole transaction retried. Small records fill the 2048 slots of a page long before its bytes, so with several
   * writers on one bucket this was the common case.
   */
  @Test
  void anInsertIsMergedIntoAPageWhoseRecordTableAnotherTransactionFilled() throws Exception {
    final LocalBucket bucket = (LocalBucket) database.getSchema().getType(TYPE).getBuckets(false).getFirst();
    final int maxRecordsInPage = bucket.getMaxRecordsInPage();
    final int prefilled = maxRecordsInPage - 8;

    database.transaction(() -> {
      for (int i = 0; i < prefilled; i++)
        database.newDocument(TYPE).set("seq", i).save();
    });

    final CountDownLatch firstWrote = new CountDownLatch(1);
    final CountDownLatch secondCommitted = new CountDownLatch(1);
    final AtomicReference<Throwable> firstFailure = new AtomicReference<>();
    final AtomicReference<Throwable> secondFailure = new AtomicReference<>();
    final AtomicReference<RID> firstRid = new AtomicReference<>();
    final List<RID> secondRids = new CopyOnWriteArrayList<>();

    // Takes the first free slot of the page and commits last.
    final Thread first = new Thread(() -> {
      try {
        database.begin();
        final MutableDocument doc = database.newDocument(TYPE).set("owner", "first");
        doc.save();
        firstRid.set(doc.getIdentity());
        firstWrote.countDown();
        assertThat(secondCommitted.await(30, TimeUnit.SECONDS)).isTrue();
        database.commit();
      } catch (final Throwable e) {
        firstFailure.set(e);
        firstWrote.countDown();
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }
    });

    // Takes every other free slot of the page, up to the last one, and commits first.
    final Thread second = new Thread(() -> {
      try {
        assertThat(firstWrote.await(30, TimeUnit.SECONDS)).isTrue();
        database.begin();
        for (int i = 0; i < 7; i++) {
          final MutableDocument doc = database.newDocument(TYPE).set("owner", "second").set("seq", i);
          doc.save();
          secondRids.add(doc.getIdentity());
        }
        database.commit();
      } catch (final Throwable e) {
        secondFailure.set(e);
      } finally {
        if (database.isTransactionActive())
          database.rollback();
        secondCommitted.countDown();
      }
    });

    first.start();
    second.start();
    first.join(60_000);
    second.join(60_000);
    assertThat(first.isAlive()).as("the first thread hung").isFalse();
    assertThat(second.isAlive()).as("the second thread hung").isFalse();

    assertThat(secondFailure.get()).as("the transaction that filled the record table").isNull();
    // Not vacuous: both transactions really shared page 0, and the second one took its last slot
    assertThat(firstRid.get().getPosition()).isEqualTo(prefilled);
    assertThat(secondRids).hasSize(7);
    assertThat(secondRids.getLast().getPosition()).isEqualTo(maxRecordsInPage - 1);

    assertThat(firstFailure.get()).as("the transaction merged into the full record table").isNull();
    database.transaction(() -> {
      assertThat(scanCount()).isEqualTo(maxRecordsInPage);
      assertThat(database.lookupByRID(firstRid.get(), true).asDocument().getString("owner")).isEqualTo("first");
      for (final RID rid : secondRids)
        assertThat(database.lookupByRID(rid, true).asDocument().getString("owner")).isEqualTo("second");
    });
  }

  /**
   * Found while measuring #9694: when the page the allocator offered had no room left once the slots running
   * transactions reserved were counted, the insert opened a brand-new page - while the free-space map kept offering the
   * same page to the next insert, so the transaction opened one page PER RECORD. The #7615 load (4 async workers on
   * one bucket) spread 9,742 small records over ~100 pages, where one worker needs 5. The rejected page is now dropped
   * from the map and the allocator asked again. The bound counts pages, not time: each worker may hold a partly filled
   * tail page of its own, which is the whole difference from a single worker.
   */
  @Test
  void concurrentInsertsDoNotOpenAPagePerRecord() {
    final int total = 9_742;
    database.async().setParallelLevel(4);
    database.async().setCommitEvery(1_000);
    final AtomicReference<Throwable> asyncFailure = new AtomicReference<>();
    database.async().onError(asyncFailure::set);

    for (int i = 0; i < total; i++)
      database.async().command("sql", "INSERT INTO " + TYPE + " SET seq = :seq", null, Map.of("seq", i));
    database.async().waitCompletion();

    assertThat(asyncFailure.get()).isNull();
    database.transaction(() -> assertThat(scanCount()).isEqualTo(total));

    final LocalBucket bucket = (LocalBucket) database.getSchema().getType(TYPE).getBuckets(false).getFirst();
    // ~5 pages hold the records; one partly filled tail page per worker comes on top of that
    assertThat(bucket.getTotalPages()).as("pages used by %s small records", total).isLessThanOrEqualTo(12);
  }

  /**
   * The page an insert was turned away from is taken out of the free-space map, because the room its committed image
   * shows is promised to running transactions. When those give it back - here by rolling back - the page must be
   * listed again, not left unlisted until the next statistics gather. The release only queues it (transaction
   * teardown must not read pages); the next allocation measures it and lists the room it really has.
   */
  @Test
  void aPageTakenOutOfTheFreeSpaceMapIsListedAgainWhenItsReservationsAreReleased() {
    final LocalBucket bucket = (LocalBucket) database.getSchema().getType(TYPE).getBuckets(false).getFirst();
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.newDocument(TYPE).set("seq", i).save();
    });

    database.begin();
    final RID reserved = database.newDocument(TYPE).set("owner", "reserving").save().getIdentity();
    assertThat(reserved.getPosition() / bucket.getMaxRecordsInPage()).as("the insert reserved a slot on page 0").isZero();

    bucket.delistReservedPage(0);
    assertThat(bucket.getListedFreeSpacePagesForTesting()).doesNotContain(0);

    database.rollback();
    assertThat(bucket.getPagesToRelistForTesting()).as("page 0 queued once its reservations were released").containsExactly(0);

    database.begin();
    try {
      bucket.relistQueuedPagesForTesting();
    } finally {
      database.rollback();
    }
    assertThat(bucket.getPagesToRelistForTesting()).isEmpty();
    assertThat(bucket.getListedFreeSpacePagesForTesting()).as("page 0 after the next allocation measured it").contains(0);
    // Measured, not the whole page: ten small records are on it
    final int usableSpaceInPage = bucket.getPageSize() - BasePage.PAGE_HEADER_SIZE - bucket.contentHeaderSize;
    assertThat(bucket.getFreeSpaceHintForPage(0)).isPositive().isLessThan(usableSpaceInPage);
  }

  /** Counts by scanning the records: {@code countType} and {@code count(*)} read the bucket's cached counter. */
  private long scanCount() {
    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM " + TYPE)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }
}
