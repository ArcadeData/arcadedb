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
import com.arcadedb.schema.LocalSchema;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #9835: a schema reload builds a NEW {@link LocalBucket} for the same file, and that instance
 * used to start its page-reservation state from scratch - the reservation counter re-seeded from the file size, and
 * empty maps of the pages and slots running transactions own. A transaction that was writing a new page (or a slot of
 * an existing page) when the reload ran then saw a later transaction handed the very same page number, slot and RID:
 * the application read back a RID that belonged to someone else, and the first of the two to commit made the other
 * fail with a {@code ConcurrentModificationException} for a conflict that does not exist.
 * <p>
 * The invariant pinned here: a reload never lets a later transaction obtain a page number, or a RID, that a
 * transaction still running at the time of the reload already holds.
 */
class Issue9835SchemaReloadKeepsPageReservationsTest extends TestHelper {

  private static final String TYPE = "Issue9835Item";

  @Override
  protected void beginTest() {
    database.getSchema().buildDocumentType().withName(TYPE).withTotalBuckets(1).create();
  }

  /**
   * The issue's repro: the first record of an empty bucket makes its transaction create page 0, which stays in flight
   * across the reload. The reload used to re-seed the counter to 0 and forget the page's owner, so the next insert
   * created page 0 too and got the same RID.
   */
  @Test
  void aReloadDoesNotReissueAPageCreatedByARunningTransaction() throws Exception {
    final LocalBucket before = bucket();

    final InFlightWriter writer = new InFlightWriter(1);
    writer.start();
    writer.awaitWritten();

    // Not vacuous: the writer really holds a brand-new page across the reload
    assertThat(before.getInFlightNewPagesForTesting()).isEqualTo(1);
    final int reservedBefore = before.reservedPageCounter.get();
    assertThat(reservedBefore).isGreaterThan(before.getCommittedPageCount());

    reloadSchema();

    final LocalBucket after = bucket();
    assertThat(after).as("the full load must have replaced the bucket instance").isNotSameAs(before);
    assertThat(after.reservedPageCounter.get()).as("reservation counter after the reload").isGreaterThanOrEqualTo(reservedBefore);
    assertThat(after.getInFlightNewPagesForTesting()).as("pages in flight after the reload").isEqualTo(1);
    // SHARED, not copied: a transaction still allocating through the retired instance draws from the same sequence
    assertThat(after.reservedPageCounter).as("the reservation counter").isSameAs(before.reservedPageCounter);

    final List<RID> laterRids = insertAndCommit(3);

    writer.commitAndJoin();

    final int maxRecordsInPage = after.getMaxRecordsInPage();
    final long writerPage = writer.rids.getFirst().getPosition() / maxRecordsInPage;
    for (final RID rid : laterRids) {
      assertThat(rid).as("a later transaction was given the writer's RID").isNotIn(writer.rids);
      assertThat(rid.getPosition() / maxRecordsInPage).as("record %s must not be on the writer's page", rid).isNotEqualTo(writerPage);
    }

    // The writer's own release reaches the instance that is live now, so its page is not held forever
    assertThat(after.getInFlightNewPagesForTesting()).isZero();

    assertAllReadable(writer.rids, "writer");
    assertAllReadable(laterRids, "later");
    database.transaction(() -> assertThat(database.countType(TYPE, false)).isEqualTo(1 + laterRids.size()));
  }

  /**
   * The same failure one level down: a transaction inserting into an EXISTING page reserves its slot (#5279), and the
   * reservation used to live only in the retired instance, so a later transaction claimed the same slot of the same
   * page after the reload.
   */
  @Test
  void aReloadDoesNotReissueASlotReservedByARunningTransaction() throws Exception {
    database.transaction(() -> database.newDocument(TYPE).set("owner", "seed").save());

    final LocalBucket before = bucket();
    final InFlightWriter writer = new InFlightWriter(1);
    writer.start();
    writer.awaitWritten();

    // Not vacuous: the writer went into the committed page 0, not into a page of its own
    assertThat(before.getInFlightNewPagesForTesting()).isZero();
    assertThat(writer.rids.getFirst().getPosition() / before.getMaxRecordsInPage()).isZero();

    reloadSchema();
    assertThat(bucket()).as("the full load must have replaced the bucket instance").isNotSameAs(before);

    final List<RID> laterRids = insertAndCommit(1);

    writer.commitAndJoin();

    for (final RID rid : laterRids)
      assertThat(rid).as("a later transaction was given the writer's RID").isNotIn(writer.rids);

    assertAllReadable(writer.rids, "writer");
    assertAllReadable(laterRids, "later");
    database.transaction(() -> assertThat(database.countType(TYPE, false)).isEqualTo(2 + laterRids.size()));
  }

  /** A second reload while the same transaction is still running must not lose what the first one carried over. */
  @Test
  void twoReloadsInARowStillKeepTheReservation() throws Exception {
    final InFlightWriter writer = new InFlightWriter(1);
    writer.start();
    writer.awaitWritten();

    reloadSchema();
    reloadSchema();

    assertThat(bucket().getInFlightNewPagesForTesting()).isEqualTo(1);
    final List<RID> laterRids = insertAndCommit(2);
    writer.commitAndJoin();

    for (final RID rid : laterRids)
      assertThat(rid).isNotIn(writer.rids);
    assertThat(bucket().getInFlightNewPagesForTesting()).isZero();
    database.transaction(() -> assertThat(database.countType(TYPE, false)).isEqualTo(1 + laterRids.size()));
  }

  private LocalBucket bucket() {
    return (LocalBucket) database.getSchema().getType(TYPE).getBuckets(false).getFirst();
  }

  private void reloadSchema() throws Exception {
    ((LocalSchema) database.getSchema().getEmbedded()).load(ComponentFile.MODE.READ_WRITE, true);
  }

  private List<RID> insertAndCommit(final int count) {
    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        rids.add(database.newDocument(TYPE).set("owner", "later").set("seq", i).save().getIdentity());
    });
    return rids;
  }

  private void assertAllReadable(final List<RID> rids, final String owner) {
    database.transaction(() -> {
      for (final RID rid : rids)
        assertThat(database.lookupByRID(rid, true).asDocument().getString("owner")).as("owner of %s", rid).isEqualTo(owner);
    });
  }

  /** A transaction that inserts, then stays open until told to commit. */
  private final class InFlightWriter extends Thread {
    private final int                        records;
    private final CountDownLatch             written   = new CountDownLatch(1);
    private final CountDownLatch             mayCommit = new CountDownLatch(1);
    private final AtomicReference<Throwable> failure   = new AtomicReference<>();
    final         List<RID>                  rids      = new ArrayList<>();

    InFlightWriter(final int records) {
      this.records = records;
      setName("issue9835-writer");
    }

    @Override
    public void run() {
      try {
        database.begin();
        for (int i = 0; i < records; i++) {
          final MutableDocument doc = database.newDocument(TYPE).set("owner", "writer").set("seq", i);
          doc.save();
          rids.add(doc.getIdentity());
        }
        written.countDown();
        assertThat(mayCommit.await(30, TimeUnit.SECONDS)).isTrue();
        database.commit();
      } catch (final Throwable e) {
        failure.set(e);
      } finally {
        written.countDown();
        if (database.isTransactionActive())
          database.rollback();
      }
    }

    void awaitWritten() throws InterruptedException {
      assertThat(written.await(30, TimeUnit.SECONDS)).as("the writer did not write").isTrue();
      assertThat(failure.get()).as("the writer failed before the reload").isNull();
    }

    void commitAndJoin() throws InterruptedException {
      mayCommit.countDown();
      join(60_000);
      assertThat(isAlive()).as("the writer hung").isFalse();
      assertThat(failure.get()).as("the writer transaction, which held its reservation across the reload").isNull();
    }
  }
}
