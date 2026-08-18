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
package com.arcadedb.database;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * #6320 - what one run of {@code CHECK DATABASE FIX} on a bucket may hold in memory.
 * <p>
 * #6294 gave the orphaned-chunk sweep of {@code LocalBucket.check(fix)} a memory budget, because the enclosing
 * transaction keeps a copy of every page it modifies and a large backlog freed in one transaction is how a repair
 * turns into the {@code OutOfMemoryError} of #4653. The RECORD repairs of the very same method - four force-deletes in
 * the per-slot loop, each taking its record's page plus every page its chain touches - were bounded by nothing, and
 * reached the same failure through the other loop.
 * <p>
 * The answer is not a second budget. What is scarce is ONE pool, so two bounds over it cannot both be right; and a
 * repair that stops when the pool is spent leaves an operator running {@code FIX} over and over to converge. The
 * mechanism that gives the pool back already existed one class over - {@code GraphDatabaseChecker} has committed its
 * graph repairs in batches of {@link GlobalConfiguration#CHECK_DATABASE_REPAIR_BATCH_PAGES} pages since #6128 - and
 * the bucket repairs now use it, all of them, so one run repairs everything with the memory bounded throughout.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6320BucketRepairBatchTest extends BucketPageLayoutTestSupport {
  private static final String TYPE      = "Damaged";
  /** A type the repair never touches, so the caller's own work sits on pages of its own. */
  private static final String BYSTANDER = "Bystander";
  /** Enough broken records, spread over enough pages, that a small page budget really is crossed several times. */
  private static final int    RECORDS = 24;
  /** {@code LocalBucket.FIRST_CHUNK} (-2), as the single zigzag byte it is stored as. */
  private static final byte   FIRST_CHUNK_MARKER = 3;

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // The fixture injects on-disk corruption on purpose; each test repairs it and checks the result itself.
    return false;
  }

  /**
   * The whole point: a repair whose page footprint exceeds the budget is split across several transactions AND still
   * repairs everything the bucket has wrong - the records and the chunks their deletion orphans - in that one run.
   */
  @Test
  void aBucketRepairBiggerThanTheBudgetCommitsInBatchesAndStillFinishes() {
    final List<RID> broken = brokenChunkChains();

    GlobalConfiguration.CHECK_DATABASE_REPAIR_BATCH_PAGES.setValue(2);

    final Map<String, Object>[] stats = new Map[1];
    final int commits = countCommitsDuring(() -> stats[0] = bucketOf(TYPE).check(0, true));

    assertThat(commits).as("a repair bigger than the page budget must not be one single transaction").isGreaterThan(1);

    assertThat((Long) stats[0].get("orphanedChunksReclaimed"))
        .as("every chunk the deleted records orphaned goes in the SAME run, where the sweep's own budget used to "
            + "leave a backlog for the next one: " + stats[0])
        .isEqualTo((Long) stats[0].get("orphanedChunks")).isPositive();

    assertRepaired(broken);
  }

  /**
   * The budget disabled restores the historical single all-or-nothing transaction, memory cost included, exactly as
   * it does for the graph repairs. An embedded user who wants those semantics can still have them.
   */
  @Test
  void theBatchBudgetCanBeDisabled() {
    final List<RID> broken = brokenChunkChains();

    GlobalConfiguration.CHECK_DATABASE_REPAIR_BATCH_PAGES.setValue(0);

    final int commits = countCommitsDuring(() -> bucketOf(TYPE).check(0, true));

    assertThat(commits).as("with the budget disabled the whole bucket repair is one transaction").isEqualTo(1);

    assertRepaired(broken);
  }

  /**
   * A read-only check must keep the caller's transaction and its view: only the repairs own a transaction of their
   * own. Pinned because the ownership is what makes the batching safe, and getting it wrong would silently commit
   * whatever the caller was in the middle of.
   */
  @Test
  void aCheckWithoutFixLeavesTheCallersTransactionAlone() {
    brokenChunkChains();

    database.begin();
    try {
      final RID uncommitted = database.newDocument(TYPE).set("payload", "u").save().getIdentity();

      bucketOf(TYPE).check(0, false);

      assertThat(database.isTransactionActive()).as("the caller's transaction must still be open").isTrue();
      assertThat(uncommitted.asDocument(true).getString("payload"))
          .as("and must still see what it had not committed").isEqualTo("u");
    } finally {
      database.rollback();
    }
  }

  /**
   * The failure path, and the reason the run tracks whether it still OWNS a transaction rather than asking
   * {@code database.isTransactionActive()} (PR review on #6320).
   * <p>
   * {@code LocalDatabase.commit()} pops the transaction context in a {@code finally}, whether or not the write
   * succeeded, so a batch commit that throws leaves this run holding nothing - and what is on the thread from that
   * moment is the CALLER's transaction, which through HTTP is always there. A cleanup that asked "is a transaction
   * active?" would answer yes and roll THAT one back: the other buckets a {@code CHECK DATABASE FIX} had already
   * repaired into it, and whatever else the caller was holding.
   * <p>
   * The failure simulated is the one batching exists to survive in the first place, taken from
   * {@code CheckDatabaseRepairBatchFailureTest}: a commit rejected from inside the WAL-write callback.
   */
  @Test
  void aFailedBatchCommitLeavesTheCallersTransactionAlone() {
    brokenChunkChains();

    // A type of its own for the caller's work, so its pages cannot overlap the ones the repair rewrites and the
    // assertion below is about transaction ownership and nothing else.
    database.transaction(() -> database.getSchema().createDocumentType(BYSTANDER, 1));

    GlobalConfiguration.CHECK_DATABASE_REPAIR_BATCH_PAGES.setValue(1);

    final DatabaseInternal db = (DatabaseInternal) database;
    final AtomicInteger commits = new AtomicInteger();
    // Fails the SECOND batch commit, so the run has genuinely committed one batch before it breaks.
    final Callable<Void> failSecondCommit = () -> {
      if (commits.incrementAndGet() == 2)
        throw new IllegalStateException("simulated replicated-entry rejection");
      return null;
    };

    database.begin();
    try {
      final RID callersOwn = database.newDocument(BYSTANDER).set("payload", "caller").save().getIdentity();

      db.registerCallback(DatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE, failSecondCommit);
      try {
        assertThatThrownBy(() -> bucketOf(TYPE).check(0, true))
            .as("a batch that cannot commit must reach the caller, not be reported as a completed repair")
            .isInstanceOf(Exception.class);
      } finally {
        db.unregisterCallback(DatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE, failSecondCommit);
      }

      assertThat(commits.get()).as("the failure must have happened mid-repair, not before it started")
          .isGreaterThanOrEqualTo(2);

      assertThat(database.isTransactionActive())
          .as("the caller's transaction must survive a repair that failed inside a transaction of its own").isTrue();
      assertThat(callersOwn.asDocument(true).getString("payload"))
          .as("with the work it was holding still in it").isEqualTo("caller");
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }
  }

  /**
   * The other thing batching changes about a repair, and the reason the record counter is invalidated at the START of
   * a fix run and not only at its end (PR review on #6320).
   * <p>
   * {@code count(*)} answers from {@code LocalBucket}'s O(1) counter without reading a page, and the records a repair
   * removes go through {@code deleteRecordInternal}, which deliberately registers no bucket delta for the commit-time
   * fold to apply. While a repair was ONE transaction that did not matter: nothing it did was durable until the end,
   * and the invalidation at the end became visible with it. Batching makes every batch durable as it commits, so a
   * counter invalidated only at the end goes on serving the PRE-repair number for as long as the repair takes - and
   * for good, if the run never reaches its end.
   * <p>
   * That last case is what this pins, because it needs no concurrency to observe: a run that commits a batch and then
   * fails leaves records genuinely gone and a counter that never heard about it.
   */
  @Test
  void aRepairThatCommittedABatchDoesNotLeaveTheRecordCounterStale() {
    brokenChunkChains();

    final long countedBefore = countRecordsFromCounter(TYPE);
    assertThat(countedBefore).as("the counter must be populated before the repair starts").isEqualTo(RECORDS);

    GlobalConfiguration.CHECK_DATABASE_REPAIR_BATCH_PAGES.setValue(1);

    final DatabaseInternal db = (DatabaseInternal) database;
    final AtomicInteger commits = new AtomicInteger();
    final Callable<Void> failSecondCommit = () -> {
      if (commits.incrementAndGet() == 2)
        throw new IllegalStateException("simulated replicated-entry rejection");
      return null;
    };

    db.registerCallback(DatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE, failSecondCommit);
    try {
      assertThatThrownBy(() -> bucketOf(TYPE).check(0, true)).isInstanceOf(Exception.class);
    } finally {
      db.unregisterCallback(DatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE, failSecondCommit);
    }

    // Ground truth from the slot walk rather than from a scan: the records this run did NOT get to are still broken,
    // and reading one throws. count() counts exactly these three categories.
    final Map<String, Object> layout = bucketStats(TYPE);
    final long reallyThere = (Long) layout.get("totalActiveRecords") + (Long) layout.get("totalPlaceholderRecords")
        + (Long) layout.get("totalMultiPageRecords");

    assertThat(reallyThere).as("the batch that committed really did remove records: " + layout)
        .isLessThan(countedBefore);
    assertThat(countRecordsFromCounter(TYPE))
        .as("count(*) must not go on answering from a counter the committed batch made wrong: " + layout)
        .isEqualTo(reallyThere);
  }

  /** Every broken record is gone, no orphan is left, and a second run finds nothing to do. */
  private void assertRepaired(final List<RID> broken) {
    final Map<String, Object> after = bucketStats(TYPE);
    assertThat((Long) after.get("totalErrors")).as("nothing may be left to repair: " + after).isZero();
    assertThat((Long) after.get("orphanedChunks")).as("and nothing left leaked: " + after).isZero();

    database.transaction(() -> {
      for (final RID rid : broken)
        assertThat(bucketOf(TYPE).existsRecord(rid)).as("broken record %s must have been removed", rid).isFalse();
    });

    final Result row = checkDatabaseRow(false);
    assertThat(numberProperty(row, "totalErrors")).as("and the database checks out: " + row.toJSON()).isZero();
  }

  /**
   * {@link #RECORDS} records big enough to spill into a chunk chain, each with its head chunk pointing at a page that
   * does not exist. {@code check(fix)} force-deletes such a record - freeing its head slot, which is one repair - and
   * the chunks it can no longer reach then fall to the orphan sweep, which is the other. One fixture, both kinds.
   */
  private List<RID> brokenChunkChains() {
    // A SMALLER page size, set before the bucket is created: the budget counts PAGES, and at the 64KB default this
    // fixture would pack into too few of them for any assertion about batching to mean anything.
    GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.setValue(16_384);

    final List<RID> rids = new ArrayList<>(RECORDS);
    database.transaction(() -> {
      database.getSchema().createDocumentType(TYPE, 1).createProperty("payload", Type.STRING);
      for (int i = 0; i < RECORDS; i++)
        rids.add(database.newDocument(TYPE).set("payload", "r" + i + "-" + "p".repeat(40_000)).save().getIdentity());
    });

    final Map<String, Object> layout = bucketStats(TYPE);
    assertThat((Long) layout.get("totalMultiPageRecords")).as("every fixture record must be a chain: " + layout)
        .isEqualTo(RECORDS);

    for (final RID rid : rids)
      breakChainOf(rid);

    return rids;
  }

  /**
   * Points the head chunk of {@code rid} at a page far past the end of the file, which is the break
   * {@code findBrokenChunkChain} reports as "next chunk pointer out of range". The chunks behind it keep their slots
   * and become unreachable the moment the head is deleted.
   */
  private void breakChainOf(final RID rid) {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int bucketPageSize = ((PaginatedComponentFile) db.getFileManager().getFile(rid.getBucketId())).getPageSize();
    final int maxRecordsInPage = bucketOf(TYPE).getMaxRecordsInPage();
    final int pageNumber = (int) (rid.getPosition() / maxRecordsInPage);

    database.transaction(() -> {
      try {
        final MutablePage page = db.getTransaction()
            .getPageToModify(new PageId(db, rid.getBucketId(), pageNumber), bucketPageSize, false);
        final int recordOffset = recordOffsetOf(page, rid);
        assertThat(page.readByte(recordOffset)).as("record %s must be a chunk head", rid).isEqualTo(FIRST_CHUNK_MARKER);
        // [marker:1][chunkSize:int][nextChunkPointer:long][content...]
        page.writeLong(recordOffset + 1 + Binary.INT_SERIALIZED_SIZE, Integer.MAX_VALUE);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  /** Counts the transactions that actually wrote WAL while {@code block} ran. */
  private int countCommitsDuring(final Runnable block) {
    final AtomicInteger commits = new AtomicInteger();
    final DatabaseInternal db = (DatabaseInternal) database;
    final Callable<Void> counter = () -> {
      commits.incrementAndGet();
      return null;
    };
    db.registerCallback(DatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE, counter);
    try {
      block.run();
    } finally {
      db.unregisterCallback(DatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE, counter);
    }
    return commits.get();
  }
}
