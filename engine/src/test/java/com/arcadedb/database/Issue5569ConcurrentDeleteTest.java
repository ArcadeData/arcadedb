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
import com.arcadedb.TestHelper;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5569 (follow-up to #5279 / #5381): a plain single-record DELETE was the last common false-conflict class on a
 * bucket page. Deleting record A poisoned the whole page, so every concurrent transaction that merely updated record B
 * on that page failed with a {@link ConcurrentModificationException} even though the two writes commute: a plain
 * in-place record is deleted by zeroing its slot-table entry (plus the optional content wipe-out), a single-slot
 * change exactly like the inserts and in-page updates the disjoint-slot merge already replays.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5569ConcurrentDeleteTest extends TestHelper {
  private boolean savedSlotMerge;

  @BeforeEach
  void saveConfig() {
    savedSlotMerge = GlobalConfiguration.TX_PAGE_SLOT_MERGE.getValueAsBoolean();
    GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(true);
  }

  @AfterEach
  void restoreConfig() {
    GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(savedSlotMerge);
  }

  /**
   * The core of the issue: N transactions delete their own victim while N others update their own survivor, all of
   * them on the SAME page, and everybody commits at the same moment with attempts=1. Nothing here is a real conflict,
   * so no transaction may fail: every victim must be gone and every survivor must hold its exact new value.
   */
  @Test
  void concurrentDeleteAndUpdateOnTheSamePageNeverConflict() throws Exception {
    final int pairs = 8;
    final RID[] victim = new RID[pairs];
    final RID[] survivor = new RID[pairs];

    database.transaction(() -> {
      database.getSchema().createDocumentType("Mix", 1).createProperty("tag", Type.STRING);
      for (int i = 0; i < pairs; i++) {
        victim[i] = database.newDocument("Mix").set("role", "victim").set("tag", "v" + i).save().getIdentity();
        survivor[i] = database.newDocument("Mix").set("role", "survivor").set("tag", "s" + i).save().getIdentity();
      }
    });
    assertThat(allOnTheSamePage("Mix", victim, survivor)).as("the fixture must live on one page").isTrue();

    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    final CyclicBarrier staged = new CyclicBarrier(pairs * 2);
    final CountDownLatch commitNow = new CountDownLatch(1);
    final AtomicInteger committed = new AtomicInteger();
    final List<Thread> threads = new ArrayList<>();

    for (int i = 0; i < pairs; i++) {
      final int n = i;

      threads.add(new Thread(() -> stagedTransaction(errors, staged, commitNow, committed, //
          () -> victim[n].asDocument(true).delete()), "deleter-" + n));

      threads.add(new Thread(() -> stagedTransaction(errors, staged, commitNow, committed, //
          () -> survivor[n].asDocument(true).modify().set("tag", "updated-" + n).save()), "updater-" + n));
    }

    for (final Thread thread : threads)
      thread.start();
    Thread.sleep(200);
    commitNow.countDown();
    for (final Thread thread : threads)
      thread.join();

    if (!errors.isEmpty())
      throw new AssertionError(errors.size() + " transaction(s) failed, first: " + errors.getFirst(), errors.getFirst());

    assertThat(committed.get()).isEqualTo(pairs * 2);
    // The merge is what made it possible: without it every commit but the first would have thrown.
    assertThat(((DatabaseInternal) database).getPageManager().getStats().txPageSlotMerges)
        .as("the disjoint-slot merge must fire").isGreaterThan(0);

    database.transaction(() -> {
      for (int i = 0; i < pairs; i++) {
        assertThat(survivor[i].asDocument(true).getString("tag")).isEqualTo("updated-" + i);
        assertThat(recordExists(victim[i])).as("victim " + i + " must be deleted").isFalse();
      }
      assertThat(database.countType("Mix", false)).isEqualTo(pairs);
    });
  }

  /**
   * Concurrent deletes of DIFFERENT records of one page: they commute with each other just as they do with an update,
   * so with attempts=1 every one of them must commit.
   */
  @Test
  void concurrentDeletesOfDifferentRecordsInTheSamePageNeverConflict() throws Exception {
    final int records = 10;
    final RID[] rids = new RID[records];

    database.transaction(() -> {
      database.getSchema().createDocumentType("Victims", 1).createProperty("tag", Type.STRING);
      for (int i = 0; i < records; i++)
        rids[i] = database.newDocument("Victims").set("tag", "t" + i).save().getIdentity();
    });
    assertThat(allOnTheSamePage("Victims", rids)).as("the fixture must live on one page").isTrue();

    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    final CyclicBarrier staged = new CyclicBarrier(records);
    final CountDownLatch commitNow = new CountDownLatch(1);
    final AtomicInteger committed = new AtomicInteger();
    final List<Thread> threads = new ArrayList<>();

    for (int i = 0; i < records; i++) {
      final int n = i;
      threads.add(new Thread(() -> stagedTransaction(errors, staged, commitNow, committed, //
          () -> rids[n].asDocument(true).delete()), "deleter-" + n));
    }

    for (final Thread thread : threads)
      thread.start();
    Thread.sleep(200);
    commitNow.countDown();
    for (final Thread thread : threads)
      thread.join();

    if (!errors.isEmpty())
      throw new AssertionError(errors.size() + " transaction(s) failed, first: " + errors.getFirst(), errors.getFirst());

    assertThat(committed.get()).isEqualTo(records);

    database.transaction(() -> {
      assertThat(database.countType("Victims", false)).isZero();
      for (int i = 0; i < records; i++)
        assertThat(recordExists(rids[i])).as("record " + i + " must be deleted").isFalse();
    });
  }

  /**
   * The other side of the contract: deleting a record another transaction is updating is a TRUE conflict, and the
   * byte-for-byte pre-image check must keep raising it. Whichever of the two commits first wins, the other one gets a
   * {@link ConcurrentModificationException} and the surviving state is exactly the winner's.
   */
  @Test
  void deletingARecordAnotherTransactionIsUpdatingStillConflicts() throws Exception {
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      database.getSchema().createDocumentType("Duel", 1).createProperty("tag", Type.STRING);
      rid[0] = database.newDocument("Duel").set("tag", "initial").save().getIdentity();
      // A co-located record nobody touches, so the page is shared exactly as in the mergeable cases above.
      database.newDocument("Duel").set("tag", "bystander").save();
    });

    final List<Throwable> unexpected = new CopyOnWriteArrayList<>();
    final CyclicBarrier staged = new CyclicBarrier(2);
    final CountDownLatch commitNow = new CountDownLatch(1);
    final AtomicInteger conflicts = new AtomicInteger();
    final boolean[] deleteWon = new boolean[1];
    final boolean[] updateWon = new boolean[1];

    final Thread deleter = new Thread(() -> {
      try {
        database.begin();
        rid[0].asDocument(true).delete();
        staged.await();
        commitNow.await();
        database.commit();
        deleteWon[0] = true;
      } catch (final ConcurrentModificationException e) {
        conflicts.incrementAndGet();
      } catch (final Throwable e) {
        unexpected.add(e);
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }
    }, "deleter");

    final Thread updater = new Thread(() -> {
      try {
        database.begin();
        rid[0].asDocument(true).modify().set("tag", "updated").save();
        staged.await();
        commitNow.await();
        database.commit();
        updateWon[0] = true;
      } catch (final ConcurrentModificationException e) {
        conflicts.incrementAndGet();
      } catch (final Throwable e) {
        unexpected.add(e);
      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }
    }, "updater");

    deleter.start();
    updater.start();
    Thread.sleep(200);
    commitNow.countDown();
    deleter.join();
    updater.join();

    if (!unexpected.isEmpty())
      throw new AssertionError("unexpected failure: " + unexpected.getFirst(), unexpected.getFirst());

    assertThat(conflicts.get()).as("two writes to the SAME record must conflict").isEqualTo(1);
    assertThat(deleteWon[0] ^ updateWon[0]).as("exactly one of the two must win").isTrue();

    database.transaction(() -> {
      if (deleteWon[0])
        assertThat(recordExists(rid[0])).as("the delete won: the record must be gone").isFalse();
      else
        assertThat(rid[0].asDocument(true).getString("tag")).isEqualTo("updated");
    });
  }

  /**
   * A record CREATED and deleted within the same transaction never existed on the committed page, so its net effect
   * there is nothing: the replay must skip the slot altogether instead of looking for a pre-image that cannot be
   * there - which would turn a perfectly mergeable transaction into a conflict.
   */
  @Test
  void insertAndDeleteOfTheSameRecordInOneTransactionStillMerges() throws Exception {
    final RID[] survivor = new RID[1];
    database.transaction(() -> {
      database.getSchema().createDocumentType("Ephemeral", 1).createProperty("tag", Type.STRING);
      survivor[0] = database.newDocument("Ephemeral").set("tag", "initial").save().getIdentity();
    });

    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    final CyclicBarrier staged = new CyclicBarrier(2);
    // The churner must commit LAST, so it is always the one forced to rebase its page: if it committed first there
    // would be no conflict to resolve and the test would prove nothing.
    final CountDownLatch updaterCommitted = new CountDownLatch(1);
    final AtomicInteger committed = new AtomicInteger();
    final RID[] ephemeral = new RID[1];

    final Thread churner = new Thread(() -> {
      try {
        database.begin();
        final MutableDocument doc = database.newDocument("Ephemeral").set("tag", "ephemeral");
        doc.save();
        ephemeral[0] = doc.getIdentity();
        doc.delete();
        staged.await();
        updaterCommitted.await();
        database.commit();
        committed.incrementAndGet();
      } catch (final Throwable e) {
        errors.add(e);
        if (database.isTransactionActive())
          database.rollback();
      }
    }, "churner");

    final Thread updater = new Thread(() -> {
      try {
        database.begin();
        survivor[0].asDocument(true).modify().set("tag", "updated").save();
        staged.await();
        database.commit();
        committed.incrementAndGet();
      } catch (final Throwable e) {
        errors.add(e);
        if (database.isTransactionActive())
          database.rollback();
      } finally {
        updaterCommitted.countDown();
      }
    }, "updater");

    churner.start();
    updater.start();
    churner.join();
    updater.join();

    if (!errors.isEmpty())
      throw new AssertionError(errors.size() + " transaction(s) failed, first: " + errors.getFirst(), errors.getFirst());

    assertThat(committed.get()).isEqualTo(2);
    // The created-then-deleted record must have landed on the very page the updater was writing to, otherwise the
    // test would not exercise the replay at all.
    assertThat(pageOf("Ephemeral", ephemeral[0])).isEqualTo(pageOf("Ephemeral", survivor[0]));

    database.transaction(() -> {
      assertThat(survivor[0].asDocument(true).getString("tag")).isEqualTo("updated");
      assertThat(recordExists(ephemeral[0])).as("the ephemeral record must not exist").isFalse();
      assertThat(database.countType("Ephemeral", false)).isEqualTo(1);
    });
  }

  /**
   * One transaction that inserts, updates AND deletes on the same page, replayed as a whole on top of a newer
   * committed version of it. The three kinds are replayed in the (unspecified) order of the tracked slot map, so this
   * is the guard that they do not depend on each other: the insert must land after the last live record whatever the
   * delete freed, and the delete must still find its own pre-image whatever the update shifted.
   */
  @Test
  void insertUpdateAndDeleteOfOneTransactionAreAllReplayedTogether() throws Exception {
    final int victims = 4;
    final RID[] victim = new RID[victims];
    final RID[] grower = new RID[victims];
    final RID[] outsider = new RID[1];

    database.transaction(() -> {
      database.getSchema().createDocumentType("AllKinds", 1).createProperty("tag", Type.STRING);
      outsider[0] = database.newDocument("AllKinds").set("tag", "outsider").save().getIdentity();
      for (int i = 0; i < victims; i++) {
        victim[i] = database.newDocument("AllKinds").set("tag", "victim-" + i).save().getIdentity();
        grower[i] = database.newDocument("AllKinds").set("tag", "grower-" + i).save().getIdentity();
      }
    });

    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    final CyclicBarrier staged = new CyclicBarrier(2);
    // The mixed transaction commits LAST, so it is always the one forced to rebase all of its slot writes at once.
    final CountDownLatch outsiderCommitted = new CountDownLatch(1);
    final AtomicInteger committed = new AtomicInteger();
    final RID[] inserted = new RID[victims];

    final Thread mixed = new Thread(() -> {
      try {
        database.begin();
        for (int i = 0; i < victims; i++) {
          inserted[i] = database.newDocument("AllKinds").set("tag", "inserted-" + i).save().getIdentity();
          grower[i].asDocument(true).modify().set("tag", "grower-" + i + "-now-a-much-longer-value").save();
          victim[i].asDocument(true).delete();
        }
        staged.await();
        outsiderCommitted.await();
        database.commit();
        committed.incrementAndGet();
      } catch (final Throwable e) {
        errors.add(e);
        if (database.isTransactionActive())
          database.rollback();
      }
    }, "mixed");

    final Thread other = new Thread(() -> {
      try {
        database.begin();
        outsider[0].asDocument(true).modify().set("tag", "outsider-updated").save();
        staged.await();
        database.commit();
        committed.incrementAndGet();
      } catch (final Throwable e) {
        errors.add(e);
        if (database.isTransactionActive())
          database.rollback();
      } finally {
        outsiderCommitted.countDown();
      }
    }, "outsider");

    mixed.start();
    other.start();
    mixed.join();
    other.join();

    if (!errors.isEmpty())
      throw new AssertionError(errors.size() + " transaction(s) failed, first: " + errors.getFirst(), errors.getFirst());

    assertThat(committed.get()).isEqualTo(2);

    database.transaction(() -> {
      assertThat(outsider[0].asDocument(true).getString("tag")).isEqualTo("outsider-updated");
      for (int i = 0; i < victims; i++) {
        assertThat(recordExists(victim[i])).as("victim " + i + " must be deleted").isFalse();
        assertThat(grower[i].asDocument(true).getString("tag")).isEqualTo("grower-" + i + "-now-a-much-longer-value");
        assertThat(inserted[i].asDocument(true).getString("tag")).isEqualTo("inserted-" + i);
      }
      assertThat(database.countType("AllKinds", false)).isEqualTo(1 + 2L * victims);
    });

    assertCheckDatabaseIsClean();
  }

  /**
   * A merged delete must remove the record from the indexes exactly like a non-merged one: the index changes are
   * transaction-level and do not depend on which page image is committed, and this is the regression guard for it.
   */
  @Test
  void aMergedDeleteStillRemovesTheIndexEntries() throws Exception {
    final int pairs = 6;
    final RID[] victim = new RID[pairs];
    final RID[] survivor = new RID[pairs];

    database.transaction(() -> {
      database.getSchema().createDocumentType("Indexed", 1).createProperty("k", Type.STRING);
      database.getSchema().getType("Indexed").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "k");
      for (int i = 0; i < pairs; i++) {
        victim[i] = database.newDocument("Indexed").set("k", "victim-" + i).save().getIdentity();
        survivor[i] = database.newDocument("Indexed").set("k", "survivor-" + i).save().getIdentity();
      }
    });

    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    final CyclicBarrier staged = new CyclicBarrier(pairs * 2);
    final CountDownLatch commitNow = new CountDownLatch(1);
    final AtomicInteger committed = new AtomicInteger();
    final List<Thread> threads = new ArrayList<>();

    for (int i = 0; i < pairs; i++) {
      final int n = i;
      threads.add(new Thread(() -> stagedTransaction(errors, staged, commitNow, committed, //
          () -> victim[n].asDocument(true).delete()), "deleter-" + n));
      threads.add(new Thread(() -> stagedTransaction(errors, staged, commitNow, committed, //
          () -> survivor[n].asDocument(true).modify().set("payload", "p" + n).save()), "updater-" + n));
    }

    for (final Thread thread : threads)
      thread.start();
    Thread.sleep(200);
    commitNow.countDown();
    for (final Thread thread : threads)
      thread.join();

    if (!errors.isEmpty())
      throw new AssertionError(errors.size() + " transaction(s) failed, first: " + errors.getFirst(), errors.getFirst());

    assertThat(committed.get()).isEqualTo(pairs * 2);

    database.transaction(() -> {
      for (int i = 0; i < pairs; i++) {
        try (final ResultSet rs = database.query("SQL", "SELECT FROM Indexed WHERE k = ?", "victim-" + i)) {
          assertThat(rs.hasNext()).as("index entry of victim " + i + " must be gone").isFalse();
        }
        try (final ResultSet rs = database.query("SQL", "SELECT FROM Indexed WHERE k = ?", "survivor-" + i)) {
          assertThat(rs.hasNext()).as("survivor " + i + " must still be indexed").isTrue();
          assertThat(rs.next().getIdentity().get()).isEqualTo(survivor[i]);
        }
      }
      // The unique index must accept the freed keys again.
      for (int i = 0; i < pairs; i++)
        database.newDocument("Indexed").set("k", "victim-" + i).save();
    });
  }

  /**
   * Deleting a record that does NOT live entirely in its own slot - here a multi-page one - is still not a single-slot
   * change, so it must keep poisoning the page and falling back to a normal retry. Co-located writes stay exact and
   * the record really disappears.
   */
  @Test
  void deletingAMultiPageRecordCoLocatedWithATrackedUpdateStillFallsBackCleanly() throws Exception {
    final RID[] big = new RID[1];
    final RID[] small = new RID[1];

    database.transaction(() -> {
      database.getSchema().createDocumentType("Huge", 1).createProperty("tag", Type.STRING);
      small[0] = database.newDocument("Huge").set("tag", "initial").save().getIdentity();
      // Far bigger than a page: the record is stored as a chunk chain whose head sits on the shared page.
      big[0] = database.newDocument("Huge").set("blob", "x".repeat(400_000)).save().getIdentity();
    });

    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    final CountDownLatch start = new CountDownLatch(1);

    final Thread deleter = new Thread(() -> {
      try {
        start.await();
        // Retries allowed: the point is that the fallback converges and never corrupts the page.
        database.transaction(() -> big[0].asDocument(true).delete(), true, 50);
      } catch (final Throwable e) {
        errors.add(e);
      }
    }, "deleter");

    final int[] lastCommitted = new int[1];
    final Thread updater = new Thread(() -> {
      try {
        start.await();
        for (int i = 1; i <= 200; i++) {
          final int n = i;
          try {
            database.transaction(() -> small[0].asDocument(true).modify().set("tag", "v" + n).save(), true, 1);
            lastCommitted[0] = n;
          } catch (final ConcurrentModificationException ignore) {
            // Expected while the delete holds the page poisoned.
          }
        }
      } catch (final Throwable e) {
        errors.add(e);
      }
    }, "updater");

    deleter.start();
    updater.start();
    start.countDown();
    deleter.join();
    updater.join();

    if (!errors.isEmpty())
      throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.getFirst(), errors.getFirst());

    database.transaction(() -> {
      assertThat(small[0].asDocument(true).getString("tag")).isEqualTo("v" + lastCommitted[0]);
      assertThat(recordExists(big[0])).as("the multi-page record must be deleted").isFalse();
      assertThat(database.countType("Huge", false)).isEqualTo(1);
    });

    assertCheckDatabaseIsClean();
  }

  /**
   * A long, contended mix of inserts, updates and deletes on a single-bucket type: whatever combination of merges,
   * poisoned pages and retries that produces, every record must hold exactly what its owner last wrote, every deleted
   * record must be gone and {@code check database} must come back clean.
   */
  @Test
  void contendedMixOfInsertsUpdatesAndDeletesKeepsTheDatabaseConsistent() throws Exception {
    final int threadCount = 6;
    final int roundsPerThread = 40;

    database.transaction(() -> database.getSchema().createDocumentType("Churn", 1).createProperty("tag", Type.STRING));

    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    final List<RID> survivors = new CopyOnWriteArrayList<>();
    final CountDownLatch start = new CountDownLatch(1);
    final List<Thread> threads = new ArrayList<>();

    for (int t = 0; t < threadCount; t++) {
      final int id = t;
      threads.add(new Thread(() -> {
        try {
          start.await();
          for (int round = 0; round < roundsPerThread; round++) {
            final int r = round;
            final RID[] created = new RID[1];

            // Retries stay on (this is a consistency guard, not a no-conflict one): whatever mix of merges and
            // retries the contention produces, the end state must be exact.
            // INSERT
            database.transaction(() -> created[0] = database.newDocument("Churn").set("owner", id)//
                .set("tag", "t" + id + "-r" + r).save().getIdentity(), true, 10);

            // UPDATE (a growth, the normal shape)
            database.transaction(() -> created[0].asDocument(true).modify()//
                .set("tag", "t" + id + "-r" + r + "-updated-and-padded").save(), true, 10);

            // DELETE every other one, keep the rest as survivors with a known final value
            if (r % 2 == 0)
              database.transaction(() -> created[0].asDocument(true).delete(), true, 10);
            else
              survivors.add(created[0]);
          }
        } catch (final Throwable e) {
          errors.add(e);
        }
      }, "churn-" + t));
    }

    for (final Thread thread : threads)
      thread.start();
    start.countDown();
    for (final Thread thread : threads)
      thread.join();

    if (!errors.isEmpty())
      throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.getFirst(), errors.getFirst());

    database.transaction(() -> {
      assertThat(database.countType("Churn", false)).isEqualTo(survivors.size());
      for (final RID rid : survivors)
        assertThat(rid.asDocument(true).getString("tag")).endsWith("-updated-and-padded");
    });

    assertCheckDatabaseIsClean();
  }

  /**
   * Runs {@code body} in an explicit transaction that waits, right before committing, until every other participant
   * has staged its own write - so all the commits race on the very same page version, with NO retry (attempts are
   * irrelevant here: {@code commit()} is called once).
   */
  private void stagedTransaction(final List<Throwable> errors, final CyclicBarrier staged, final CountDownLatch commitNow,
      final AtomicInteger committed, final Runnable body) {
    try {
      database.begin();
      body.run();
      staged.await();
      commitNow.await();
      database.commit();
      committed.incrementAndGet();
    } catch (final Throwable e) {
      errors.add(e);
      if (database.isTransactionActive())
        database.rollback();
    }
  }

  private boolean recordExists(final RID rid) {
    try {
      return rid.asDocument(true) != null;
    } catch (final RecordNotFoundException e) {
      return false;
    }
  }

  private int pageOf(final String typeName, final RID rid) {
    final LocalBucket bucket = (LocalBucket) database.getSchema().getType(typeName).getBuckets(false).getFirst();
    return (int) (rid.getPosition() / bucket.getMaxRecordsInPage());
  }

  @SafeVarargs
  private boolean allOnTheSamePage(final String typeName, final RID[]... groups) {
    int page = -1;
    for (final RID[] group : groups)
      for (final RID rid : group) {
        final int p = pageOf(typeName, rid);
        if (page == -1)
          page = p;
        else if (page != p)
          return false;
      }
    return true;
  }

  private void assertCheckDatabaseIsClean() {
    try (final ResultSet rs = database.command("SQL", "check database")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(numberProperty(row, "totalErrors")).as("check database: " + row.toJSON()).isZero();
        assertThat(numberProperty(row, "autoFix")).as("check database: " + row.toJSON()).isZero();
      }
    }
  }

  private static long numberProperty(final Result row, final String name) {
    final Object value = row.getProperty(name);
    return value instanceof Number n ? n.longValue() : 0L;
  }
}
