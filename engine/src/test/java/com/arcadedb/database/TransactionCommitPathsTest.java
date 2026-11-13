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

import com.arcadedb.TestHelper;
import com.arcadedb.database.TransactionIndexContext.ComparableKey;
import com.arcadedb.database.TransactionIndexContext.IndexKey.IndexKeyOperation;
import com.arcadedb.database.TransactionIndexContext.IndexKey;
import com.arcadedb.database.async.DatabaseAsyncExecutorImpl;
import com.arcadedb.engine.DatabaseChecker;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests of the commit paths of a transaction (first phase, replication position, lanes, publishing). Each nested class keeps the regression of one reported issue (see the Javadoc of each).
 */
public class TransactionCommitPathsTest {
  /**
   * Issue #6964, unit-level pin: {@code TransactionContext.commit1stPhase(boolean isLeader)} must replay the
   * transaction's queued index operations regardless of {@code isLeader}. This is the exact defect - on a Raft
   * replica originating its own commit, {@code isLeader} is {@code false} while the record itself still needs to be
   * committed, and the index replay was wrongly gated on that flag - but it is pinned here directly on
   * {@code TransactionContext}, without spinning up a Raft cluster, so a regression fails fast in the ordinary
   * engine unit-test lane instead of only in the slower {@code ha-raft} integration test
   * ({@code ReplicaInsertUniqueIndexInvisibleIT}) that exercises the same invariant end to end over a real cluster.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class NonLeaderCommit1stPhaseIndexReplay extends TestHelper {

    private static final String TYPE = "Issue6964Singleton";

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createDocumentType(TYPE).createProperty("name", Type.STRING);
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE, "name");
      });
    }

    @Test
    void nonLeaderCommit1stPhaseStillReplaysQueuedIndexChanges() {
      database.begin();
      database.newDocument(TYPE).set("name", "value").save();

      final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
      final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(false);

      assertThat(tx.isIndexChangesReplayed())
          .as("#6964: index replay must not depend on isLeader - it is what makes a replica's own WAL bytes carry "
              + "its index pages")
          .isTrue();

      tx.commit2ndPhase(phase1);

      database.begin();
      assertThat(database.lookupByKey(TYPE, "name", "value").hasNext())
          .as("the index entry queued before commit1stPhase(false) must be visible after commit")
          .isTrue();
      database.commit();
    }
  }

  /**
   * Regression test for the aliasing defect found in the review of PR #8001, on the O(1) lane lookup issue #7967
   * added.
   * <p>
   * An index's lanes are held in an {@link java.util.IdentityHashMap} keyed by the index, precisely so that WHICH
   * index a lane belongs to is decided by identity. The per-index value, though, is a plain {@code List} of
   * {@code TreeMap}s - and {@code TreeMap} inherits {@code equals()} from {@code AbstractMap}, so it compares by
   * CONTENT, and two empty ones are always equal. Dropping a retracted lane with {@code List.remove(Object)}
   * therefore removed the first CONTENT-equal lane, which is not necessarily the one being retracted.
   * <p>
   * One index owns more than one lane whenever it renames itself mid-transaction, which an {@code LSM_VECTOR} index
   * does when a compaction swaps in the component file it is named after (issue #6105): the next write opens a second
   * lane under the new name, and {@code commit()} replays both. Evicting the wrong one leaves that lane queued and
   * still replayed at commit, but unreachable through the lookup - so a read-your-own-writes search answers with part
   * of the transaction's own writes missing, silently.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class LaneIdentity {

    @Test
    void retractingOneLaneOfAnIndexLeavesTheOtherOneReachable() {
      final TransactionIndexContext changes = new TransactionIndexContext(null);
      final IndexInternal index = nonUniqueIndex("Doc_0_first");

      // Lane one, under the name the index answers to now.
      changes.addIndexKeyLock(index, IndexKeyOperation.ADD, new Object[] { "k1" }, new RID(1, 1));
      assertThat(changes.getIndexKeyLanes(index)).as("precondition: one lane so far").hasSize(1);

      // The rename a compaction performs, and the second lane the next write opens under the new name.
      Mockito.when(index.getName()).thenReturn("Doc_0_second");
      changes.addIndexKeyLock(index, IndexKeyOperation.ADD, new Object[] { "k2" }, new RID(1, 2));
      assertThat(changes.getIndexKeyLanes(index)).as("the rename must have opened a second lane").hasSize(2);

      // Retract a third entry that lands alone on the SECOND lane's key. Undoing it empties that key, which empties
      // no lane - so nothing is dropped and both lanes must survive.
      changes.armRecordUndo();
      changes.addIndexKeyLock(index, IndexKeyOperation.ADD, new Object[] { "k3" }, new RID(1, 3));
      changes.undoRecordChanges();
      changes.disarmRecordUndo();

      assertThat(changes.getIndexKeyLanes(index))
          .as("retracting an entry must not cost the index either of its lanes")
          .hasSize(2);
    }

    /**
     * The shape that actually got it wrong: two lanes of one index holding the SAME content, and the LATER one
     * dropped by name. {@code List.remove(Object)} scans from the front and removes the first CONTENT-equal element,
     * which here is the lane that was NOT being dropped - so the surviving lane became unreachable through the
     * lookup while still sitting in {@code indexEntries} and still being replayed by the commit, and the dropped
     * one stayed reachable through it.
     * <p>
     * Two lanes carry identical content whenever the same record is written, the index renames itself, and the record
     * is written again - a re-embedding across a compaction, which is exactly the sequence issue #6105 is about.
     */
    @Test
    void droppingOneLaneByNameLeavesAnIdenticalSiblingLaneReachable() {
      final TransactionIndexContext changes = new TransactionIndexContext(null);
      final IndexInternal index = nonUniqueIndex("Doc_0_first");

      final RID rid = new RID(1, 1);
      changes.addIndexKeyLock(index, IndexKeyOperation.ADD, new Object[] { "k1" }, rid);

      // The rename, and the SAME record written again: the second lane ends up content-equal to the first.
      Mockito.when(index.getName()).thenReturn("Doc_0_second");
      changes.addIndexKeyLock(index, IndexKeyOperation.ADD, new Object[] { "k1" }, rid);

      final var lanes = changes.getIndexKeyLanes(index);
      assertThat(lanes).as("precondition: two lanes").hasSize(2);
      assertThat(lanes.get(0)).as("...and they are content-equal, which is what made this go wrong").isEqualTo(lanes.get(1));
      final var firstLane = lanes.get(0);

      // The SECOND lane is dropped by name. A content-equality removal scans from the front and takes the FIRST
      // match, which is the other lane - the one that has to survive.
      changes.removeIndex("Doc_0_second");

      // isSameAs, not containsExactly: the two lanes are EQUAL, so an equality assertion cannot tell them apart -
      // which is the same reason List.remove(Object) could not, and is the whole defect.
      final var remaining = changes.getIndexKeyLanes(index);
      assertThat(remaining).hasSize(1);
      assertThat(remaining.getFirst())
          .as("the lane dropped by name is the one that goes, not whichever happened to look like it")
          .isSameAs(firstLane);
      assertThat(changes.getTotalEntries())
          .as("and the surviving lane's entry is still queued for the commit to replay")
          .isEqualTo(1);
    }

    /**
     * The commit picks one winner per RID across ALL of an index's lanes, not one per lane.
     * <p>
     * Raised by CodeRabbit on PR #8001. An index that renames itself mid-transaction owns a lane under each name, and
     * a record rewritten either side of that rename has an entry in both. Deduplicating per lane hands the batch a
     * winner from each - which is exactly the duplicate-embedding defect issue #7971 exists to close, reintroduced
     * through a second lane. The survivor has to be chosen by write order across the lanes.
     */
    @Test
    void theWinnerPerRidIsChosenAcrossEveryLaneOfTheIndex() {
      final TransactionIndexContext changes = new TransactionIndexContext(null);
      final IndexInternal index = nonUniqueIndex("Doc_0_first");
      final RID rid = new RID(1, 1);

      changes.addIndexKeyLock(index, IndexKeyOperation.ADD, new Object[] { "v1" }, rid);

      Mockito.when(index.getName()).thenReturn("Doc_0_second");
      changes.addIndexKeyLock(index, IndexKeyOperation.ADD, new Object[] { "v2" }, rid);

      final var lanes = changes.getIndexKeyLanes(index);
      assertThat(lanes).as("precondition: the rename opened a second lane").hasSize(2);

      // One entry in each lane, for one RID. The later write is the one on the second lane.
      final IndexKey first = onlyEntryOf(lanes.get(0));
      final IndexKey second = onlyEntryOf(lanes.get(1));
      assertThat(second.sequence)
          .as("the entry on the lane opened after the rename must carry the later write order")
          .isGreaterThan(first.sequence);
    }

    private static IndexKey onlyEntryOf(final java.util.TreeMap<ComparableKey, Map<IndexKey, IndexKey>> lane) {
      assertThat(lane).hasSize(1);
      final Map<IndexKey, IndexKey> values = lane.firstEntry().getValue();
      assertThat(values).hasSize(1);
      return values.values().iterator().next();
    }

    private static IndexInternal nonUniqueIndex(final String name) {
      final IndexInternal index = Mockito.mock(IndexInternal.class);
      Mockito.when(index.getName()).thenReturn(name);
      Mockito.when(index.isUnique()).thenReturn(false);
      Mockito.when(index.isTransactionKeyOrderRequired()).thenReturn(true);
      Mockito.when(index.getNullStrategy()).thenReturn(LSMTreeIndexAbstract.NULL_STRATEGY.ERROR);
      return index;
    }
  }

  /**
   * Regression tests for issue #8292: engine code that commits on the inner {@link LocalDatabase} instead of the
   * database's current wrapper. On an HA node the wrapper is the Raft-replicated database, so a commit made on the inner
   * instance applies its pages on this node only and replicates nothing (the #5492 family).
   * <p>
   * The wrapper here is a delegating proxy that counts the {@code commit()} calls it receives, installed the same way the
   * HA plugin installs {@code RaftReplicatedDatabase} ({@link LocalDatabase#setWrappedDatabaseInstance}). Each test drives
   * one entry point that used to commit on the inner instance and asserts the commits now reach the wrapper.
   * <p>
   * The proxy counts every {@code commit()} it receives, nested ones included (a dictionary registration commits in its
   * own nested transaction). Every fixture therefore writes its property names BEFORE the wrap, so the count reflects
   * only the commits of the entry point under test.
   */
  @Nested
  class EngineCommitsReachTheWrapper extends TestHelper {

    private final AtomicInteger wrapperCommits     = new AtomicInteger();
    private final AtomicInteger asyncWorkerCommits = new AtomicInteger();

    /**
     * The async executor is created lazily and captured the wrapper as it stood at that moment. One created before the
     * HA wrap (a parallel SELECT or a scheduled index compaction while the server was starting) - or before a plugin
     * restart replaced the wrapper - kept committing every async write on the stale instance for the life of the
     * database.
     */
    @Test
    void anAsyncExecutorCreatedBeforeTheWrapCommitsThroughTheWrapper() {
      // THE FIRST RECORD REGISTERS "id" IN THE DICTIONARY BEFORE THE WRAP: that registration commits through the wrapper
      // in its own nested transaction, on whatever thread meets the name first, and would pass this test on its own
      database.transaction(() -> {
        database.getSchema().createDocumentType("AsyncDoc");
        database.newDocument("AsyncDoc").set("id", 0).save();
      });

      final LocalDatabase local = (LocalDatabase) database;
      // CREATE THE EXECUTOR BEFORE THE WRAP, AS A STARTING SERVER CAN
      local.async();

      final DatabaseInternal previous = local.getWrappedDatabaseInstance();
      local.setWrappedDatabaseInstance(countingWrapper(local));
      try {
        local.async().transaction(() -> local.newDocument("AsyncDoc").set("id", 1).save());
        local.async().waitCompletion();

        assertThat(asyncWorkerCommits.get()).as("the async worker must commit through the current wrapper").isGreaterThan(0);
      } finally {
        local.setWrappedDatabaseInstance(previous);
      }

      assertThat(database.countType("AsyncDoc", false)).isEqualTo(2);
    }

    /**
     * A plugin restart replaces wrapper A with wrapper B: the executor must follow, and A must stop receiving commits.
     */
    @Test
    void anAsyncExecutorFollowsAReplacedWrapper() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("AsyncDoc");
        database.newDocument("AsyncDoc").set("id", 0).save();
      });

      final LocalDatabase local = (LocalDatabase) database;
      final DatabaseInternal previous = local.getWrappedDatabaseInstance();
      final AtomicInteger commitsOnA = new AtomicInteger();
      final AtomicInteger commitsOnB = new AtomicInteger();
      try {
        local.setWrappedDatabaseInstance(countingWrapper(local, commitsOnA, new AtomicInteger()));
        local.async().transaction(() -> local.newDocument("AsyncDoc").set("id", 1).save());
        local.async().waitCompletion();
        assertThat(commitsOnA.get()).as("the async worker must commit through wrapper A").isGreaterThan(0);

        final int commitsOnABeforeReplacement = commitsOnA.get();
        local.setWrappedDatabaseInstance(countingWrapper(local, new AtomicInteger(), commitsOnB));
        local.async().transaction(() -> local.newDocument("AsyncDoc").set("id", 2).save());
        local.async().waitCompletion();

        assertThat(commitsOnB.get()).as("the async worker must commit through the replacing wrapper B").isGreaterThan(0);
        assertThat(commitsOnA.get()).as("the replaced wrapper A must receive no further commits")
            .isEqualTo(commitsOnABeforeReplacement);
      } finally {
        local.setWrappedDatabaseInstance(previous);
      }

      assertThat(database.countType("AsyncDoc", false)).isEqualTo(3);
    }

    /**
     * A rebind while a worker holds an open batch: the batch began under wrapper A and commits through wrapper B. That
     * is still one transaction, because every wrapper delegates to the same embedded database and the transaction lives
     * in the worker's thread context - so every record of the batch must land, committed through B.
     */
    @Test
    void aBatchOpenAcrossARebindCommitsWhollyThroughTheNewWrapper() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("BatchDoc");
        database.newDocument("BatchDoc").set("id", -1).save();
      });

      final LocalDatabase local = (LocalDatabase) database;
      final DatabaseInternal previous = local.getWrappedDatabaseInstance();
      final AtomicInteger asyncCommitsOnB = new AtomicInteger();
      final DatabaseInternal wrapperB = countingWrapper(local, new AtomicInteger(), asyncCommitsOnB);
      final AtomicInteger hookCalls = new AtomicInteger();
      try {
        local.async().setParallelLevel(1);
        local.async().setCommitEvery(1_000);
        local.setWrappedDatabaseInstance(countingWrapper(local, new AtomicInteger(), new AtomicInteger()));

        // REBIND RIGHT BEFORE THE BATCH COMMIT, WITH THE BATCH STILL OPEN ON THE WORKER
        DatabaseAsyncExecutorImpl.TEST_BEFORE_BATCH_COMMIT_HOOK = callNumber -> {
          if (hookCalls.getAndIncrement() == 0)
            local.setWrappedDatabaseInstance(wrapperB);
        };

        for (int i = 0; i < 20; i++)
          local.async().createRecord(local.newDocument("BatchDoc").set("id", i), null);
        local.async().waitCompletion();

        assertThat(hookCalls.get()).as("the batch commit must have been reached").isGreaterThan(0);
        assertThat(asyncCommitsOnB.get()).as("the open batch must commit through the wrapper installed meanwhile")
            .isGreaterThan(0);
      } finally {
        DatabaseAsyncExecutorImpl.TEST_BEFORE_BATCH_COMMIT_HOOK = null;
        local.setWrappedDatabaseInstance(previous);
      }

      assertThat(database.countType("BatchDoc", false)).isEqualTo(21);
    }

    /**
     * {@code copyType()} copies the records in batches of {@code transactionBatchSize}, and committed every batch on the
     * schema's own database reference, which is the inner instance.
     */
    @Test
    void copyTypeCommitsTheCopiedRecordsThroughTheWrapper() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Source");
        for (int i = 0; i < 50; i++)
          database.newDocument("Source").set("id", i).save();
      });

      final LocalDatabase local = (LocalDatabase) database;
      final DatabaseInternal previous = local.getWrappedDatabaseInstance();
      local.setWrappedDatabaseInstance(countingWrapper(local));
      try {
        local.getSchema().copyType("Source", "Target", LocalDocumentType.class, 1, 65536, 10);
        // 50 records in batches of 10: five batch commits at least, plus the final one
        assertThat(wrapperCommits.get()).as("every copied batch must commit through the current wrapper")
            .isGreaterThanOrEqualTo(5);
      } finally {
        local.setWrappedDatabaseInstance(previous);
      }

      assertThat(database.countType("Target", false)).isEqualTo(50);
    }

    /**
     * {@code CHECK DATABASE} hands the checker the wrapper, but the checker is public API: one built around the inner
     * instance committed its fixes on this node only.
     */
    @Test
    void aCheckerBuiltOnTheInnerInstanceCommitsThroughTheWrapper() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Checked");
        for (int i = 0; i < 10; i++)
          database.newDocument("Checked").set("id", i).save();
      });

      final LocalDatabase local = (LocalDatabase) database;
      final DatabaseInternal previous = local.getWrappedDatabaseInstance();
      local.setWrappedDatabaseInstance(countingWrapper(local));
      try {
        new DatabaseChecker(local).setFix(true).check();
        assertThat(wrapperCommits.get()).as("the checker's commits must go through the current wrapper").isGreaterThan(0);
      } finally {
        local.setWrappedDatabaseInstance(previous);
      }
    }

    /**
     * {@code LocalBucket.check(fix)} - reached from {@code CHECK DATABASE FIX} - opens its repair transaction on the
     * bucket's own database reference, which is the inner instance, so its repairs committed on this node only.
     */
    @Test
    void aBucketRepairPassCommitsThroughTheWrapper() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Repaired");
        for (int i = 0; i < 10; i++)
          database.newDocument("Repaired").set("id", i).save();
      });

      final LocalDatabase local = (LocalDatabase) database;
      final LocalBucket bucket = (LocalBucket) local.getSchema().getType("Repaired").getBuckets(false).getFirst();
      final DatabaseInternal previous = local.getWrappedDatabaseInstance();
      local.setWrappedDatabaseInstance(countingWrapper(local));
      try {
        bucket.check(0, true);
        assertThat(wrapperCommits.get()).as("the repair pass must commit through the current wrapper").isGreaterThan(0);
      } finally {
        local.setWrappedDatabaseInstance(previous);
      }
    }

    private DatabaseInternal countingWrapper(final DatabaseInternal delegate) {
      return countingWrapper(delegate, wrapperCommits, asyncWorkerCommits);
    }

    private static DatabaseInternal countingWrapper(final DatabaseInternal delegate, final AtomicInteger commits,
        final AtomicInteger asyncCommits) {
      return (DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
          new Class<?>[] { DatabaseInternal.class }, (proxy, method, args) -> {
            if ("commit".equals(method.getName()) && (args == null || args.length == 0)) {
              commits.incrementAndGet();
              if (Thread.currentThread().getName().startsWith("AsyncExecutor-"))
                asyncCommits.incrementAndGet();
            }
            try {
              return method.invoke(delegate, args);
            } catch (final InvocationTargetException e) {
              throw e.getCause();
            }
          });
    }
  }

  /**
   * Issue #8784: the replication layer ends a transaction whose pages another thread publishes (a Raft replica, a leader
   * whose entry its state machine applies from the WAL bytes) and a transaction with nothing to publish without running
   * phase 2. {@link TransactionContext#concludeCommitWithoutPublishing()} is that end, and it must be a COMMIT: the
   * after-commit callbacks fire, the commit counter moves, the saved records are clean, the context is released.
   */
  @Nested
  class ConcludeCommitWithoutPublishing extends TestHelper {
    private static final String TYPE_NAME = "Issue8784Doc";

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().createDocumentType(TYPE_NAME);
        database.newDocument(TYPE_NAME).set("name", "seed").save();
      });
    }

    /** The replica tail: phase 1 ran, the pages are published elsewhere, this thread concludes. */
    @Test
    void aTransactionWhosePagesArePublishedElsewhereEndsAsACommit() {
      final AtomicInteger fired = new AtomicInteger();

      database.begin();
      final MutableDocument doc = database.iterateType(TYPE_NAME, false).next().asDocument().modify();
      doc.set("name", "updated").save();
      assertThat(doc.isDirty()).isTrue();

      final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
      tx.addAfterCommitCallback(fired::incrementAndGet);
      final long commitCountBefore = tx.getCommitCount();

      final TransactionContext.TransactionPhase1 phase1 = tx.commit1stPhase(false);
      assertThat(phase1).as("the update must leave something to publish").isNotNull();

      tx.concludeCommitWithoutPublishing();

      assertThat(fired.get()).as("the after-commit callback fires exactly once").isEqualTo(1);
      assertThat(tx.getCommitCount()).as("the commit is counted (#7916 retry guard)").isEqualTo(commitCountBefore + 1);
      assertThat(TransactionContext.isPartiallyCommitted(tx, commitCountBefore)).isTrue();
      assertThat(doc.isDirty()).as("the committed record is clean").isFalse();
      assertThat(tx.isActive()).isFalse();
      assertThat(database.isTransactionActive()).isFalse();

      // The file locks phase 1 took are released: the next transaction on the same type commits.
      database.transaction(() -> database.newDocument(TYPE_NAME).set("name", "after").save());
    }

    /** The read-only arm: phase 1 found nothing to publish, which is still a commit. */
    @Test
    void aTransactionWithNothingToPublishEndsAsACommit() {
      final AtomicInteger fired = new AtomicInteger();

      database.begin();
      final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
      tx.addAfterCommitCallback(fired::incrementAndGet);
      final long commitCountBefore = tx.getCommitCount();

      assertThat(tx.commit1stPhase(true)).isNull();
      tx.concludeCommitWithoutPublishing();

      assertThat(fired.get()).isEqualTo(1);
      assertThat(tx.getCommitCount()).isEqualTo(commitCountBefore + 1);
      assertThat(database.isTransactionActive()).isFalse();
    }

    /** The baseline the HA path must match: a plain embedded commit with nothing to write fires and counts the same way. */
    @Test
    void theEmbeddedReadOnlyCommitIsTheBaseline() {
      final AtomicInteger fired = new AtomicInteger();

      database.begin();
      final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
      tx.addAfterCommitCallback(fired::incrementAndGet);
      final long commitCountBefore = tx.getCommitCount();
      database.commit();

      assertThat(fired.get()).isEqualTo(1);
      assertThat(tx.getCommitCount()).isEqualTo(commitCountBefore + 1);
    }
  }

  /**
   * Issue #8686: the replication position a transaction records at begin is per transaction. A context is reused across
   * transactions on a thread, so {@code begin()} must clear it, or a transaction begun by any other route would read as
   * prepared at the previous one's position.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ReplicationBasePosition extends TestHelper {

    @Test
    void beginClearsThePositionOfAPreviousTransaction() {
      database.begin();
      final TransactionContext tx = ((DatabaseInternal) database).getTransaction();
      assertThat(tx.getReplicationBasePosition()).as("unknown until a replicated database stamps it").isEqualTo(-1L);

      tx.setReplicationBasePosition(42L);
      assertThat(tx.getReplicationBasePosition()).isEqualTo(42L);
      database.commit();

      database.begin();
      assertThat(((DatabaseInternal) database).getTransaction().getReplicationBasePosition()).isEqualTo(-1L);
      database.rollback();
    }
  }
}
