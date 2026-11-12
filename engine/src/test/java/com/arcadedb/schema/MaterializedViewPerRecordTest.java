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
package com.arcadedb.schema;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.FileManager;
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.event.BeforeRecordUpdateListener;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Per-record materialized views (issue #11475): an INCREMENTAL view whose rows map 1:1 to source records carries a
 * {@code _sourceRID} link on each row, indexed UNIQUE, and its full pass matches rows to their source through it.
 */
class MaterializedViewPerRecordTest extends TestHelper {

  @Test
  void incrementalPerRecordViewGetsTheLinkAndIndex() {
    createPersons(3);
    final MaterializedViewImpl view = createView("PV", "SELECT name FROM Person", MaterializedViewRefreshMode.INCREMENTAL);

    assertThat(view.isPerRecord()).isTrue();
    final DocumentType backing = database.getSchema().getType("PV");
    assertThat(backing.getProperty(MaterializedViewPerRecordRefresher.SOURCE_RID).getType()).isEqualTo(Type.LINK);
    assertThat(backing.getAllIndexes(false))
        .anyMatch(i -> i.isUnique() && i.getPropertyNames().equals(List.of(MaterializedViewPerRecordRefresher.SOURCE_RID)));

    int rows = 0;
    try (final ResultSet rs = database.query("sql", "SELECT FROM PV")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final RID source = row.getProperty(MaterializedViewPerRecordRefresher.SOURCE_RID);
        assertThat(source).isNotNull();
        final Document person = (Document) database.lookupByRID(source, true);
        assertThat(person.getTypeName()).isEqualTo("Person");
        assertThat(person.getString("name")).isEqualTo(row.<String>getProperty("name"));
        ++rows;
      }
    }
    assertThat(rows).isEqualTo(3);
  }

  @Test
  void manualViewGetsNoLink() {
    createPersons(3);
    final MaterializedViewImpl view = createView("MV", "SELECT name FROM Person", MaterializedViewRefreshMode.MANUAL);

    assertThat(view.isPerRecord()).isFalse();
    assertThat(database.getSchema().getType("MV").existsProperty(MaterializedViewPerRecordRefresher.SOURCE_RID)).isFalse();
    assertThat(database.getSchema().getType("MV").getAllIndexes(false)).isEmpty();
  }

  @Test
  void nonPerRecordIncrementalViewGetsNoLink() {
    createPersons(3);
    final MaterializedViewImpl view = createView("OV", "SELECT name FROM Person ORDER BY name",
        MaterializedViewRefreshMode.INCREMENTAL);

    assertThat(view.isPerRecord()).isFalse();
    assertThat(database.getSchema().getType("OV").existsProperty(MaterializedViewPerRecordRefresher.SOURCE_RID)).isFalse();
    try (final ResultSet rs = database.query("sql", "SELECT FROM OV")) {
      assertThat(rs.stream().filter(r -> r.hasProperty(MaterializedViewPerRecordRefresher.SOURCE_RID)).count()).isZero();
    }
  }

  @Test
  void perRecordFlagSurvivesReopen() {
    createPersons(3);
    createView("PV", "SELECT name FROM Person", MaterializedViewRefreshMode.INCREMENTAL);

    reopenDatabase();

    assertThat(((MaterializedViewImpl) database.getSchema().getMaterializedView("PV")).isPerRecord()).isTrue();
  }

  @Test
  void fullPassKeepsEachRowOnItsSource() {
    createPersons(20);
    createView("PV", "SELECT name, age FROM Person", MaterializedViewRefreshMode.INCREMENTAL);

    final Map<RID, RID> before = rowBySource("PV");
    assertThat(before).hasSize(20);

    database.command("sql", "REFRESH MATERIALIZED VIEW PV");
    database.command("sql", "REFRESH MATERIALIZED VIEW PV");

    assertThat(rowBySource("PV")).isEqualTo(before);
  }

  /**
   * The full pass matches each row to its source by the link, not by scan order. Two sources are deleted while the view is not
   * maintained on commit (ALTERed to MANUAL, which keeps the link), so the defining query's order no longer lines up with the
   * view's: a scan-order match would write each later source over the row of the one before it, moving every row off its
   * source. Matched by link, the row of every unchanged source keeps its RID and its content, and only the two rows go.
   */
  @Test
  void fullPassAfterTheSourceOrderMovedKeepsEachRowOnItsSource() {
    createPersons(20);
    createView("PV", "SELECT name, age FROM Person", MaterializedViewRefreshMode.INCREMENTAL);
    database.command("sql", "ALTER MATERIALIZED VIEW PV REFRESH MANUAL");
    final Map<RID, RID> before = rowBySource("PV");
    assertThat(before).hasSize(20);
    final RID p0 = sourceOf("p0");
    final RID p5 = sourceOf("p5");

    database.transaction(() -> database.command("sql", "DELETE FROM Person WHERE name IN ['p0', 'p5']"));
    database.command("sql", "REFRESH MATERIALIZED VIEW PV");

    final Map<RID, RID> expected = new HashMap<>(before);
    expected.remove(p0);
    expected.remove(p5);
    assertThat(rowBySource("PV")).as("every unchanged source keeps its row").isEqualTo(expected);
    try (final ResultSet rs = database.query("sql", "SELECT FROM PV")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final Document source = (Document) database.lookupByRID(row.<RID>getProperty(MaterializedViewPerRecordRefresher.SOURCE_RID),
            true);
        assertThat(row.<String>getProperty("name")).as("the row carries its own source's content").isEqualTo(source.getString("name"));
      }
    }
  }

  /**
   * 50 full passes of an unchanged view write nothing to its {@code _sourceRID} index: every row keeps its key, so
   * {@code DocumentIndexer.updateDocument} skips the index. On an unchanged source a scan-order match would keep every key too, so
   * this does not tell the two matches apart; {@link #fullPassAfterTheSourceOrderMovedKeepsEachRowOnItsSource} does.
   */
  @Test
  @Tag("slow")
  void fullPassOnUnchangedSourceDoesNotGrowTheIndex() throws Exception {
    createPersons(2000);
    createView("SoakPV", "SELECT name, age FROM Person", MaterializedViewRefreshMode.INCREMENTAL);

    database.command("sql", "REFRESH MATERIALIZED VIEW SoakPV");
    final long afterFirst = indexBytesOf("SoakPV");
    assertThat(afterFirst).as("the index is on disk to begin with").isPositive();

    for (int pass = 0; pass < 50; pass++)
      database.command("sql", "REFRESH MATERIALIZED VIEW SoakPV");

    assertThat(rowBySource("SoakPV")).hasSize(2000);
    assertThat(indexBytesOf("SoakPV"))
        .as("50 full passes of an unchanged per-record view must not write its _sourceRID index")
        .isEqualTo(afterFirst);
  }

  @Test
  void fullPassAddsAndRemovesRows() {
    createPersons(5);
    // Until the per-record delta pass exists, the refresh owed to the commit below is a full pass as well; the explicit REFRESH
    // after it pins the full pass whatever the commit path becomes
    createView("PV", "SELECT name, age FROM Person WHERE age >= 2", MaterializedViewRefreshMode.INCREMENTAL);
    final Map<RID, RID> before = rowBySource("PV");

    database.transaction(() -> {
      database.command("sql", "DELETE FROM Person WHERE name = 'p2'");
      database.newDocument("Person").set("name", "p99").set("age", 99).save();
      // moves out of the WHERE
      database.command("sql", "UPDATE Person SET age = 0 WHERE name = 'p4'");
    });

    database.command("sql", "REFRESH MATERIALIZED VIEW PV");

    assertThat(viewRows("PV")).containsExactlyInAnyOrderElementsOf(queryRows("SELECT name, age FROM Person WHERE age >= 2"));
    assertThat(viewRows("PV")).containsExactlyInAnyOrder("p3/3", "p99/99");

    // the surviving row stayed on its record: a scan-order match would have written p3 over the record p2 left
    final Map<RID, RID> after = rowBySource("PV");
    int survivors = 0;
    for (final Map.Entry<RID, RID> entry : after.entrySet())
      if (before.containsKey(entry.getKey())) {
        assertThat(entry.getValue()).as("the row of a surviving source keeps its record").isEqualTo(before.get(entry.getKey()));
        ++survivors;
      }
    assertThat(survivors).as("p3 survived the mutation, so the check above reached a row").isEqualTo(1);
  }

  // ---- delta capture and the per-record pass on commit ----

  private static final String ADULTS_QUERY = "SELECT name, age FROM Person WHERE age >= 18";

  @Test
  void createUpdateDeleteKeepTheViewEqualToTheQuery() {
    final MaterializedViewImpl view = createAdultsView(10);
    assertThat(view.isPerRecord()).isTrue();
    assertViewMatchesQuery();

    database.transaction(() -> database.newDocument("Person").set("name", "new").set("age", 40).save());
    assertViewMatchesQuery();

    database.transaction(() -> database.command("sql", "UPDATE Person SET age = 55 WHERE name = 'a3'"));
    assertViewMatchesQuery();
    assertThat(viewRows("Adults")).contains("a3/55");

    database.transaction(() -> database.command("sql", "DELETE FROM Person WHERE name = 'a4'"));
    assertViewMatchesQuery();
    assertThat(viewRows("Adults")).noneMatch(r -> r.startsWith("a4/"));
  }

  @Test
  void updateOutOfAndBackIntoWhere() {
    createAdultsView(5);
    final RID a2 = sourceOf("a2");
    final RID rowBefore = rowBySource("Adults").get(a2);
    assertThat(rowBefore).isNotNull();

    database.transaction(() -> database.command("sql", "UPDATE Person SET age = 10 WHERE name = 'a2'"));
    assertViewMatchesQuery();
    assertThat(rowBySource("Adults")).doesNotContainKey(a2);

    database.transaction(() -> database.command("sql", "UPDATE Person SET age = 30 WHERE name = 'a2'"));
    assertViewMatchesQuery();
    assertThat(rowBySource("Adults")).containsKey(a2);
    assertThat(viewRows("Adults")).contains("a2/30");
  }

  @Test
  void subtypeRecordIsMaintained() {
    createAdultsView(3);
    database.transaction(() -> database.newDocument("Employee").set("name", "emp").set("age", 33).save());

    assertViewMatchesQuery();
    assertThat(viewRows("Adults")).contains("emp/33");
  }

  @Test
  void createAndDeleteInSameTransactionLeavesNoRow() {
    createAdultsView(3);
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Person").set("name", "ghost").set("age", 50).save();
      doc.delete();
    });

    assertViewMatchesQuery();
    assertThat(viewRows("Adults")).noneMatch(r -> r.startsWith("ghost/"));
  }

  @Test
  void rolledBackChangesDoNotReachTheView() {
    createAdultsView(5);

    database.begin();
    database.command("sql", "UPDATE Person SET age = 99 WHERE name = 'a1'");
    database.newDocument("Person").set("name", "rolledBack").set("age", 60).save();
    database.rollback();

    // an unrelated commit runs a pass, which must not pick up anything of the rolled-back transaction
    final long before = writes();
    database.transaction(() -> database.newDocument("Person").set("name", "after").set("age", 70).save());
    assertThat(writes() - before).as("1 source create + 1 view row write, nothing of the rolled-back transaction").isLessThanOrEqualTo(2);

    assertViewMatchesQuery();
    assertThat(viewRows("Adults")).noneMatch(r -> r.startsWith("rolledBack/") || r.equals("a1/99"));
  }

  @Test
  void readYourWritesAfterCommit() {
    createAdultsView(3);
    database.transaction(() -> database.newDocument("Person").set("name", "mine").set("age", 41).save());
    // same thread, right after the commit returned
    assertThat(viewRows("Adults")).contains("mine/41");
  }

  /**
   * 11 changed records against a cap of 10: the transaction overflows and its commit makes a full pass, inline. Told apart from a
   * delta pass by its writes: the 11 records alone cost 11 source writes and 11 view row writes, the full pass rewrites all 300
   * rows of the view.
   */
  @Test
  void transactionPastTheCapTriggersACorrectFullPassInline() {
    createAdultsView(300);
    withMaxDelta(10, () -> {
      final long before = writes();
      database.transaction(() -> database.command("sql",
          "UPDATE Person SET age = age + 1 WHERE name IN ['a0','a1','a2','a3','a4','a5','a6','a7','a8','a9','a10']"));
      // same thread, right after the commit returned
      assertThat(writes() - before).as("the overflow made a full pass, which rewrites all 300 rows; a delta pass writes about 22")
          .isGreaterThanOrEqualTo(11 + 300);
      assertViewMatchesQuery();
      assertThat(viewRows("Adults")).contains("a10/29");
      assertThat(database.getSchema().getMaterializedView("Adults").getStatus()).isEqualTo("VALID");
    });
  }

  /**
   * The pending delta counts its distinct records only once its raw size has doubled since the last count (the listener's guard),
   * so the records appended in between can take it past the cap uncounted. The drain counts them: past the cap, the pass is full.
   */
  @Test
  void pendingDeltaPastTheCapBetweenTwoCountsIsFullAtTheDrain() {
    final MaterializedViewImpl view = createAdultsView(3);
    withMaxDelta(10, () -> {
      assertThat(view.tryBeginRefresh()).isTrue();
      try {
        // 6 distinct records, each twice: 12 raw, counted (12 > 10) down to 6, and the next count waits for 12 raw again
        final RecordIdentities twice = new RecordIdentities(12);
        for (int i = 0; i < 12; i++)
          twice.add(1, i % 6);
        view.appendPendingDelta(twice);
        // 6 more distinct records: 12 raw, 12 distinct, not counted at the append
        final RecordIdentities more = new RecordIdentities(6);
        for (int i = 6; i < 12; i++)
          more.add(1, i);
        view.appendPendingDelta(more);

        final MaterializedViewImpl.PendingDelta drained = view.drainPendingDelta();
        assertThat(drained.full()).as("12 distinct records are past the cap of 10").isTrue();
        assertThat(drained.recovery()).isFalse();
      } finally {
        view.endRefresh();
      }
    });
  }

  /** A transaction's buffer is handed over, not copied, when nothing is pending: the after-commit callback never reads it again. */
  @Test
  void anAdoptedBufferIsTheDrainedOne() {
    final MaterializedViewImpl view = createAdultsView(3);
    assertThat(view.tryBeginRefresh()).isTrue();
    try {
      final RecordIdentities buffer = new RecordIdentities(4);
      buffer.add(1, 0);
      buffer.add(1, 1);
      view.adoptPendingDelta(buffer);

      final MaterializedViewImpl.PendingDelta drained = view.drainPendingDelta();
      assertThat(drained.full()).isFalse();
      assertThat(drained.rids()).isSameAs(buffer);
      assertThat(drained.rids().size()).isEqualTo(2);
    } finally {
      view.endRefresh();
    }
  }

  @Test
  void pendingDeltaPastTheCapCollapsesToFull() {
    final MaterializedViewImpl view = createAdultsView(3);
    withMaxDelta(10, () -> {
      assertThat(view.tryBeginRefresh()).isTrue();
      try {
        final RecordIdentities rids = new RecordIdentities(11);
        for (int i = 0; i < 11; i++)
          rids.add(1, i);
        view.appendPendingDelta(rids);

        final MaterializedViewImpl.PendingDelta drained = view.drainPendingDelta();
        assertThat(drained.full()).isTrue();
        assertThat(drained.recovery()).isFalse();
        assertThat(drained.rids().size()).isZero();
      } finally {
        view.endRefresh();
      }
    });
  }

  @Test
  void sameRecordUpdatedManyTimesDoesNotOverflow() {
    createAdultsView(500);
    withMaxDelta(10, () -> {
      final long before = writes();
      database.transaction(() -> {
        for (int i = 0; i < 50; i++)
          database.command("sql", "UPDATE Person SET age = " + (30 + i) + " WHERE name = 'a100'");
      });
      // each of the 50 updates counts as a source write: the pass itself adds at most the one view row (plus slack)
      assertThat(writes() - before).as("distinct RIDs are counted: one record is not an overflow").isLessThanOrEqualTo(50 + 4);
      assertViewMatchesQuery();
    });
  }

  private void withMaxDelta(final int cap, final Runnable body) {
    final ContextConfiguration config = database.getConfiguration();
    final int previous = config.getValueAsInteger(GlobalConfiguration.MATERIALIZED_VIEW_PER_RECORD_MAX_DELTA);
    config.setValue(GlobalConfiguration.MATERIALIZED_VIEW_PER_RECORD_MAX_DELTA, cap);
    try {
      body.run();
    } finally {
      config.setValue(GlobalConfiguration.MATERIALIZED_VIEW_PER_RECORD_MAX_DELTA, previous);
    }
  }

  /** Regression for #11475: one changed source record costs one view row write, not a rewrite of the whole view. */
  @Test
  void onePassTouchesOnlyChangedRows() {
    createAdultsView(5000);
    assertThat(viewRows("Adults")).hasSize(5000);

    final long before = writes();
    database.transaction(() -> database.command("sql", "UPDATE Person SET age = 77 WHERE name = 'a100'"));
    final long writesDelta = writes() - before;

    assertThat(writesDelta).as("1 source update + 1 view row write, small slack").isLessThanOrEqualTo(4);
    assertThat(viewRows("Adults")).contains("a100/77");
    assertViewMatchesQuery();
  }

  @Test
  void perRecordPathAfterReopen() {
    createAdultsView(2000);
    reopenDatabase();
    assertThat(((MaterializedViewImpl) database.getSchema().getMaterializedView("Adults")).isPerRecord()).isTrue();

    final long before = writes();
    database.transaction(() -> database.command("sql", "UPDATE Person SET age = 88 WHERE name = 'a7'"));
    final long writesDelta = writes() - before;

    assertThat(writesDelta).as("the per-record pass survives a reopen").isLessThanOrEqualTo(4);
    assertThat(viewRows("Adults")).contains("a7/88");
    assertViewMatchesQuery();
  }

  /**
   * Accepted behavior change (#11475): a column computed by following edges refreshes when its own source record changes, not
   * when an edge is added.
   */
  @Test
  void edgeChangeDoesNotRefreshTraversalColumnUntilItsRowChanges() {
    database.transaction(() -> {
      database.getSchema().createVertexType("User").createProperty("id", Type.INTEGER);
      database.getSchema().createEdgeType("FriendOf");
    });
    final RID[] users = new RID[3];
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        users[i] = database.newVertex("User").set("id", i).save().getIdentity();
      // the edge list of user 0 exists before the view: adding to it later does not rewrite the vertex
      users[0].asVertex().newEdge("FriendOf", users[1]);
    });
    final MaterializedViewImpl view = createView("Friends", "SELECT id, out('FriendOf').size() AS f FROM User",
        MaterializedViewRefreshMode.INCREMENTAL);
    assertThat(view.isPerRecord()).isTrue();
    assertThat(friendsOf(0)).isEqualTo(1);

    database.transaction(() -> users[0].asVertex().newEdge("FriendOf", users[2]));
    assertThat(friendsOf(0)).as("an edge is not a tracked source: the row keeps its value").isEqualTo(1);

    database.transaction(() -> users[0].asVertex().modify().set("touched", true).save());
    assertThat(friendsOf(0)).as("the row is recomputed when its own source record changes").isEqualTo(2);
  }

  @Test
  void recordAtDrainedRidOfAnotherTypeProducesNoRow() {
    final MaterializedViewImpl view = createAdultsView(3);
    database.transaction(() -> database.getSchema().createDocumentType("Other"));
    final RID[] other = new RID[1];
    database.transaction(() -> other[0] = database.newDocument("Other").set("name", "x").set("age", 50).save().getIdentity());
    final List<String> before = viewRows("Adults");

    final RecordIdentities rids = new RecordIdentities(2);
    rids.add(other[0]);
    rids.add(sourceOf("a1"));
    MaterializedViewPerRecordRefresher.deltaPass(database, view, rids);

    assertThat(viewRows("Adults")).containsExactlyInAnyOrderElementsOf(before);
    assertViewMatchesQuery();
  }

  // ---- failure recovery ----

  private static final String SCHEDULER_THREAD_PREFIX = "ArcadeDB-MV-Scheduler-";

  /**
   * A failed delta pass loses the records it drained, so the view must not report VALID until a full pass has run, and that full
   * pass is handed to the scheduler rather than charged to whichever commit comes next.
   */
  @Test
  void failedDeltaPassLeavesViewNonValidAndRecoversOffTheCommittingThread() {
    final MaterializedViewImpl view = createAdultsView(10);
    failNextPassOf("Adults", () -> database.transaction(() -> database.command("sql", "UPDATE Person SET age = 50 WHERE name = 'a1'")));
    assertThat(view.getStatus()).as("a failed pass leaves the view non-VALID").isNotEqualTo(MaterializedViewStatus.VALID.name());

    final List<String> writerThreads = new CopyOnWriteArrayList<>();
    final BeforeRecordUpdateListener recordThread = record -> {
      writerThreads.add(Thread.currentThread().getName());
      return true;
    };
    final DocumentType backing = database.getSchema().getType("Adults");
    backing.getEvents().registerListener(recordThread);
    try {
      database.transaction(() -> database.command("sql", "UPDATE Person SET age = 51 WHERE name = 'a2'"));
      Awaitility.await("the recovery full pass ends VALID").atMost(30, TimeUnit.SECONDS)
          .until(() -> MaterializedViewStatus.VALID.name().equals(view.getStatus()));
    } finally {
      backing.getEvents().unregisterListener(recordThread);
    }

    assertViewMatchesQuery();
    assertThat(viewRows("Adults")).contains("a1/50", "a2/51");
    assertThat(writerThreads).as("the recovery is a full pass: it rewrites every row").hasSizeGreaterThanOrEqualTo(10);
    assertThat(writerThreads).as("and it runs on the scheduler, not on the committing thread")
        .allMatch(name -> name.startsWith(SCHEDULER_THREAD_PREFIX));
  }

  /** The commits made while the recovery pass runs are passes owed too, and none may be dropped by it. */
  @Test
  void commitsDuringRecoveryAreNotLost() throws Exception {
    final MaterializedViewImpl view = createAdultsView(10);
    failNextPassOf("Adults", () -> database.transaction(() -> database.command("sql", "UPDATE Person SET age = 50 WHERE name = 'a1'")));

    final CountDownLatch recoveryStarted = new CountDownLatch(1);
    final CountDownLatch commitsDone = new CountDownLatch(1);
    final BeforeRecordUpdateListener holdRecovery = record -> {
      if (Thread.currentThread().getName().startsWith(SCHEDULER_THREAD_PREFIX) && recoveryStarted.getCount() > 0) {
        recoveryStarted.countDown();
        try {
          commitsDone.await(30, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      return true;
    };
    final DocumentType backing = database.getSchema().getType("Adults");
    backing.getEvents().registerListener(holdRecovery);
    try {
      try {
        database.transaction(() -> database.command("sql", "UPDATE Person SET age = 51 WHERE name = 'a2'"));
        assertThat(recoveryStarted.await(30, TimeUnit.SECONDS)).as("the recovery pass reached its first write").isTrue();

        for (int i = 0; i < 20; i++) {
          final int n = i;
          database.transaction(() -> {
            database.newDocument("Person").set("name", "during" + n).set("age", 20 + n).save();
            database.command("sql", "UPDATE Person SET age = ? WHERE name = ?", 30 + n, "a" + (n % 10));
          });
        }
      } finally {
        commitsDone.countDown();
      }

      Awaitility.await("the recovery and the passes owed behind it end VALID").atMost(30, TimeUnit.SECONDS)
          .until(() -> MaterializedViewStatus.VALID.name().equals(view.getStatus()) && viewRows("Adults").size() == 30);
    } finally {
      backing.getEvents().unregisterListener(holdRecovery);
    }
    assertViewMatchesQuery();
  }

  /**
   * The anti-goal of #11475: a delta pass that follows a failure without a full pass would end VALID over a view missing the
   * records the failed pass drained.
   */
  @Test
  void perRecordFailureNeverEndsValidWithoutAFullPass() {
    final MaterializedViewImpl view = createAdultsView(10);
    failNextPassOf("Adults", () -> database.transaction(() -> database.command("sql", "UPDATE Person SET age = 50 WHERE name = 'a1'")));

    final AtomicInteger fullPassWrites = new AtomicInteger();
    final BeforeRecordUpdateListener marker = record -> {
      fullPassWrites.incrementAndGet();
      return true;
    };
    final DocumentType backing = database.getSchema().getType("Adults");
    backing.getEvents().registerListener(marker);
    try {
      // one record: a per-record-sized commit, which on its own would make a one-row delta pass
      database.transaction(() -> database.command("sql", "UPDATE Person SET age = 51 WHERE name = 'a2'"));
      Awaitility.await("the view ends VALID").atMost(30, TimeUnit.SECONDS)
          .until(() -> MaterializedViewStatus.VALID.name().equals(view.getStatus()));
    } finally {
      backing.getEvents().unregisterListener(marker);
    }

    assertThat(fullPassWrites.get()).as("a full pass ran before the view ended VALID: it writes every row, a delta pass one")
        .isGreaterThanOrEqualTo(10);
    assertViewMatchesQuery();
    assertThat(viewRows("Adults")).contains("a1/50");
  }

  /**
   * The recovery mark lives in memory and the non-VALID status on disk: a reopen in between must still make the next pass a full
   * one, or a delta pass would end VALID over a view missing the records of the failed pass.
   */
  @Test
  void perRecordFailureSurvivesAReopenAsAFullPass() {
    createAdultsView(10);
    failNextPassOf("Adults", () -> database.transaction(() -> database.command("sql", "UPDATE Person SET age = 50 WHERE name = 'a1'")));

    reopenDatabase();
    final MaterializedViewImpl view = (MaterializedViewImpl) database.getSchema().getMaterializedView("Adults");
    assertThat(view.getStatus()).as("the failure is still visible after the reopen").isNotEqualTo(MaterializedViewStatus.VALID.name());

    database.transaction(() -> database.command("sql", "UPDATE Person SET age = 51 WHERE name = 'a2'"));
    Awaitility.await("the view ends VALID").atMost(30, TimeUnit.SECONDS)
        .until(() -> MaterializedViewStatus.VALID.name().equals(view.getStatus()));

    assertViewMatchesQuery();
    assertThat(viewRows("Adults")).contains("a1/50", "a2/51");
  }

  /** Arms a failure on the first write to the backing type, runs {@code commit}, and disarms it. The commit itself succeeds. */
  private void failNextPassOf(final String viewName, final Runnable commit) {
    final DocumentType backing = database.getSchema().getType(viewName);
    final AtomicInteger written = new AtomicInteger();
    final BeforeRecordCreateListener boomOnCreate = record -> {
      if (written.incrementAndGet() == 1)
        throw new IllegalStateException("simulated failure part way through the refresh");
      return true;
    };
    final BeforeRecordUpdateListener boomOnUpdate = record -> {
      if (written.incrementAndGet() == 1)
        throw new IllegalStateException("simulated failure part way through the refresh");
      return true;
    };
    backing.getEvents().registerListener(boomOnCreate);
    backing.getEvents().registerListener(boomOnUpdate);
    try {
      commit.run();
    } finally {
      backing.getEvents().unregisterListener(boomOnCreate);
      backing.getEvents().unregisterListener(boomOnUpdate);
    }
    assertThat(written.get()).as("the pass reached the backing type and failed there").isPositive();
  }

  private MaterializedViewImpl createAdultsView(final int count) {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Person");
      type.createProperty("name", Type.STRING);
      type.createProperty("age", Type.INTEGER);
      database.getSchema().buildDocumentType().withName("Employee").withSuperType("Person").create();
    });
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        database.newDocument("Person").set("name", "a" + i).set("age", 18 + (i % 40)).save();
    });
    return createView("Adults", ADULTS_QUERY, MaterializedViewRefreshMode.INCREMENTAL);
  }

  private void assertViewMatchesQuery() {
    assertThat(viewRows("Adults")).containsExactlyInAnyOrderElementsOf(queryRows(ADULTS_QUERY));
  }

  private RID sourceOf(final String name) {
    try (final ResultSet rs = database.query("sql", "SELECT FROM Person WHERE name = ?", name)) {
      return rs.next().getIdentity().get();
    }
  }

  private int friendsOf(final int id) {
    try (final ResultSet rs = database.query("sql", "SELECT f FROM Friends WHERE id = ?", id)) {
      return rs.next().<Number>getProperty("f").intValue();
    }
  }

  private long writes() {
    final Map<String, Object> stats = database.getStats();
    return ((Number) stats.get("updateRecord")).longValue() + ((Number) stats.get("createRecord")).longValue()
        + ((Number) stats.get("deleteRecord")).longValue();
  }

  private void createPersons(final int count) {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Person");
      type.createProperty("name", Type.STRING);
      type.createProperty("age", Type.INTEGER);
    });
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        database.newDocument("Person").set("name", "p" + i).set("age", i).save();
    });
  }

  private MaterializedViewImpl createView(final String name, final String query, final MaterializedViewRefreshMode mode) {
    database.transaction(() -> database.getSchema().buildMaterializedView()
        .withName(name)
        .withQuery(query)
        .withRefreshMode(mode)
        .create());
    return (MaterializedViewImpl) database.getSchema().getMaterializedView(name);
  }

  private Map<RID, RID> rowBySource(final String viewName) {
    final Map<RID, RID> map = new HashMap<>();
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + viewName)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final RID source = row.getProperty(MaterializedViewPerRecordRefresher.SOURCE_RID);
        assertThat(map.put(source, row.getIdentity().get())).as("one row per source record").isNull();
      }
    }
    return map;
  }

  private List<String> viewRows(final String viewName) {
    return queryRows("SELECT name, age FROM " + viewName);
  }

  private List<String> queryRows(final String sql) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        rows.add(row.getProperty("name") + "/" + row.getProperty("age"));
      }
    }
    return rows;
  }

  /** Copied from {@code MaterializedViewRefreshAtomicityTest}: the index files' size once every committed page reached them. */
  private long indexBytesOf(final String viewName) throws Exception {
    assertThat(((DatabaseInternal) database).getPageManager().waitAllPagesOfDatabaseAreFlushed(database))
        .as("every committed page reached the index files before measuring them").isTrue();
    final FileManager files = ((DatabaseInternal) database).getFileManager();
    long bytes = 0;
    for (final TypeIndex index : database.getSchema().getType(viewName).getAllIndexes(false))
      for (final IndexInternal bucketIndex : index.getIndexesOnBuckets())
        for (final int fileId : bucketIndex.getFileIds())
          bytes += files.getFile(fileId).getSize();
    return bytes;
  }
}
