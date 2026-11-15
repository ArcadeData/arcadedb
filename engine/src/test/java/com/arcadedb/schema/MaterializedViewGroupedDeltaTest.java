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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.event.BeforeRecordUpdateListener;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Grouped materialized views maintained by delta (issue #11937): an INCREMENTAL view grouping one source type with decomposable
 * aggregates applies each commit's delta to the groups it changed, and is still equal to its defining query after every commit.
 */
class MaterializedViewGroupedDeltaTest extends TestHelper {
  private static final String G_SUMS    = "SELECT cat, count(*) AS n, sum(v) AS s, avg(v) AS a, count(v) AS c FROM I GROUP BY cat";
  private static final String G_EXTREME =
      "SELECT cat, sub, min(v) AS mn, max(v) AS mx, max(name) AS mxn, min(w) AS mw FROM I WHERE v > -20 GROUP BY cat, sub";
  private static final String G_LONGS   = "SELECT sub, sum(w) AS sw, count(*) FROM I GROUP BY sub";
  private static final String G_LOOSE   = "SELECT k, count(*) AS n FROM I GROUP BY k";

  private static final Map<String, String> VIEWS = new LinkedHashMap<>();

  static {
    VIEWS.put("GSums", G_SUMS);
    VIEWS.put("GExtreme", G_EXTREME);
    VIEWS.put("GLongs", G_LONGS);
    VIEWS.put("GLoose", G_LOOSE);
  }

  private static final String[] CATS = { "a", "b", "c", "d", null };

  private final AtomicInteger uids = new AtomicInteger();

  @Test
  void eligibleShapesHaveAPlan() {
    createSource();
    for (final String query : VIEWS.values())
      assertThat(MaterializedViewGroupedPlan.of(database, query)).as(query).isNotNull();
    assertThat(MaterializedViewGroupedPlan.of(database, "SELECT cat AS c, count(*) FROM I GROUP BY cat")).isNotNull();
    assertThat(MaterializedViewGroupedPlan.of(database, "SELECT count(*) AS n FROM I GROUP BY cat")).isNotNull();
    assertThat(MaterializedViewGroupedPlan.of(database, "SELECT cat, COUNT(*) AS n FROM I WHERE name = 'x' AND v IS NOT NULL GROUP BY cat"))
        .isNotNull();
  }

  @Test
  void ineligibleShapesHaveNoPlan() {
    createSource();
    for (final String query : List.of(//
        "SELECT cat, count(*) FROM I", // no GROUP BY
        "SELECT cat, count(*) AS n FROM I GROUP BY cat ORDER BY n", //
        "SELECT cat, count(*) AS n FROM I GROUP BY cat LIMIT 2", //
        "SELECT cat, count(DISTINCT v) AS n FROM I GROUP BY cat", //
        "SELECT cat, sum(v * 2) AS n FROM I GROUP BY cat", //
        "SELECT cat, sum(v) + 1 AS n FROM I GROUP BY cat", //
        "SELECT cat.toUpperCase() AS u, count(*) FROM I GROUP BY u", //
        "SELECT cat, name, count(*) FROM I GROUP BY cat", // a non-key, non-aggregate column
        "SELECT cat, count(*) FROM I WHERE name.length() > 2 GROUP BY cat", // a method in the WHERE
        "SELECT cat, count(*) FROM I WHERE sysdate() > 0 GROUP BY cat", // a function in the WHERE
        "SELECT cat, count(*) FROM I WHERE v IN (SELECT v FROM J) GROUP BY cat", // a subquery
        "SELECT cat, count(*) FROM (SELECT FROM I) GROUP BY cat", //
        "SELECT cat, count(*) FROM I WHERE v > $parent.x GROUP BY cat", //
        "SELECT sub AS cat, count(*) FROM I GROUP BY cat")) // GROUP BY an alias of another property
      assertThat(MaterializedViewGroupedPlan.of(database, query)).as(query).isNull();
  }

  /** The commit after the first one updates the one row of its group, and reads none of the source. */
  @Test
  void aSingleInsertUpdatesOneRowWithoutScanningTheSource() {
    createSource();
    database.transaction(() -> {
      for (int i = 0; i < 500; i++)
        insert(i % 5 == 0 ? null : CATS[i % 4], i % 3, i % 50, (long) i, "n" + i, null, "I");
    });
    final MaterializedViewImpl view = createView("GSums", G_SUMS);
    assertThat(view.isGroupedDelta()).isTrue();
    // the first commit builds the in-memory state
    database.transaction(() -> insert("a", 0, 1, 1L, "x", null, "I"));
    assertViewMatchesQuery("GSums");

    final AtomicInteger rowWrites = new AtomicInteger();
    final BeforeRecordUpdateListener countUpdates = record -> {
      rowWrites.incrementAndGet();
      return true;
    };
    final BeforeRecordCreateListener countCreates = record -> {
      rowWrites.incrementAndGet();
      return true;
    };
    final BeforeRecordDeleteListener countDeletes = record -> {
      rowWrites.incrementAndGet();
      return true;
    };
    final DocumentType backing = database.getSchema().getType("GSums");
    backing.getEvents().registerListener(countUpdates);
    backing.getEvents().registerListener(countCreates);
    backing.getEvents().registerListener(countDeletes);
    try {
      final long scansBefore = scans();
      database.transaction(() -> insert("b", 1, 7, 7L, "y", null, "I"));
      assertThat(rowWrites.get()).as("one group changed: one row written").isEqualTo(1);
      assertThat(scans()).as("the delta pass reads no source record").isEqualTo(scansBefore);

      rowWrites.set(0);
      database.transaction(() -> insert("zz", 1, 7, 7L, "y", null, "I"));
      assertThat(rowWrites.get()).as("a new group: one row created").isEqualTo(1);
      assertThat(scans()).isEqualTo(scansBefore);

      rowWrites.set(0);
      database.transaction(() -> database.command("sql", "UPDATE I SET cat = 'c', v = v + 1 WHERE cat = 'zz'"));
      assertThat(rowWrites.get()).as("a record moving to another group: two rows").isEqualTo(2);
      assertThat(scans()).isEqualTo(scansBefore);

      rowWrites.set(0);
      database.transaction(() -> {
        try (final ResultSet rs = database.query("sql", "SELECT FROM I WHERE cat = 'b' LIMIT 1")) {
          ((Document) rs.next().getRecord().get()).modify().set("v", 1000).save();
        }
      });
      assertThat(rowWrites.get()).as("an update within a group: one row").isEqualTo(1);
      database.transaction(() -> database.command("sql", "DELETE FROM I WHERE v = 1000"));
      assertThat(scans()).as("updates and deletes read no source record either").isEqualTo(scansBefore);
    } finally {
      backing.getEvents().unregisterListener(countUpdates);
      backing.getEvents().unregisterListener(countCreates);
      backing.getEvents().unregisterListener(countDeletes);
    }
    assertViewMatchesQuery("GSums");
  }

  @Test
  void deletingTheLastMemberOfAGroupRemovesItsRow() {
    createSource();
    createAllViews();
    database.transaction(() -> {
      insert("solo", 1, 5, 5L, "s", 1, "I");
      insert("a", 1, 5, 5L, "s", 1, "I");
    });
    assertAllViewsMatch();
    database.transaction(() -> database.command("sql", "DELETE FROM I WHERE cat = 'solo'"));
    assertAllViewsMatch();
    try (final ResultSet rs = database.query("sql", "SELECT FROM GSums WHERE cat = 'solo'")) {
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /** A record that leaves the WHERE, and one whose group changes, move between groups. */
  @Test
  void updatesMoveRecordsBetweenGroupsAndInAndOutOfTheWhere() {
    createSource();
    createAllViews();
    database.transaction(() -> {
      for (int i = 0; i < 20; i++)
        insert(CATS[i % 4], i % 3, i, (long) i, "n" + i, i % 2, "I");
    });
    database.transaction(() -> database.command("sql", "UPDATE I SET cat = 'b', v = -30 WHERE cat = 'a'"));
    assertAllViewsMatch();
    database.transaction(() -> database.command("sql", "UPDATE I SET v = 40 WHERE v = -30"));
    assertAllViewsMatch();
    database.transaction(() -> database.command("sql", "UPDATE I SET v = null, name = null, w = null WHERE cat = 'c'"));
    assertAllViewsMatch();
  }

  @Test
  void rolledBackTransactionsDoNotReachTheView() {
    createSource();
    createAllViews();
    database.transaction(() -> insert("a", 1, 5, 5L, "s", 1, "I"));
    database.begin();
    insert("a", 1, 6, 6L, "t", 1, "I");
    database.command("sql", "DELETE FROM I WHERE cat = 'a'");
    database.rollback();
    assertAllViewsMatch();
    try (final ResultSet rs = database.query("sql", "SELECT n FROM GSums WHERE cat = 'a'")) {
      assertThat(rs.next().<Long>getProperty("n")).isEqualTo(1L);
    }
  }

  /** Values a delta cannot apply exactly (a double sum) leave the view on the full pass, and still equal to its query. */
  @Test
  void floatingPointValuesFallBackToTheFullPass() {
    database.transaction(() -> database.getSchema().createDocumentType("F"));
    database.transaction(() -> {
      for (int i = 0; i < 30; i++)
        database.newDocument("F").set("g", i % 3).set("x", i * 0.1).save();
    });
    final String query = "SELECT g, sum(x) AS s, count(*) AS n FROM F GROUP BY g";
    final MaterializedViewImpl view = createView("FV", query);
    assertThat(view.isGroupedDelta()).isTrue();
    for (int i = 0; i < 10; i++) {
      final int n = i;
      database.transaction(() -> database.newDocument("F").set("g", n % 3).set("x", n * 0.7).save());
      assertThat(viewRows("FV")).containsExactlyInAnyOrderElementsOf(queryRows(query));
    }
    database.transaction(() -> database.command("sql", "DELETE FROM F WHERE g = 1"));
    assertThat(viewRows("FV")).containsExactlyInAnyOrderElementsOf(queryRows(query));
    assertThat(view.groupedRuntime()).isNotNull();
    assertThat(view.groupedRuntime().groups()).as("out of the exact domain: the full pass").isNull();
  }

  /** An Integer total that overflows the int range is widened by the query in an order-dependent way: full pass. */
  @Test
  void integerSumNearTheIntRangeFallsBackToTheFullPass() {
    createSource();
    final MaterializedViewImpl view = createView("GSums", G_SUMS);
    database.transaction(() -> insert("a", 1, Integer.MAX_VALUE - 5, 1L, "x", null, "I"));
    database.transaction(() -> insert("a", 1, 10, 1L, "x", null, "I"));
    assertViewMatchesQuery("GSums");
    database.transaction(() -> insert("a", 1, -10, 1L, "x", null, "I"));
    assertViewMatchesQuery("GSums");
    assertThat(view.groupedRuntime().groups()).isNull();
  }

  /**
   * The pending delta lives in memory: a commit whose delta was not applied when the process died is missing from a view persisted
   * VALID. The open after the crash rebuilds the view from its source, without a manual REFRESH, and deltas apply again after.
   */
  @Test
  void aCommitWhoseDeltaDiedWithTheProcessReachesTheViewAfterTheReopen() {
    createSource();
    database.transaction(() -> {
      for (int i = 0; i < 50; i++)
        insert(CATS[i % 4], i % 3, i, (long) i, "n" + i, i % 3, "I");
    });
    createView("GSums", G_SUMS);
    database.transaction(() -> insert("a", 0, 1, 1L, "x", 1, "I"));
    final MaterializedViewImpl view = (MaterializedViewImpl) database.getSchema().getMaterializedView("GSums");

    // Own the refresh, as a pass still running would: the commit below only leaves its delta pending, which the crash takes down
    assertThat(view.tryBeginRefresh()).isTrue();
    database.transaction(() -> {
      insert("late", 1, 40, 4L, "l", 1, "I");
      database.command("sql", "UPDATE I SET cat = 'b' WHERE cat = 'a'");
    });
    assertThat(viewRows("GSums")).as("the delta was not applied").isNotEqualTo(queryRows(G_SUMS));

    ((com.arcadedb.database.DatabaseInternal) database).kill();
    database.close();
    database = factory.open();

    Awaitility.await("the open rebuilds the view").atMost(30, TimeUnit.SECONDS).pollInterval(50, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> {
          assertThat(database.getSchema().getMaterializedView("GSums").getStatus()).isEqualTo(MaterializedViewStatus.VALID.name());
          assertViewMatchesQuery("GSums");
        });

    final long scans = scans();
    database.transaction(() -> insert("later", 2, 9, 9L, "m", 2, "I"));
    database.transaction(() -> insert("later", 2, 9, 9L, "m", 2, "I"));
    assertViewMatchesQuery("GSums");
    assertThat(scans() - scans).as("after the recovery, the commits apply as deltas").isLessThanOrEqualTo(1);
  }

  /** The state lives in memory: a reopen rebuilds it, and the commits after the reopen are applied to the persisted rows. */
  @Test
  void viewSurvivesReopenAndKeepsApplyingDeltas() {
    createSource();
    createAllViews();
    final Random random = new Random(7);
    for (int i = 0; i < 40; i++)
      randomTransaction(random);
    assertAllViewsMatch();

    reopenDatabase();
    assertAllViewsMatch();
    for (final String name : VIEWS.keySet())
      assertThat(((MaterializedViewImpl) database.getSchema().getMaterializedView(name)).isGroupedDelta()).isTrue();

    for (int i = 0; i < 40; i++) {
      randomTransaction(random);
      assertAllViewsMatch();
    }
    reopenDatabase();
    assertAllViewsMatch();
  }

  /**
   * The views without {@code min}/{@code max} never lose an extreme: after the rebuild of their first pass, every random transaction
   * is applied as a delta, and no further pass reads the source.
   */
  @Test
  void randomTransactionsOnCountsAndSumsAreAllAppliedAsDeltas() {
    createSource();
    final Random random = new Random(1193);
    database.transaction(() -> {
      for (int i = 0; i < 60; i++)
        insertRandom(random);
    });
    final List<String> views = List.of("GSums", "GLongs", "GLoose");
    for (final String name : views)
      createView(name, VIEWS.get(name));
    database.transaction(() -> insertRandom(random));
    final long scansAfterFirstPass = scans();

    for (int i = 0; i < 300; i++) {
      randomTransaction(random);
      for (final String name : views)
        assertViewMatchesQuery(name);
    }
    assertThat(scans()).as("no rebuild after the first pass").isEqualTo(scansAfterFirstPass);
  }

  /**
   * The correctness gate: random transactions of inserts, updates and deletes (group changes, nulls, records leaving and entering the
   * WHERE, deletes of whole groups, statements that fail and are taken back, rollbacks), and after each one every view equals its
   * defining query.
   */
  @Test
  void randomTransactionsKeepEveryViewEqualToItsQuery() {
    createSource();
    final Random random = new Random(11937);
    database.transaction(() -> {
      for (int i = 0; i < 60; i++)
        insertRandom(random);
    });
    createAllViews();
    for (final String name : VIEWS.keySet())
      assertThat(((MaterializedViewImpl) database.getSchema().getMaterializedView(name)).isGroupedDelta()).as(name).isTrue();

    for (int i = 0; i < 500; i++) {
      randomTransaction(random);
      assertAllViewsMatch();
      if (i == 250)
        reopenDatabase();
    }

    for (final String name : List.of("GSums", "GLongs", "GLoose")) {
      final MaterializedViewImpl view = (MaterializedViewImpl) database.getSchema().getMaterializedView(name);
      assertThat(view.groupedRuntime()).as(name).isNotNull();
      assertThat(view.groupedRuntime().groups()).as(name + " stays on the delta path").isNotNull();
    }
  }

  /**
   * Writers commit concurrently while another thread asks for full refreshes: the rebuild passes race the commits, and the view must
   * still end equal to its query.
   */
  @Test
  void concurrentWritersAndRebuildsConverge() throws Exception {
    createSource();
    database.transaction(() -> {
      for (int i = 0; i < 200; i++)
        insertRandom(new Random(i));
    });
    createAllViews();

    final AtomicBoolean stop = new AtomicBoolean();
    final List<Throwable> failures = new CopyOnWriteArrayList<>();
    final List<Thread> threads = new ArrayList<>();
    for (int t = 0; t < 4; t++) {
      final Random random = new Random(100 + t);
      threads.add(new Thread(() -> {
        try {
          for (int i = 0; i < 150; i++)
            try {
              database.transaction(() -> randomOperations(random, 1 + random.nextInt(4)), false, 50);
            } catch (final DuplicatedKeyException | RecordNotFoundException e) {
              // rolled back: a unique key or a record another writer took
            }
        } catch (final Throwable e) {
          failures.add(e);
        }
      }));
    }
    final Thread refresher = new Thread(() -> {
      try {
        while (!stop.get()) {
          for (final String name : VIEWS.keySet())
            database.getSchema().getMaterializedView(name).refresh();
          Thread.sleep(5);
        }
      } catch (final Throwable e) {
        failures.add(e);
      }
    });
    refresher.start();
    for (final Thread thread : threads)
      thread.start();
    for (final Thread thread : threads)
      thread.join(TimeUnit.MINUTES.toMillis(5));
    stop.set(true);
    refresher.join(TimeUnit.MINUTES.toMillis(1));
    assertThat(failures).isEmpty();

    // one more commit, so every view makes a pass after the last of them
    database.transaction(() -> insert("a", 1, 1, 1L, "z", 1, "I"));
    Awaitility.await("every view ends equal to its query").atMost(60, TimeUnit.SECONDS).untilAsserted(this::assertAllViewsMatch);
  }

  /**
   * Commits land while a rebuild pass is halfway through its scan: on records the scan has counted, on records it has not reached,
   * twice on one record, and new records. The scan cannot tell which version of each it read; the rebuild must still end equal to
   * the query, with no further pass to repair it.
   */
  @Test
  void commitsDuringARebuildScanAreCorrectedFor() throws Exception {
    createSource();
    database.transaction(() -> {
      for (int i = 0; i < 300; i++)
        insert(CATS[i % 4], i % 3, i % 40, (long) i, "n" + i, i % 3, "I");
    });
    createAllViews();
    database.transaction(() -> insert("a", 0, 1, 1L, "x", 1, "I"));
    assertAllViewsMatch();

    for (final String name : VIEWS.keySet()) {
      final List<RID> seen = new CopyOnWriteArrayList<>();
      final CountDownLatch reached = new CountDownLatch(1);
      final CountDownLatch release = new CountDownLatch(1);
      final Thread rebuild = new Thread(() -> database.getSchema().getMaterializedView(name).refresh());
      MaterializedViewGroupedRefresher.scanObserver = rid -> {
        if (Thread.currentThread() != rebuild)
          return;
        seen.add(rid);
        if (seen.size() == 150) {
          reached.countDown();
          await(release);
        }
      };
      try {
        rebuild.start();
        assertThat(reached.await(30, TimeUnit.SECONDS)).as("the rebuild of %s is halfway through its scan", name).isTrue();

        final List<RID> notSeen = new ArrayList<>();
        try (final ResultSet rs = database.query("sql", "SELECT @rid AS r FROM I")) {
          while (rs.hasNext()) {
            final RID rid = rs.next().getProperty("r");
            if (!seen.contains(rid))
              notSeen.add(rid);
          }
        }
        assertThat(notSeen).hasSizeGreaterThan(10);
        database.transaction(() -> {
          ((Document) database.lookupByRID(seen.get(10), true)).modify().set("cat", "moved").set("v", 33).save();
          ((Document) database.lookupByRID(notSeen.get(3), true)).modify().set("cat", "b").set("v", -5).save();
        });
        database.transaction(() -> {
          database.lookupByRID(seen.get(20), true).delete();
          database.lookupByRID(notSeen.get(5), true).delete();
        });
        database.transaction(() -> {
          for (int i = 0; i < 3; i++)
            insert("new", i, 100 + i, 7L, "z", null, i == 0 ? "J" : "I");
        });
        database.transaction(() -> ((Document) database.lookupByRID(seen.get(10), true)).modify().set("cat", "a").set("v", -15)
            .save());
        database.transaction(() -> ((Document) database.lookupByRID(notSeen.get(3), true)).modify().set("v", 39).save());
      } finally {
        release.countDown();
        rebuild.join(TimeUnit.SECONDS.toMillis(60));
        MaterializedViewGroupedRefresher.scanObserver = null;
      }
      assertAllViewsMatch();
      assertThat(((MaterializedViewImpl) database.getSchema().getMaterializedView(name)).groupedRuntime().groups())
          .as(name).isNotNull();
    }
  }

  /**
   * A transaction that reached its commit point and has not delivered its delta yet when a rebuild begins: the rebuild takes it over,
   * and its delta is not applied on top.
   */
  @Test
  void aTransactionInFlightWhenARebuildBeginsIsTakenOver() throws Exception {
    createSource();
    database.transaction(() -> {
      for (int i = 0; i < 100; i++)
        insert(CATS[i % 4], i % 3, i % 40, (long) i, "n" + i, i % 3, "I");
    });
    createAllViews();
    database.transaction(() -> insert("a", 0, 1, 1L, "x", 1, "I"));

    final RID moved;
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS r FROM I WHERE cat = 'b' LIMIT 1")) {
      moved = rs.next().getProperty("r");
    }
    final CountDownLatch scanning = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final List<Thread> rebuilds = new CopyOnWriteArrayList<>();
    MaterializedViewGroupedRefresher.scanObserver = rid -> {
      if (rebuilds.contains(Thread.currentThread())) {
        scanning.countDown();
        await(release);
      }
    };
    try {
      database.transaction(() -> {
        // registered before the first change, so it runs before the view's own callback: the transaction is past its commit point
        // and has not delivered its delta
        ((com.arcadedb.database.DatabaseInternal) database).getTransaction().addAfterCommitCallback(() -> {
          final Thread rebuild = new Thread(() -> database.getSchema().getMaterializedView("GSums").refresh());
          rebuilds.add(rebuild);
          rebuild.start();
          await(scanning);
        });
        ((Document) database.lookupByRID(moved, true)).modify().set("cat", "a").set("v", 25).save();
        insert("c", 1, 3, 3L, "y", 2, "I");
        database.command("sql", "DELETE FROM I WHERE cat = 'd' LIMIT 2");
      });
    } finally {
      release.countDown();
      for (final Thread rebuild : rebuilds)
        rebuild.join(TimeUnit.SECONDS.toMillis(60));
      MaterializedViewGroupedRefresher.scanObserver = null;
    }
    assertThat(rebuilds).hasSize(1);
    assertAllViewsMatch();
  }

  private static void await(final CountDownLatch latch) {
    try {
      latch.await(30, TimeUnit.SECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void randomTransaction(final Random random) {
    final boolean rollback = random.nextInt(10) == 0;
    try {
      database.begin();
      randomOperations(random, 1 + random.nextInt(6));
      if (rollback)
        database.rollback();
      else
        database.commit();
    } catch (final RuntimeException e) {
      // a unique key violated at the commit, or a statement that could not be taken back refusing it: the transaction is gone
      if (database.isTransactionActive())
        database.rollback();
    }
  }

  private void randomOperations(final Random random, final int count) {
    for (int op = 0; op < count; op++) {
      switch (random.nextInt(12)) {
      case 0, 1, 2 -> insertRandom(random);
      case 3, 4 -> {
        final Document found = randomRecord(random);
        if (found != null) {
          final MutableDocument doc = found.modify();
          randomize(random, doc);
          doc.save();
        }
      }
      case 5 -> {
        final Document found = randomRecord(random);
        if (found != null)
          found.delete();
      }
      case 6 -> database.command("sql", "UPDATE I SET cat = ?, v = ? WHERE sub = ? AND v < ?", randomCat(random), randomInt(random),
          random.nextInt(3), random.nextInt(40) - 20);
      case 7 -> database.command("sql", "DELETE FROM I WHERE cat = ? AND sub = ?", randomCat(random), random.nextInt(3));
      case 8 -> {
        // a statement that fails part way, after it updated a record: it is taken back, the transaction goes on
        try {
          database.command("sql", "UPDATE I SET v = v + 1, uid = 'duplicate' WHERE cat = ?", randomCat(random));
        } catch (final DuplicatedKeyException e) {
          // expected when it matched two records or more
        }
      }
      case 9 -> {
        // the same record changed twice in one transaction, and created then deleted in it
        final MutableDocument doc = insertRandom(random);
        randomize(random, doc);
        doc.save();
        if (random.nextBoolean())
          doc.delete();
      }
      case 10 -> database.command("sql", "UPDATE I SET k = ? WHERE sub = ?", random.nextBoolean() ? null : random.nextInt(3),
          random.nextInt(3));
      default -> {
        final Document found = randomRecord(random);
        if (found != null) {
          // a record whose group changes and changes back
          final MutableDocument doc = found.modify();
          final Object cat = doc.get("cat");
          doc.set("cat", "moved").save();
          doc.set("cat", cat).save();
        }
      }
      }
    }
  }

  private MutableDocument insertRandom(final Random random) {
    final MutableDocument doc = database.newDocument(random.nextInt(4) == 0 ? "J" : "I");
    doc.set("uid", "u" + uids.incrementAndGet());
    randomize(random, doc);
    return doc.save();
  }

  private void randomize(final Random random, final MutableDocument doc) {
    doc.set("cat", randomCat(random));
    doc.set("sub", random.nextInt(3));
    doc.set("v", random.nextInt(8) == 0 ? null : randomInt(random));
    doc.set("w", random.nextInt(8) == 0 ? null : (long) (random.nextInt(2000) - 1000) * 1_000_000_000L);
    doc.set("name", random.nextInt(8) == 0 ? null : "n" + random.nextInt(30));
    doc.set("k", random.nextInt(5) == 0 ? null : random.nextInt(3));
  }

  private static String randomCat(final Random random) {
    return CATS[random.nextInt(CATS.length)];
  }

  private static int randomInt(final Random random) {
    return random.nextInt(100) - 50;
  }

  /** A random record of the source, or null when there is none or another writer deleted it meanwhile. */
  private Document randomRecord(final Random random) {
    final long count = database.countType("I", true);
    if (count == 0)
      return null;
    final long skip = random.nextLong(count);
    final RID rid;
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS r FROM I SKIP " + skip + " LIMIT 1")) {
      if (!rs.hasNext())
        return null;
      rid = rs.next().getProperty("r");
    }
    try {
      return (Document) database.lookupByRID(rid, true);
    } catch (final RecordNotFoundException e) {
      return null;
    }
  }

  private void insert(final String cat, final Integer sub, final Integer v, final Long w, final String name, final Integer k,
      final String type) {
    final MutableDocument doc = database.newDocument(type);
    doc.set("uid", "u" + uids.incrementAndGet()).set("cat", cat).set("sub", sub).set("v", v).set("w", w).set("name", name)
        .set("k", k);
    doc.save();
  }

  private void createSource() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("I");
      type.createProperty("uid", Type.STRING);
      type.createProperty("cat", Type.STRING);
      type.createProperty("sub", Type.INTEGER);
      type.createProperty("v", Type.INTEGER);
      type.createProperty("w", Type.LONG);
      type.createProperty("name", Type.STRING);
      // k is schemaless
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "uid");
      database.getSchema().buildDocumentType().withName("J").withSuperType("I").create();
    });
  }

  private void createAllViews() {
    for (final Map.Entry<String, String> view : VIEWS.entrySet())
      createView(view.getKey(), view.getValue());
  }

  private MaterializedViewImpl createView(final String name, final String query) {
    database.transaction(() -> database.getSchema().buildMaterializedView().withName(name).withQuery(query)
        .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL).create());
    return (MaterializedViewImpl) database.getSchema().getMaterializedView(name);
  }

  private void assertAllViewsMatch() {
    for (final String name : VIEWS.keySet())
      if (database.getSchema().existsMaterializedView(name))
        assertViewMatchesQuery(name);
  }

  private void assertViewMatchesQuery(final String name) {
    assertThat(viewRows(name)).as(name).containsExactlyInAnyOrderElementsOf(queryRows(VIEWS.get(name)));
  }

  private long scans() {
    return ((Number) database.getStats().get("scanType")).longValue();
  }

  private List<Map<String, Object>> viewRows(final String name) {
    return rows("SELECT FROM " + name);
  }

  private List<Map<String, Object>> queryRows(final String query) {
    return rows(query);
  }

  private List<Map<String, Object>> rows(final String query) {
    final List<Map<String, Object>> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final Map<String, Object> values = new HashMap<>();
        for (final String property : row.getPropertyNames())
          if (!property.startsWith("@"))
            values.put(property, row.getProperty(property));
        rows.add(values);
      }
    }
    return rows;
  }
}
