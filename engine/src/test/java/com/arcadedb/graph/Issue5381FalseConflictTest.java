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
package com.arcadedb.graph;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduces and guards the fix for the "false conflict" reported in issue #5381 (reinforcing #5279): concurrent
 * transactions that each write DIFFERENT records which happen to be co-located on the SAME bucket page collide with
 * a page-level {@link ConcurrentModificationException} even though the records are logically unrelated. The
 * disjoint-slot merge (TX_PAGE_SLOT_MERGE) replays each transaction's own slot writes on top of the newer committed page
 * instead of failing the whole transaction, and only when the concurrent commit did not touch the same record.
 */
@Tag("slow")
class Issue5381FalseConflictTest extends TestHelper {
  private boolean savedSlotMerge;
  private int     savedThreshold;
  private long    savedMaxBytes;

  @BeforeEach
  void saveConfig() {
    savedSlotMerge = GlobalConfiguration.TX_PAGE_SLOT_MERGE.getValueAsBoolean();
    savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
    savedMaxBytes = GlobalConfiguration.TX_PAGE_SLOT_MERGE_MAX_BYTES.getValueAsLong();
  }

  @AfterEach
  void restoreConfig() {
    GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(savedSlotMerge);
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
    GlobalConfiguration.TX_PAGE_SLOT_MERGE_MAX_BYTES.setValue(savedMaxBytes);
  }

  /**
   * Pure disjoint-slot IN-PLACE UPDATE contention: N vertices packed onto a single bucket page, each updated by
   * exactly ONE thread (so no two threads ever touch the same record - every page conflict is provably false).
   * With attempts=1 the merge alone (no retry loop) must absorb the false conflicts, the merge counter must fire,
   * and every per-record value must be exactly its final write.
   */
  @Test
  void concurrentInPlaceUpdatesOfDistinctCoLocatedRecords() throws Exception {
    GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(true);

    final int records = 12;
    final int updatesPerRecord = 500;

    // Fixed-width value so every update is a same-size in-place overwrite (like #5381's RID->RID head-pointer
    // flip). A growing value would hit the record-growth path, which the merge deliberately does not cover.
    final RID[] rids = new RID[records];
    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc", 1).createProperty("tag", Type.STRING);
      for (int i = 0; i < records; i++) {
        final var doc = database.newDocument("Doc");
        doc.set("tag", String.format("%08d", 0));
        doc.save();
        rids[i] = doc.getIdentity();
      }
    });

    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    // Per-record last value this thread actually committed (single owner per record => no cross-thread races).
    final int[] lastCommitted = new int[records];
    final CountDownLatch start = new CountDownLatch(1);
    final List<Thread> threads = new ArrayList<>();

    for (int t = 0; t < records; t++) {
      final RID rid = rids[t];
      final int idx = t;
      final Thread thread = new Thread(() -> {
        try {
          start.await();
          for (int i = 1; i <= updatesPerRecord; i++) {
            final String value = String.format("%08d", i);
            try {
              database.transaction(() -> rid.asDocument(true).modify().set("tag", value).save(), true, 1);
              lastCommitted[idx] = i;
            } catch (final ConcurrentModificationException ignore) {
              // absorbed-or-not is asserted below via the merge counter and exact final state, not a ratio.
            }
          }
        } catch (final Throwable e) {
          errors.add(e);
        }
      }, "updater-" + t);
      threads.add(thread);
      thread.start();
    }

    start.countDown();
    for (final Thread thread : threads)
      thread.join();

    if (!errors.isEmpty())
      throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.getFirst(), errors.getFirst());

    // Feature actually worked: the merge fired at least once (0 would mean every false conflict was thrown).
    final long merges = ((DatabaseInternal) database).getPageManager().getStats().slotMerges;
    assertThat(merges).as("disjoint-slot update merge must fire").isGreaterThan(0);

    // Correctness, flake-free regardless of contention: each record holds EXACTLY its own last committed value
    // (no lost, torn, or cross-record write from a rebase). A single owner per record makes this exact.
    database.transaction(() -> {
      for (int i = 0; i < records; i++)
        assertThat(rids[i].asDocument(true).getString("tag")).isEqualTo(String.format("%08d", lastCommitted[i]));
    });
  }

  /**
   * Exercises the INSERT rebase path (trackRebasableInsert -> writeRecordAtSlot): one inserter thread appends
   * brand-new records into a single shared bucket page while several updater threads churn OTHER slots of that
   * same page in place. With attempts=1 the inserter can only keep committing if the merge replays its insert into
   * its still-free slot on the newer committed page; without it, every insert into the churning page would fail.
   */
  @Test
  void concurrentInsertIntoPageChurnedByOtherSlots() throws Exception {
    GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(true);

    final int hubs = 8;          // pre-existing records that share page 0 and get churned in place
    final int inserts = 300;     // small records, all land on the reused shared page(s)
    final int updatesPerHub = 400;

    final RID[] hub = new RID[hubs];
    database.transaction(() -> {
      database.getSchema().createDocumentType("Shared", 1).createProperty("tag", Type.STRING);
      for (int i = 0; i < hubs; i++) {
        final var d = database.newDocument("Shared");
        d.set("tag", String.format("%08d", 0));
        d.set("kind", "hub");
        d.save();
        hub[i] = d.getIdentity();
      }
    });

    final long mergesBefore = ((DatabaseInternal) database).getPageManager().getStats().slotMerges;
    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    final AtomicInteger insertCme = new AtomicInteger();
    final CountDownLatch start = new CountDownLatch(1);
    final List<Thread> threads = new ArrayList<>();

    // Churn threads: repeated same-size in-place updates of distinct hubs -> constant version bumps on the page.
    for (int t = 0; t < hubs; t++) {
      final RID h = hub[t];
      final Thread churn = new Thread(() -> {
        try {
          start.await();
          for (int i = 1; i <= updatesPerHub; i++) {
            final String v = String.format("%08d", i);
            try {
              database.transaction(() -> h.asDocument(true).modify().set("tag", v).save(), true, 1);
            } catch (final ConcurrentModificationException ignore) {
              // not under test here
            }
          }
        } catch (final Throwable e) {
          errors.add(e);
        }
      }, "churn-" + t);
      threads.add(churn);
    }

    // Single inserter: no insert-vs-insert self-collision, so its only conflicts are the churn threads' OTHER slots.
    final Thread inserter = new Thread(() -> {
      try {
        start.await();
        for (int i = 0; i < inserts; i++) {
          final int n = i;
          try {
            database.transaction(() -> {
              final var d = database.newDocument("Shared");
              d.set("tag", String.format("%08d", n));
              d.set("kind", "leaf");
              d.save();
            }, true, 1);
          } catch (final ConcurrentModificationException e) {
            insertCme.incrementAndGet();
          }
        }
      } catch (final Throwable e) {
        errors.add(e);
      }
    }, "inserter");
    threads.add(inserter);

    for (final Thread thread : threads)
      thread.start();
    start.countDown();
    for (final Thread thread : threads)
      thread.join();

    if (!errors.isEmpty())
      throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.getFirst(), errors.getFirst());

    final long merges = ((DatabaseInternal) database).getPageManager().getStats().slotMerges - mergesBefore;
    assertThat(merges).as("insert/update slot merges must fire on the churned shared page").isGreaterThan(0);

    // Insert-rebase proof, robust to CI load: with attempts=1 into a page churned by other slots, WITHOUT the
    // insert rebase nearly every insert would fail. Requiring a majority to commit is a wide, non-flaky bound that
    // still can only hold if writeRecordAtSlot is replaying the inserts.
    assertThat(insertCme.get()).as("most inserts into the churned page must be absorbed by the merge")
        .isLessThan(inserts / 2);

    // Every committed leaf must be present exactly once (no insert lost or duplicated by a rebase), hubs intact.
    database.transaction(() -> {
      assertThat(database.countType("Shared", false)).isEqualTo(hubs + inserts - insertCme.get());
      long leaves = 0;
      try (final var rs = database.query("SQL", "SELECT count(*) AS c FROM Shared WHERE kind = 'leaf'")) {
        leaves = rs.next().<Number>getProperty("c").longValue();
      }
      assertThat(leaves).isEqualTo(inserts - insertCme.get());
    });
  }

  /**
   * A record-GROWTH update (serialized size increases) on a page shared with other records must NOT be merged: it
   * shifts other slots, so it poisons the page and falls back to a normal retry. Several threads each grow their
   * OWN co-located record; correctness (each holds exactly its last committed, larger value) proves the growth
   * path was not rebased onto a stale page.
   */
  @Test
  void growingUpdatesOnCoLocatedRecordsFallBackAndStayCorrect() throws Exception {
    GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(true);

    final int records = 6;
    final int steps = 250;

    final RID[] rids = new RID[records];
    database.transaction(() -> {
      database.getSchema().createDocumentType("Grow", 1).createProperty("tag", Type.STRING);
      for (int i = 0; i < records; i++) {
        final var doc = database.newDocument("Grow");
        doc.set("tag", "");
        doc.save();
        rids[i] = doc.getIdentity();
      }
    });

    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    final CountDownLatch start = new CountDownLatch(1);
    final List<Thread> threads = new ArrayList<>();
    for (int t = 0; t < records; t++) {
      final RID rid = rids[t];
      final Thread thread = new Thread(() -> {
        try {
          start.await();
          for (int i = 1; i <= steps; i++) {
            final String value = "x".repeat(i); // strictly growing -> record-growth path -> poison + retry
            database.transaction(() -> rid.asDocument(true).modify().set("tag", value).save(), true, 50);
          }
        } catch (final Throwable e) {
          errors.add(e);
        }
      }, "grow-" + t);
      threads.add(thread);
      thread.start();
    }
    start.countDown();
    for (final Thread thread : threads)
      thread.join();

    if (!errors.isEmpty())
      throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.getFirst(), errors.getFirst());

    // With retries each grow commits; correct final length proves growth fell back cleanly (no torn/merged record).
    database.transaction(() -> {
      for (int i = 0; i < records; i++)
        assertThat(rids[i].asDocument(true).getString("tag")).isEqualTo("x".repeat(steps));
    });
  }

  /**
   * With the per-transaction retention cap set tiny, the merge disables itself mid-transaction and every touched
   * page falls back to plain MVCC. This must stay correct: no data loss or corruption, values still consistent.
   */
  @Test
  void tinyRetentionCapFallsBackWithoutCorruption() throws Exception {
    GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(true);
    GlobalConfiguration.TX_PAGE_SLOT_MERGE_MAX_BYTES.setValue(1L); // disable after the very first tracked image

    final int records = 8;
    final int updatesPerRecord = 200;

    final RID[] rids = new RID[records];
    database.transaction(() -> {
      database.getSchema().createDocumentType("Capped", 1).createProperty("tag", Type.STRING);
      for (int i = 0; i < records; i++) {
        final var doc = database.newDocument("Capped");
        doc.set("tag", String.format("%08d", 0));
        doc.save();
        rids[i] = doc.getIdentity();
      }
    });

    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    final CountDownLatch start = new CountDownLatch(1);
    final List<Thread> threads = new ArrayList<>();
    for (int t = 0; t < records; t++) {
      final RID rid = rids[t];
      final Thread thread = new Thread(() -> {
        try {
          start.await();
          for (int i = 1; i <= updatesPerRecord; i++) {
            final String value = String.format("%08d", i);
            // Retry here: with the merge capped off, contention resolves the old way.
            database.transaction(() -> rid.asDocument(true).modify().set("tag", value).save(), true, 50);
          }
        } catch (final Throwable e) {
          errors.add(e);
        }
      }, "capped-" + t);
      threads.add(thread);
      thread.start();
    }
    start.countDown();
    for (final Thread thread : threads)
      thread.join();

    if (!errors.isEmpty())
      throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.getFirst(), errors.getFirst());

    database.transaction(() -> {
      for (int i = 0; i < records; i++)
        assertThat(rids[i].asDocument(true).getString("tag")).isEqualTo(String.format("%08d", updatesPerRecord));
    });
  }

  /**
   * A genuine SAME-record conflict must still raise ConcurrentModificationException: the merge must never silently
   * overwrite (lose) a concurrent update to the very record this transaction is updating.
   */
  @Test
  void concurrentUpdatesOfTheSameRecordStillConflict() throws Exception {
    GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(true);

    final RID[] rid = new RID[1];
    database.transaction(() -> {
      database.getSchema().createDocumentType("One", 1).createProperty("n", Type.INTEGER);
      final var d = database.newDocument("One");
      d.set("n", 0);
      d.save();
      rid[0] = d.getIdentity();
    });

    // Our transaction reads the base and stages a write to record 'rid'.
    database.begin();
    final var a = rid[0].asDocument(true).modify();
    a.set("n", 100);
    a.save();

    // A competing transaction (another thread) commits a write to the SAME record while ours is still open.
    final Thread other = new Thread(() -> database.transaction(() -> rid[0].asDocument(true).modify().set("n", 200).save()));
    other.start();
    other.join();

    // Committing ours must raise a conflict (same record changed under us), NOT be merged away.
    assertThat(database.isTransactionActive()).isTrue();
    try {
      database.commit();
      throw new AssertionError("Expected a ConcurrentModificationException on a genuine same-record conflict");
    } catch (final ConcurrentModificationException expected) {
      // correct: the same-record conflict surfaced instead of being merged
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }

    // The competitor's write must be the one that survived.
    database.transaction(() -> assertThat(rid[0].asDocument(true).<Integer>getInteger("n")).isEqualTo(200));
  }

  /**
   * The #5381 shape end-to-end: several hot HUB vertices packed onto a single vertex bucket, each fed edges by its
   * own thread on the classic (non-striped) layout. The head-pointer flip that rewrites a hub vertex whenever its
   * head chunk fills makes DIFFERENT hubs on the same page collide falsely. Leaf/edge inserts use many buckets so
   * the only interesting contention is the head-flip false conflict. With the merge and a small retry budget the
   * workload must complete with EVERY edge preserved and the merge must have fired.
   */
  @Test
  void hotVertexEdgeAppendPreservesAllEdges() throws Exception {
    GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(true);
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(0); // classic layout, as in a 26.7.x deployment

    final int hubs = 6;
    final int edgesPerHub = 500;

    database.transaction(() -> {
      database.getSchema().createVertexType("Hub", 1).createProperty("id", Type.INTEGER); // single bucket: hubs share pages
      database.getSchema().createVertexType("Leaf", 16);
      database.getSchema().createEdgeType("LINK", 16);
    });

    final RID[] hub = new RID[hubs];
    database.transaction(() -> {
      for (int i = 0; i < hubs; i++) {
        final MutableVertex v = database.newVertex("Hub");
        v.set("id", i);
        v.save();
        hub[i] = v.getIdentity();
      }
    });

    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    final CountDownLatch start = new CountDownLatch(1);
    final List<Thread> threads = new ArrayList<>();

    for (int t = 0; t < hubs; t++) {
      final RID h = hub[t];
      final Thread thread = new Thread(() -> {
        try {
          start.await();
          for (int i = 0; i < edgesPerHub; i++)
            database.transaction(() -> {
              final MutableVertex leaf = database.newVertex("Leaf");
              leaf.save();
              h.asVertex(true).newEdge("LINK", leaf);
            }, true, 10);
        } catch (final Throwable e) {
          errors.add(e);
        }
      }, "hub-" + t);
      threads.add(thread);
      thread.start();
    }

    start.countDown();
    for (final Thread thread : threads)
      thread.join();

    if (!errors.isEmpty())
      throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.getFirst(), errors.getFirst());

    final long merges = ((DatabaseInternal) database).getPageManager().getStats().slotMerges;
    assertThat(merges).as("head-flip false conflicts must be merged").isGreaterThan(0);

    database.transaction(() -> {
      long total = 0;
      for (int i = 0; i < hubs; i++) {
        final long c = hub[i].asVertex(true).countEdges(Vertex.DIRECTION.OUT, "LINK");
        assertThat(c).as("hub " + i + " must keep all its edges").isEqualTo(edgesPerHub);
        total += c;
      }
      assertThat(total).isEqualTo((long) hubs * edgesPerHub);
    });
  }

  /**
   * Regression for the multi-page corruption gap: a multi-page record's continuation chunks land on reused pages
   * via inline record-table writes that bypass the tracking hooks. If such a page also carried a tracked small
   * insert, an erroneous rebase would replay only the insert and drop the chunk, silently corrupting the large
   * record. The fix poisons every page a multi-page write touches. Here each transaction inserts BOTH a small
   * record and a large (multi-page) record into a single shared bucket under concurrency, so small inserts and
   * chunk writes co-locate and pages conflict; every large record must still read back intact.
   */
  @Test
  void multiPageRecordCoLocatedWithTrackedInsertStaysIntact() throws Exception {
    GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(true);

    // Deterministic layout: a 16 KB page holds ~8 KB of usable content. Seed page 0 with a ~3 KB filler so it has
    // ~5 KB free. One transaction then, on page 0: inserts a small record (a TRACKED rebasable insert) and a large
    // record whose FIRST chunk fills a fresh page while its trailing chunk (~4 KB) reuses page 0's free space. A
    // competitor bumps page 0's version, forcing this transaction to conflict on page 0. Without the fix, page 0 is
    // still "rebasable" (only the small insert is tracked, the chunk write never poisoned it), so the rebase replays
    // the insert and silently drops the chunk - corrupting the large record. The fix poisons page 0 on the chunk
    // write, so the transaction cleanly retries and the large record stays intact.
    final int pageSize = 16 * 1024;
    final String bigPayload = "B".repeat(12_000);   // spans 2 pages; trailing chunk fits page 0's ~5 KB free
    final String filler = "F".repeat(3_000);

    final RID[] fillerRid = new RID[1];
    database.transaction(() -> {
      database.getSchema().createDocumentType("MP", 1, pageSize);
      final var f = database.newDocument("MP").set("kind", "filler").set("s", filler);
      f.save();
      fillerRid[0] = f.getIdentity();
    });

    final CountDownLatch inserted = new CountDownLatch(1);
    final CountDownLatch competitorDone = new CountDownLatch(1);
    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    final RID[] bigRid = new RID[1];

    final Thread main = new Thread(() -> {
      try {
        for (int attempt = 0; attempt < 20; attempt++) {
          database.begin();
          database.newDocument("MP").set("kind", "small").set("s", "a").save(); // tracked insert on page 0
          final var big = database.newDocument("MP").set("kind", "big").set("blob", bigPayload);
          big.save(); // multi-page; the trailing chunk lands on page 0 alongside the tracked insert
          if (attempt == 0) {
            inserted.countDown();      // let the competitor bump page 0
            competitorDone.await();    // ...and wait until it has committed, so our commit conflicts on page 0
          }
          try {
            database.commit();
            bigRid[0] = big.getIdentity();
            break;
          } catch (final ConcurrentModificationException e) {
            database.rollback(); // expected WITH the fix on attempt 0: page 0 poisoned -> clean retry
          }
        }
      } catch (final Throwable e) {
        errors.add(e);
      }
    }, "main");

    final Thread competitor = new Thread(() -> {
      try {
        inserted.await();
        // Same-size in-place update of the filler on page 0 -> bumps page 0's committed version.
        database.transaction(() -> fillerRid[0].asDocument(true).modify().set("s", filler).save(), true, 50);
      } catch (final Throwable e) {
        errors.add(e);
      } finally {
        competitorDone.countDown();
      }
    }, "competitor");

    main.start();
    competitor.start();
    main.join();
    competitor.join();

    if (!errors.isEmpty())
      throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.getFirst(), errors.getFirst());

    // The large record must read back byte-for-byte: a dropped chunk would truncate it (or fail to deserialize).
    database.transaction(() -> {
      assertThat(bigRid[0]).as("the big record must have committed").isNotNull();
      assertThat(bigRid[0].asDocument(true).getString("blob")).isEqualTo(bigPayload);
    });
  }

  /**
   * A merged in-place update must keep secondary indexes consistent. Each record's INDEXED property is updated by
   * its own thread (distinct co-located records sharing a page => false conflicts absorbed by the merge). Because
   * the rebase preserves the RID and re-applies the same bytes the transaction computed, the index delete/insert
   * entries staged for that RID stay valid: every record must be findable by its final value via the index, with
   * no stale entries pointing at old values.
   */
  @Test
  void mergedInPlaceUpdateKeepsIndexConsistent() throws Exception {
    GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(true);

    final int records = 10;
    final int updatesPerRecord = 300;

    final RID[] rids = new RID[records];
    database.transaction(() -> {
      database.getSchema().createDocumentType("Idx", 1).createProperty("k", Type.STRING);
      database.command("SQL", "CREATE INDEX ON Idx (k) NOTUNIQUE");
      for (int i = 0; i < records; i++) {
        final var doc = database.newDocument("Idx");
        doc.set("k", String.format("%02d-%08d", i, 0)); // fixed width => same-size in-place updates
        doc.save();
        rids[i] = doc.getIdentity();
      }
    });

    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    final int[] lastCommitted = new int[records];
    final CountDownLatch start = new CountDownLatch(1);
    final List<Thread> threads = new ArrayList<>();
    for (int t = 0; t < records; t++) {
      final RID rid = rids[t];
      final int idx = t;
      final Thread thread = new Thread(() -> {
        try {
          start.await();
          for (int i = 1; i <= updatesPerRecord; i++) {
            final String v = String.format("%02d-%08d", idx, i); // globally unique per (record, iteration)
            try {
              database.transaction(() -> rid.asDocument(true).modify().set("k", v).save(), true, 1);
              lastCommitted[idx] = i;
            } catch (final ConcurrentModificationException ignore) {
              // fine; correctness is checked against the actually-committed value below
            }
          }
        } catch (final Throwable e) {
          errors.add(e);
        }
      }, "idx-" + t);
      threads.add(thread);
      thread.start();
    }
    start.countDown();
    for (final Thread thread : threads)
      thread.join();

    if (!errors.isEmpty())
      throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.getFirst(), errors.getFirst());

    final long merges = ((DatabaseInternal) database).getPageManager().getStats().slotMerges;
    assertThat(merges).as("in-place update merge must fire").isGreaterThan(0);

    database.transaction(() -> {
      for (int i = 0; i < records; i++) {
        final String finalValue = String.format("%02d-%08d", i, lastCommitted[i]);

        // The record's stored value matches its last committed write...
        assertThat(rids[i].asDocument(true).getString("k")).isEqualTo(finalValue);

        // ...and the INDEX finds exactly that record by that value (no lost/duplicated index entry).
        long hits = 0;
        RID found = null;
        try (final var rs = database.query("SQL", "SELECT FROM Idx WHERE k = ?", finalValue)) {
          while (rs.hasNext()) {
            found = rs.next().getIdentity().orElse(null);
            hits++;
          }
        }
        assertThat(hits).as("record " + i + " must be found exactly once by index").isEqualTo(1);
        assertThat(found).isEqualTo(rids[i]);

        // And no stale index entry survives for a superseded value.
        if (lastCommitted[i] > 1)
          try (final var rs = database.query("SQL", "SELECT FROM Idx WHERE k = ?",
              String.format("%02d-%08d", i, lastCommitted[i] - 1))) {
            assertThat(rs.hasNext()).as("stale index value for record " + i + " must be gone").isFalse();
          }
      }
    });
  }

  /**
   * A DELETE on a page shared with other records must poison it (a delete frees a slot and can relink placeholder
   * chains), so it is never merged - it falls back to a normal retry. Threads delete their own co-located victims
   * while other threads update co-located survivors in place; the survivors keep exact values and every victim is
   * gone, proving the delete fell back cleanly rather than being rebased.
   */
  @Test
  void deleteOnSharedPageFallsBackAndStaysCorrect() throws Exception {
    GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(true);

    final int pairs = 6;
    final int updates = 250;

    final RID[] survivor = new RID[pairs];
    final RID[] victim = new RID[pairs];
    database.transaction(() -> {
      database.getSchema().createDocumentType("Mix", 1).createProperty("tag", Type.STRING);
      for (int i = 0; i < pairs; i++) {
        final var s = database.newDocument("Mix");
        s.set("tag", String.format("%08d", 0));
        s.set("role", "survivor");
        s.save();
        survivor[i] = s.getIdentity();
        final var v = database.newDocument("Mix");
        v.set("role", "victim");
        v.save();
        victim[i] = v.getIdentity();
      }
    });

    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    final int[] lastCommitted = new int[pairs];
    final CountDownLatch start = new CountDownLatch(1);
    final List<Thread> threads = new ArrayList<>();

    for (int t = 0; t < pairs; t++) {
      final RID s = survivor[t];
      final int idx = t;
      threads.add(new Thread(() -> {
        try {
          start.await();
          for (int i = 1; i <= updates; i++) {
            final String v = String.format("%08d", i);
            try {
              database.transaction(() -> s.asDocument(true).modify().set("tag", v).save(), true, 1);
              lastCommitted[idx] = i;
            } catch (final ConcurrentModificationException ignore) {
            }
          }
        } catch (final Throwable e) {
          errors.add(e);
        }
      }, "survivor-" + t));

      final RID vic = victim[t];
      threads.add(new Thread(() -> {
        try {
          start.await();
          // Delete the co-located victim (poisons the shared page); retry until it commits.
          database.transaction(() -> vic.asDocument(true).delete(), true, 50);
        } catch (final Throwable e) {
          errors.add(e);
        }
      }, "deleter-" + t));
    }

    for (final Thread thread : threads)
      thread.start();
    start.countDown();
    for (final Thread thread : threads)
      thread.join();

    if (!errors.isEmpty())
      throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.getFirst(), errors.getFirst());

    database.transaction(() -> {
      // Survivors keep their exact last committed value; victims are gone; nothing corrupted.
      for (int i = 0; i < pairs; i++)
        assertThat(survivor[i].asDocument(true).getString("tag")).isEqualTo(String.format("%08d", lastCommitted[i]));
      assertThat(database.countType("Mix", false)).isEqualTo(pairs);
      try (final var rs = database.query("SQL", "SELECT count(*) AS c FROM Mix WHERE role = 'victim'")) {
        assertThat(rs.next().<Number>getProperty("c").longValue()).isZero();
      }
    });
  }
}
