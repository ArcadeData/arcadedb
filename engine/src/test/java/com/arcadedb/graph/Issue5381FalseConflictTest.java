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
 * disjoint-slot merge (TX_SLOT_MERGE) replays each transaction's own slot writes on top of the newer committed page
 * instead of failing the whole transaction, and only when the concurrent commit did not touch the same record.
 */
@Tag("slow")
class Issue5381FalseConflictTest extends TestHelper {
  private boolean savedSlotMerge;
  private int     savedThreshold;
  private long    savedMaxBytes;

  @BeforeEach
  void saveConfig() {
    savedSlotMerge = GlobalConfiguration.TX_SLOT_MERGE.getValueAsBoolean();
    savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
    savedMaxBytes = GlobalConfiguration.TX_SLOT_MERGE_MAX_BYTES.getValueAsLong();
  }

  @AfterEach
  void restoreConfig() {
    GlobalConfiguration.TX_SLOT_MERGE.setValue(savedSlotMerge);
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
    GlobalConfiguration.TX_SLOT_MERGE_MAX_BYTES.setValue(savedMaxBytes);
  }

  /**
   * Pure disjoint-slot IN-PLACE UPDATE contention: N vertices packed onto a single bucket page, each updated by
   * exactly ONE thread (so no two threads ever touch the same record - every page conflict is provably false).
   * With attempts=1 the merge alone (no retry loop) must absorb the false conflicts, the merge counter must fire,
   * and every per-record value must be exactly its final write.
   */
  @Test
  void concurrentInPlaceUpdatesOfDistinctCoLocatedRecords() throws Exception {
    GlobalConfiguration.TX_SLOT_MERGE.setValue(true);

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
    final AtomicInteger cme = new AtomicInteger();
    final CountDownLatch start = new CountDownLatch(1);
    final List<Thread> threads = new ArrayList<>();

    for (int t = 0; t < records; t++) {
      final RID rid = rids[t];
      final Thread thread = new Thread(() -> {
        try {
          start.await();
          for (int i = 1; i <= updatesPerRecord; i++) {
            final String value = String.format("%08d", i);
            try {
              database.transaction(() -> rid.asDocument(true).modify().set("tag", value).save(), true, 1);
            } catch (final ConcurrentModificationException e) {
              cme.incrementAndGet();
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

    final long merges = ((DatabaseInternal) database).getPageManager().getStats().slotMerges;
    assertThat(merges).as("disjoint-slot update merge must fire").isGreaterThan(0);

    // The whole point: false conflicts between DISTINCT records on one page must be absorbed, not retried away.
    assertThat(cme.get()).as("false update conflicts should be nearly eliminated by the merge")
        .isLessThan(records * updatesPerRecord / 20);

    // Because a single thread owns each record and its last successful write is updatesPerRecord (attempts=1 means
    // a lost write only skips one value, never rolls one back), every record ends at its final applied value.
    database.transaction(() -> {
      for (int i = 0; i < records; i++)
        assertThat(rids[i].asDocument(true).getString("tag")).isEqualTo(String.format("%08d", updatesPerRecord));
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
    GlobalConfiguration.TX_SLOT_MERGE.setValue(true);

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

    // With the insert rebase, an isolated insert commit into the churning page rarely fails: attempts=1 succeeds.
    assertThat(insertCme.get()).as("inserts into the churned page should be absorbed by the merge")
        .isLessThan(inserts / 10);

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
   * With the per-transaction retention cap set tiny, the merge disables itself mid-transaction and every touched
   * page falls back to plain MVCC. This must stay correct: no data loss or corruption, values still consistent.
   */
  @Test
  void tinyRetentionCapFallsBackWithoutCorruption() throws Exception {
    GlobalConfiguration.TX_SLOT_MERGE.setValue(true);
    GlobalConfiguration.TX_SLOT_MERGE_MAX_BYTES.setValue(1L); // disable after the very first tracked image

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
    GlobalConfiguration.TX_SLOT_MERGE.setValue(true);

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
    GlobalConfiguration.TX_SLOT_MERGE.setValue(true);
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
}
