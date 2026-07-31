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
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.exception.ConcurrentModificationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #5596: both commit-time page merges - the commutative edge-append merge and the disjoint-slot merge - drop
 * this transaction's whole image of a conflicting page and replay only its tracked writes on top of the newer
 * committed version. That is sound ONLY if every byte the transaction wrote to that page belongs to a tracked write.
 * Before this fix the invariant was maintained by hand, through {@code poisonEdgeAppendPage}/
 * {@code poisonSlotRebasePage} calls scattered across the writers: a writer that forgot one committed a page from
 * which its own change had silently vanished - a lost write on the leader, replicated faithfully to every follower.
 * <p>
 * The merges now have to PROVE their coverage: a page is re-derivable only when every modification was made inside a
 * declaration naming that merge ({@code MutablePage.beginCoveredWrite}). These tests use the synthetic writer the
 * issue asks for - a transaction that dirties bytes belonging to no tracked write at all - once against each merge,
 * so they hold no matter which poison calls exist, and pin that the merges still fire on a fully covered page.
 * <p>
 * DELIBERATELY NOT {@code @Tag("slow")}, unlike the contention suites it sits next to
 * ({@code Issue5381FalseConflictTest}, {@code EdgeAppendMergeRaceTest}, ...). Those run thousands of transactions;
 * these are sized to the smallest contention that reproduces, and the whole class measures ~0.37s (heaviest method
 * 0.19s; the 16-thread growth/delete one 0.04s) - well inside a regular CI run. It belongs there: a merge that stops
 * firing because a writer forgot its declaration is a silent throughput regression no correctness assertion catches,
 * so the merge-counter and coverage-decline assertions below have to run on every build to be worth having.
 * Re-measure before tagging this class, rather than tagging it by family resemblance.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5596MergeCoverageTest extends TestHelper {
  private static final String MARKER = "ZZZZZZZZ";
  private static final String POKED  = "QQQQQQQQ";

  private boolean savedEdgeMerge;
  private boolean savedSlotMerge;
  private int     savedThreshold;

  @BeforeEach
  void saveConfig() {
    savedEdgeMerge = GlobalConfiguration.GRAPH_EDGE_APPEND_MERGE.getValueAsBoolean();
    savedSlotMerge = GlobalConfiguration.TX_PAGE_SLOT_MERGE.getValueAsBoolean();
    savedThreshold = GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.getValueAsInteger();
    GlobalConfiguration.GRAPH_EDGE_APPEND_MERGE.setValue(true);
    GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(true);
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(0); // classic (non-striped) edge lists
  }

  @AfterEach
  void restoreConfig() {
    GlobalConfiguration.GRAPH_EDGE_APPEND_MERGE.setValue(savedEdgeMerge);
    GlobalConfiguration.TX_PAGE_SLOT_MERGE.setValue(savedSlotMerge);
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(savedThreshold);
  }

  /**
   * Three hubs of one bucket, so their OUT edge-list head chunks share the first page of the single shared
   * {@code Hub_out_edges} segments file. Distinct leaf/edge types per writer keep every OTHER file uncontended.
   */
  private void createSchema() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Hub", 1);
      database.getSchema().createVertexType("LeafA", 1);
      database.getSchema().createVertexType("LeafC", 1);
      database.getSchema().createEdgeType("LinkA", 1);
      database.getSchema().createEdgeType("LinkC", 1);
    });
  }

  private RID newHubWithOneEdge(final String leafType, final String edgeType) {
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("Hub").save();
      final MutableVertex leaf = database.newVertex(leafType).save();
      hub.newEdge(edgeType, leaf);
      rid[0] = hub.getIdentity();
    });
    return rid[0];
  }

  private RID outHeadChunk(final RID hub) {
    final RID[] chunk = new RID[1];
    database.transaction(() -> chunk[0] = ((VertexInternal) hub.asVertex(true)).getOutEdgesHeadChunk());
    return chunk[0];
  }

  /** Page merges declined because the page could not prove its coverage (#5596). */
  private long declinedByCoverage() {
    return ((DatabaseInternal) database).getPageManager().getStats().mergesDeclinedByCoverage;
  }

  private void assertSamePage(final RID a, final RID b) {
    assertThat(a.getBucketId()).as("same segments bucket").isEqualTo(b.getBucketId());
    final int maxRecords = ((LocalBucket) database.getSchema().getBucketById(a.getBucketId())).getMaxRecordsInPage();
    assertThat(a.getPosition() / maxRecords).as("same segments page").isEqualTo(b.getPosition() / maxRecords);
  }

  /**
   * Creates, in its own committed transaction, an edge-list chunk no vertex points to, carrying {@code marker} in
   * its otherwise unused tail. It lands on the same segments page as the hubs' head chunks, so the test has bytes it
   * can poke there without breaking any edge list.
   */
  private RID createOrphanChunk(final int segmentsBucketId, final String marker) {
    final RID[] rid = new RID[1];
    final String bucketName = database.getSchema().getBucketById(segmentsBucketId).getName();
    database.transaction(() -> {
      final MutableEdgeSegment orphan = new MutableEdgeSegment((DatabaseInternal) database, 128);
      // Straight into the chunk's unused tail: no edge ever occupies it, so the poke below cannot corrupt a list.
      orphan.getBuffer().putByteArray(64, marker.getBytes());
      ((DatabaseInternal) database).createRecord(orphan, bucketName);
      rid[0] = orphan.getIdentity();
    });
    return rid[0];
  }

  /** Reads the marker back from the COMMITTED page, so a merged-away write cannot hide behind a cached record. */
  private String readMarker(final PageId pageId, final int pageSize) {
    final String[] value = new String[1];
    database.transaction(() -> {
      try {
        final BasePage page = ((DatabaseInternal) database).getTransaction().getPage(pageId, pageSize);
        final byte[] found = new byte[MARKER.length()];
        final int offset = findMarkerOffset(page, POKED.getBytes());
        page.readByteArray(offset >= 0 ? offset : findMarkerOffset(page, MARKER.getBytes()), found);
        value[0] = new String(found);
      } catch (final IOException e) {
        throw new RuntimeException(e);
      }
    });
    return value[0];
  }

  /**
   * The synthetic writer the issue asks for, on the edge-append side: a transaction registers a tracked in-chunk
   * append AND dirties bytes of a co-located record on the same segments page that no tracked append accounts for -
   * standing in for the inline record-table writes of the multi-page writers, a bulk load, or any writer that does
   * not know the merge exists. A competitor then bumps the shared page's version.
   * <p>
   * Without the coverage proof the page still looks rebasable from the tracked append alone: the merge reloads the
   * committed page, replays the append and commits - those bytes are gone with no error at all. With the proof the
   * merge declines, the transaction retries, and the write lands. Deliberately independent of the poison calls: an
   * undeclared writer is refused whether or not anybody remembered to exclude it.
   */
  @Test
  void anUndeclaredPageWriteIsNeverEdgeMergedAway() throws Exception {
    createSchema();

    final RID hubA = newHubWithOneEdge("LeafA", "LinkA");   // the tracked in-chunk append
    final RID hubC = newHubWithOneEdge("LeafC", "LinkC");   // the competitor's append

    final RID chunkA = outHeadChunk(hubA);
    final RID chunkC = outHeadChunk(hubC);
    // A chunk no vertex points to, so poking its bytes cannot break the graph: the marker is written into its
    // otherwise unused tail, and read back raw.
    final RID chunkB = createOrphanChunk(chunkA.getBucketId(), MARKER);
    assertSamePage(chunkA, chunkB);
    assertSamePage(chunkA, chunkC);

    final LocalBucket segments = (LocalBucket) database.getSchema().getBucketById(chunkA.getBucketId());
    final PageId pageId = new PageId(database, segments.getFileId(),
        (int) (chunkA.getPosition() / segments.getMaxRecordsInPage()));

    final long declinedBefore = declinedByCoverage();
    final CountDownLatch mainTxWritesDone = new CountDownLatch(1);
    final CountDownLatch bumpCommitted = new CountDownLatch(1);
    final List<Throwable> errors = new CopyOnWriteArrayList<>();

    // Competitor: an in-chunk append to hubC, i.e. a plain version bump of the shared segments page.
    final Thread bumper = new Thread(() -> {
      try {
        mainTxWritesDone.await();
        database.transaction(() -> {
          final MutableVertex leaf = database.newVertex("LeafC").save();
          hubC.asVertex(true).newEdge("LinkC", leaf);
        }, true, 50);
      } catch (final Throwable e) {
        errors.add(e);
      } finally {
        bumpCommitted.countDown();
      }
    }, "bumper");
    bumper.start();

    boolean committed = false;
    for (int attempt = 0; attempt < 20 && !committed; attempt++) {
      database.begin();
      try {
        final MutableVertex leaf = database.newVertex("LeafA").save();
        hubA.asVertex(true).newEdge("LinkA", leaf);   // TRACKED append on the shared page

        // The UNDECLARED writer: bytes on the very same page that no tracked append accounts for.
        final MutablePage page = ((DatabaseInternal) database).getTransaction()
            .getPageToModify(pageId, segments.getPageSize(), false);
        final int markerOffset = findMarker(page);
        assertThat(markerOffset).as("the marker must be locatable on the segments page").isGreaterThan(0);
        page.writeByteArray(markerOffset, POKED.getBytes());

        if (attempt == 0) {
          mainTxWritesDone.countDown();
          bumpCommitted.await();
        }
        database.commit();
        committed = true;
      } catch (final ConcurrentModificationException expected) {
        // WITH the fix this is the outcome of attempt 0: the merge refuses the page and the transaction retries.
        if (database.isTransactionActive())
          database.rollback();
      }
    }
    bumper.join();

    if (!errors.isEmpty())
      throw new AssertionError("competitor failed: " + errors.getFirst(), errors.getFirst());

    assertThat(committed).as("the transaction must eventually commit").isTrue();

    // The heart of the issue: an edge-merged page would have discarded these bytes without a word.
    assertThat(readMarker(pageId, segments.getPageSize())).as("the undeclared write must not have been merged away")
        .isEqualTo(POKED);

    // ...and the coverage proof is what refused the page, not some unrelated conflict.
    assertThat(declinedByCoverage() - declinedBefore).as("the merge must have been declined for lack of coverage")
        .isGreaterThan(0);

    // ...and neither writer's append may be lost either.
    database.transaction(() -> {
      assertThat(hubA.asVertex(true).countEdges(Vertex.DIRECTION.OUT, "LinkA")).isEqualTo(2);
      assertThat(hubC.asVertex(true).countEdges(Vertex.DIRECTION.OUT, "LinkC")).isEqualTo(2);
    });
  }

  /**
   * The other half of the guard: the coverage proof must not be so tight that it refuses everything. The same
   * shared-page contention WITHOUT the undeclared writer must still be absorbed by the merge - the counter has to
   * move, and every edge has to survive.
   */
  @Test
  void aCleanAppendConflictIsStillMerged() throws Exception {
    createSchema();

    final RID hubA = newHubWithOneEdge("LeafA", "LinkA");
    final RID hubC = newHubWithOneEdge("LeafC", "LinkC");
    assertSamePage(outHeadChunk(hubA), outHeadChunk(hubC));

    final long mergesBefore = ((DatabaseInternal) database).getPageManager().getStats().edgeAppendMerges;
    final long declinedBefore = declinedByCoverage();

    final int rounds = 40;
    final CountDownLatch start = new CountDownLatch(1);
    final List<Throwable> errors = new CopyOnWriteArrayList<>();

    final Thread other = new Thread(() -> {
      try {
        start.await();
        for (int i = 0; i < rounds; i++)
          database.transaction(() -> {
            final MutableVertex leaf = database.newVertex("LeafC").save();
            hubC.asVertex(true).newEdge("LinkC", leaf);
          }, true, 50);
      } catch (final Throwable e) {
        errors.add(e);
      }
    }, "other");
    other.start();

    try {
      start.countDown();
      for (int i = 0; i < rounds; i++)
        database.transaction(() -> {
          final MutableVertex leaf = database.newVertex("LeafA").save();
          hubA.asVertex(true).newEdge("LinkA", leaf);
        }, true, 50);
    } finally {
      other.join();
    }

    if (!errors.isEmpty())
      throw new AssertionError("second writer failed: " + errors.getFirst(), errors.getFirst());

    final long merges = ((DatabaseInternal) database).getPageManager().getStats().edgeAppendMerges - mergesBefore;
    assertThat(merges).as("the edge-append merge must still fire on a fully covered page").isGreaterThan(0);
    // The load-bearing half: a writer that forgot its declaration would show up HERE, as declines instead of merges.
    assertThat(declinedByCoverage() - declinedBefore)
        .as("an append-only page must never be declined for lack of coverage").isZero();

    database.transaction(() -> {
      assertThat(hubA.asVertex(true).countEdges(Vertex.DIRECTION.OUT, "LinkA")).isEqualTo(1 + rounds);
      assertThat(hubC.asVertex(true).countEdges(Vertex.DIRECTION.OUT, "LinkC")).isEqualTo(1 + rounds);
    });
  }

  /**
   * The same over-tightening guard for the disjoint-slot merge, on the two shapes whose declarations are the easiest
   * to get wrong because they write far more than one record's bytes: a record GROWTH (which shifts the records that
   * follow and rewrites their slot-table offsets) and a plain record DELETE (#5569). Both are fully declared, so on a
   * page several writers contend for the merge must keep firing and NOTHING may be declined for lack of coverage - a
   * missing declaration on either would silently turn absorbed contention back into retries, which no correctness
   * assertion would notice.
   */
  @Test
  void cleanGrowthAndDeleteConflictsAreStillMerged() throws Exception {
    final int records = 8;
    final int steps = 60;

    final RID[] grow = new RID[records];
    final RID[] victim = new RID[records];
    database.transaction(() -> {
      database.getSchema().createDocumentType("Mixed", 1);
      for (int i = 0; i < records; i++) {
        grow[i] = database.newDocument("Mixed").set("role", "grow").set("tag", "").save().getIdentity();
        victim[i] = database.newDocument("Mixed").set("role", "victim").save().getIdentity();
      }
    });

    final long mergesBefore = ((DatabaseInternal) database).getPageManager().getStats().txPageSlotMerges;
    final long declinedBefore = declinedByCoverage();
    final CountDownLatch start = new CountDownLatch(1);
    final List<Throwable> errors = new CopyOnWriteArrayList<>();
    final List<Thread> threads = new java.util.ArrayList<>();

    for (int t = 0; t < records; t++) {
      final RID g = grow[t];
      threads.add(new Thread(() -> {
        try {
          start.await();
          for (int i = 1; i <= steps; i++) {
            final String value = "x".repeat(i); // strictly growing -> the in-page growth declaration
            database.transaction(() -> g.asDocument(true).modify().set("tag", value).save(), true, 50);
          }
        } catch (final Throwable e) {
          errors.add(e);
        }
      }, "grow-" + t));

      final RID v = victim[t];
      threads.add(new Thread(() -> {
        try {
          start.await();
          database.transaction(() -> v.asDocument(true).delete(), true, 50);
        } catch (final Throwable e) {
          errors.add(e);
        }
      }, "delete-" + t));
    }

    for (final Thread thread : threads)
      thread.start();
    start.countDown();
    for (final Thread thread : threads)
      thread.join();

    if (!errors.isEmpty())
      throw new AssertionError(errors.size() + " thread(s) failed, first: " + errors.getFirst(), errors.getFirst());

    final long merges = ((DatabaseInternal) database).getPageManager().getStats().txPageSlotMerges - mergesBefore;
    assertThat(merges).as("growth and delete slot merges must still fire on fully covered pages").isGreaterThan(0);
    assertThat(declinedByCoverage() - declinedBefore)
        .as("a page of declared growths and deletes must never be declined").isZero();

    // And the writes themselves are intact: every grower reached its final length, every victim is gone.
    database.transaction(() -> {
      for (int i = 0; i < records; i++)
        assertThat(grow[i].asDocument(true).getString("tag")).isEqualTo("x".repeat(steps));
      assertThat(database.countType("Mixed", false)).isEqualTo(records);
    });
  }

  /**
   * The disjoint-slot merge half of the same guard, with the synthetic writer the issue asks for: a transaction
   * makes ONE tracked in-place record update on a page and then dirties bytes that belong to no tracked slot write
   * at all (here, inside the content of a co-located record - a stand-in for the inline record-table writes of the
   * multi-page writers, or for any future writer that does not know the merge exists). A competitor bumps the page's
   * version. Replaying the tracked update alone on the newer committed page would silently discard those bytes; the
   * coverage proof refuses the page instead, so the transaction retries and the write survives.
   */
  @Test
  void anUndeclaredPageWriteIsNeverSlotMergedAway() throws Exception {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Poke", 1);
      database.newDocument("Poke").set("role", "tracked").set("tag", "00000000").save();
      database.newDocument("Poke").set("role", "victim").set("marker", MARKER).save();
      database.newDocument("Poke").set("role", "bumped").set("tag", "00000000").save();
    });

    final RID tracked = ridOf("tracked");
    final RID bumped = ridOf("bumped");
    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(tracked.getBucketId());
    final PageId pageId = new PageId(database, bucket.getFileId(),
        (int) (tracked.getPosition() / bucket.getMaxRecordsInPage()));
    assertThat(bumped.getPosition() / bucket.getMaxRecordsInPage()).as("all three records must share one page")
        .isEqualTo(tracked.getPosition() / bucket.getMaxRecordsInPage());

    final long declinedBefore = declinedByCoverage();
    final CountDownLatch mainTxWritesDone = new CountDownLatch(1);
    final CountDownLatch bumpCommitted = new CountDownLatch(1);
    final List<Throwable> errors = new CopyOnWriteArrayList<>();

    // Competitor: a same-size in-place update of ANOTHER slot on the page -> a false page conflict.
    final Thread bumper = new Thread(() -> {
      try {
        mainTxWritesDone.await();
        database.transaction(() -> bumped.asDocument(true).modify().set("tag", "99999999").save(), true, 50);
      } catch (final Throwable e) {
        errors.add(e);
      } finally {
        bumpCommitted.countDown();
      }
    }, "bumper");
    bumper.start();

    boolean committed = false;
    for (int attempt = 0; attempt < 20 && !committed; attempt++) {
      database.begin();
      try {
        // The TRACKED write: a same-size in-place overwrite of one record.
        tracked.asDocument(true).modify().set("tag", "11111111").save();

        // The UNDECLARED write: bytes on the same page that no tracked slot write accounts for.
        final MutablePage page = ((DatabaseInternal) database).getTransaction().getPageToModify(pageId, bucket.getPageSize(), false);
        final int markerOffset = findMarker(page);
        assertThat(markerOffset).as("the marker must be locatable on the page").isGreaterThan(0);
        page.writeByteArray(markerOffset, POKED.getBytes());

        if (attempt == 0) {
          mainTxWritesDone.countDown();
          bumpCommitted.await();
        }
        database.commit();
        committed = true;
      } catch (final ConcurrentModificationException expected) {
        // WITH the fix: the merge declines the page because of the undeclared bytes, and we retry.
        if (database.isTransactionActive())
          database.rollback();
      }
    }
    bumper.join();

    if (!errors.isEmpty())
      throw new AssertionError("competitor failed: " + errors.getFirst(), errors.getFirst());

    assertThat(committed).as("the transaction must eventually commit").isTrue();

    database.transaction(() -> {
      assertThat(ridOf("victim").asDocument(true).getString("marker")).as("the undeclared write must not be merged away")
          .isEqualTo(POKED);
      assertThat(declinedByCoverage() - declinedBefore).as("the merge must have been declined for lack of coverage")
          .isGreaterThan(0);
      assertThat(tracked.asDocument(true).getString("tag")).isEqualTo("11111111");
      assertThat(bumped.asDocument(true).getString("tag")).isEqualTo("99999999");
    });
  }

  private RID ridOf(final String role) {
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      try (final var rs = database.query("SQL", "SELECT FROM Poke WHERE role = ?", role)) {
        rid[0] = rs.next().getIdentity().orElseThrow();
      }
    });
    return rid[0];
  }

  /** Locates the marker string inside the page content, so the poke lands on real record bytes. */
  private int findMarker(final BasePage page) {
    return findMarkerOffset(page, MARKER.getBytes());
  }

  private int findMarkerOffset(final BasePage page, final byte[] needle) {
    for (int i = 0; i <= page.getContentSize() - needle.length; i++) {
      int k = 0;
      while (k < needle.length && page.readByte(i + k) == needle[k])
        ++k;
      if (k == needle.length)
        return i;
    }
    return -1;
  }
}
