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
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #5155: the edge-removal chain-walk must anchor (fetch a mutable, tx-retained page for) ONLY the chunk it
 * actually modifies, not every read-only chunk it traverses on the way there. Removing an edge that lives in
 * the deepest chunk of a multi-page super-node must therefore touch the same tiny number of pages as removing
 * an edge that lives in the head chunk. Before this optimization the walk anchored every visited chunk via
 * page.modify(), so a deep removal copied the whole chain prefix's pages into the transaction for nothing
 * (those unwritten pages are pruned again before the commit-time version check - pure churn/GC). The
 * #5148/#5153 correctness guarantee (anchor the modified chunk before mutating it) is kept.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue5155EdgeRemovalAnchorTest extends TestHelper {

  private static final int EDGES      = 30_000; // enough IN edges on the hub to span several pages of 8KB chunks
  private static final int BATCH_SIZE = 1_000;

  @Override
  protected void beginTest() {
    // This white-box suite counts anchored pages while walking the CLASSIC single-chain layout (its stats
    // helper casts the head to EdgeSegment): pin super-node promotion off (#5156) so the 30K-edge hub does not
    // switch to the striped layout, whose per-chain removal inherits the same anchoring contract and is
    // covered by SuperNodeStripingTest. TestHelper's teardown resets the configuration.
    GlobalConfiguration.GRAPH_SUPERNODE_THRESHOLD.setValue(0);
  }

  /**
   * White-box: removing a deep edge must anchor no more pages than removing a head edge, and strictly fewer
   * pages than the whole chain spans - proving read-only hops are not anchored.
   */
  @Test
  void removalAnchorsOnlyModifiedChunk() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Hub", 1);
      database.getSchema().createVertexType("Src", 8);
      database.getSchema().createEdgeType("LINK", 8);
    });

    final RID[] hubHolder = new RID[1];
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("Hub");
      hub.save();
      hubHolder[0] = hub.getIdentity();
    });
    final RID hubRID = hubHolder[0];

    // Insert the edges in insertion order (batched for speed); keep the very first (oldest -> deepest/tail
    // chunk, walked last) and the very last (newest -> head chunk, walked first) edge RIDs.
    final RID[] firstEdge = new RID[1];
    final RID[] lastEdge = new RID[1];
    for (int b = 0; b < EDGES / BATCH_SIZE; b++) {
      final int base = b * BATCH_SIZE;
      database.transaction(() -> {
        for (int i = 0; i < BATCH_SIZE; i++) {
          final MutableVertex src = database.newVertex("Src");
          src.save();
          final RID edgeRID = src.newEdge("LINK", hubRID).getIdentity();
          if (base == 0 && i == 0)
            firstEdge[0] = edgeRID;
          lastEdge[0] = edgeRID;
        }
      });
    }

    // Precondition: the IN chain must really span more than one page, else the assertion below is vacuous.
    final int[] chainStats = chainPageStats(hubRID);
    final int chunkCount = chainStats[0];
    final int distinctPages = chainStats[1];
    assertThat(chunkCount).as("hub IN chain chunk count").isGreaterThanOrEqualTo(3);
    assertThat(distinctPages).as("hub IN chain distinct pages").isGreaterThanOrEqualTo(2);

    final int pagesHead = anchoredPagesForRemoval(hubRID, lastEdge[0]);
    final int pagesDeep = anchoredPagesForRemoval(hubRID, firstEdge[0]);

    // Head removal anchors exactly the head chunk's page.
    assertThat(pagesHead).as("pages anchored removing head edge").isEqualTo(1);
    // Deep removal walked the whole (multi-page) chain but anchors just the one chunk it modifies - no more
    // pages than the head removal, and strictly fewer than the chain spans.
    assertThat(pagesDeep).as("pages anchored removing deepest edge").isEqualTo(pagesHead);
    assertThat(pagesDeep).as("pages anchored removing deepest edge vs whole chain").isLessThan(distinctPages);
  }

  /**
   * Functional: removals at the head, deep in the chain, and of every edge must all stay exact and leave a
   * clean integrity check - the read-only-walk refactor must not drop or mis-link anything.
   */
  @Test
  void multiChunkRemovalStaysExactAndClean() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Hub", 1);
      database.getSchema().createVertexType("Src", 8);
      database.getSchema().createEdgeType("LINK", 8);
    });

    final RID[] hubHolder = new RID[1];
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("Hub");
      hub.save();
      hubHolder[0] = hub.getIdentity();
    });
    final RID hubRID = hubHolder[0];

    final int total = 2_000;
    final List<RID> edges = new ArrayList<>(total);
    for (int b = 0; b < total / BATCH_SIZE; b++) {
      database.transaction(() -> {
        for (int i = 0; i < BATCH_SIZE; i++) {
          final MutableVertex src = database.newVertex("Src");
          src.save();
          edges.add(src.newEdge("LINK", hubRID).getIdentity());
        }
      });
    }

    assertThat(inDegree(hubRID)).isEqualTo(total);

    // Remove the newest edge (head chunk).
    database.transaction(() -> edges.get(total - 1).asEdge().delete());
    // Remove the oldest edge (deepest chunk).
    database.transaction(() -> edges.get(0).asEdge().delete());
    // Remove a mid-chain edge.
    database.transaction(() -> edges.get(total / 2).asEdge().delete());

    assertThat(inDegree(hubRID)).isEqualTo(total - 3);

    // Drain everything that is left, exercising empty-chunk relink across the whole chain.
    for (int i = 1; i < total - 1; i++) {
      if (i == total / 2)
        continue;
      final RID e = edges.get(i);
      database.transaction(() -> e.asEdge().delete());
    }

    assertThat(inDegree(hubRID)).isEqualTo(0);
    assertIntegrityClean();
  }

  private long inDegree(final RID hubRID) {
    final long[] holder = new long[1];
    database.transaction(() -> holder[0] = hubRID.asVertex().countEdges(Vertex.DIRECTION.IN, "LINK"));
    return holder[0];
  }

  /** Walks the hub IN chunk chain (read-only) and returns {chunkCount, distinctPageCount}. */
  private int[] chainPageStats(final RID hubRID) {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int[] stats = new int[2];
    db.transaction(() -> {
      final VertexInternal hub = (VertexInternal) hubRID.asVertex();
      RID chunkRID = hub.getInEdgesHeadChunk();
      final Set<Integer> pages = new HashSet<>();
      int chunks = 0;
      while (chunkRID != null) {
        final LocalBucket bucket = (LocalBucket) db.getSchema().getBucketById(chunkRID.getBucketId());
        pages.add((int) (chunkRID.getPosition() / bucket.getMaxRecordsInPage()));
        chunks++;
        chunkRID = ((EdgeSegment) db.lookupByRID(chunkRID, true)).getPreviousRID();
      }
      stats[0] = chunks;
      stats[1] = pages.size();
    });
    return stats;
  }

  /**
   * Runs a single {@code removeEdgeRID} on the hub IN list inside an isolated transaction and returns how many
   * pages it anchored (modified/new pages delta). The transaction is rolled back so measurements are
   * independent and leave the data unchanged.
   */
  private int anchoredPagesForRemoval(final RID hubRID, final RID edgeRID) {
    final DatabaseInternal db = (DatabaseInternal) database;
    db.begin();
    try {
      final VertexInternal hub = (VertexInternal) hubRID.asVertex();
      final EdgeLinkedList inList = db.getGraphEngine().getEdgeHeadChunk(hub, Vertex.DIRECTION.IN);
      final int before = db.getTransaction().getModifiedPages();
      inList.removeEdgeRID(edgeRID);
      return db.getTransaction().getModifiedPages() - before;
    } finally {
      db.rollback();
    }
  }

  private void assertIntegrityClean() {
    try (final ResultSet rs = database.command("sql", "check database")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(longProperty(row, "autoFix")).as("check database autoFix: " + row.toJSON()).isEqualTo(0L);
        assertThat(longProperty(row, "totalErrors")).as("check database totalErrors: " + row.toJSON()).isEqualTo(0L);
      }
    }
  }

  private static long longProperty(final Result row, final String name) {
    final Object value = row.getProperty(name);
    return value == null ? 0L : ((Number) value).longValue();
  }
}
