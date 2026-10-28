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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10746: the edge flush of {@link GraphBatch} wrote its records with {@code LocalBucket.createRecordsBulk}, which always
 * started on a brand-new page and never looked at the free-space map. Under a steady insert/delete workload with a flat live set
 * the edge bucket grew by one 64 KiB page per flush, while the slots of the deleted edges stayed empty on the older pages: 2,000
 * pages for about 5,300 live edges, where the same workload through an ordinary transaction keeps 4.
 * <p>
 * The bulk writer now places a record where {@code createRecord} would as long as that is a page that already exists (a free
 * slot of an older page, the room left on the last one), and appends the rest sequentially, without the per-record search, from
 * the first page it has to open.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10746GraphBatchEdgeFlushReusesSpaceTest extends TestHelper {
  private static final int WINDOW         = 20;
  private static final int NODES          = 30;
  private static final int EDGES_PER_NODE = 3;
  private static final int ROUNDS         = 200;

  /** The reporter's shape: a sliding window of rounds, each one flushed by its own batch, the oldest round deleted after it. */
  @Test
  void aChurnOfSmallFlushesKeepsTheEdgeBucketFlat() {
    createSchema();
    RID[] previous = new RID[0];
    for (int round = 0; round < ROUNDS; round++) {
      previous = batchRound(round, previous);
      deleteOldestRound(round);
    }

    final long liveEdges = database.countType("Link", false);
    assertThat(liveEdges).isGreaterThan(WINDOW * NODES);
    assertThat(linkBucket().getTotalPages()).as("edge bucket pages after %d flushes, %d live edges", ROUNDS, liveEdges)
        .isLessThanOrEqualTo(4);
    assertEveryEdgeReadsBack();
  }

  /**
   * The bulk shape the writer was made for: one flush of many edges into an empty bucket still fills its pages one after the
   * other, so it opens no more pages than the records need.
   */
  @Test
  void aLargeFlushIntoAnEmptyBucketStillPacksItsPages() {
    createSchema();
    final int edges = 20_000;
    try (final GraphBatch batch = database.batch().build()) {
      final RID[] vertices = batch.createVertices("Node", 2_000);
      final Random random = new Random(3);
      for (int i = 0; i < edges; i++)
        batch.newEdge(vertices[i % vertices.length], "Link", vertices[random.nextInt(vertices.length)], "w", i);
    }

    assertThat(database.countType("Link", false)).isEqualTo(edges);
    final LocalBucket bucket = linkBucket();
    final int recordsPerFullPage = Math.min(bucket.getMaxRecordsInPage(), bucket.getPageSize() / 20);
    assertThat(bucket.getTotalPages()).isLessThanOrEqualTo(edges / recordsPerFullPage + 2);
    assertEveryEdgeReadsBack();
  }

  /** A record larger than a page cannot be appended in one piece: it is written as a chunk chain, like createRecord writes it. */
  @Test
  void anEdgeLargerThanAPageIsWrittenAsAMultiPageRecord() {
    createSchema();
    final String big = "y".repeat(100_000);
    final RID[] vertices;
    try (final GraphBatch batch = database.batch().build()) {
      vertices = batch.createVertices("Node", 3);
      batch.newEdge(vertices[0], "Link", vertices[1], "w", 1);
      batch.newEdge(vertices[1], "Link", vertices[2], "payload", big);
      batch.newEdge(vertices[2], "Link", vertices[0], "w", 3);
    }

    assertThat(database.countType("Link", false)).isEqualTo(3);
    try (final ResultSet rs = database.query("sql", "SELECT payload FROM Link WHERE payload IS NOT NULL")) {
      assertThat(rs.next().<String>getProperty("payload")).isEqualTo(big);
    }
    assertEveryEdgeReadsBack();
  }

  private void createSchema() {
    database.getSchema().buildVertexType().withName("Node").withTotalBuckets(1).create().createProperty("id", Type.LONG);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Node", "id");
    database.getSchema().buildEdgeType().withName("Link").withTotalBuckets(1).create();
  }

  private RID[] batchRound(final int round, final RID[] previous) {
    final Random random = new Random(round);
    final RID[] fresh;
    try (final GraphBatch batch = database.batch().build()) {
      final Object[][] properties = new Object[NODES][];
      for (int i = 0; i < NODES; i++)
        properties[i] = new Object[] { "id", (long) round * NODES + i };
      fresh = batch.createVertices("Node", properties);
      for (int i = 0; i < NODES; i++)
        for (int k = 0; k < EDGES_PER_NODE; k++) {
          final int pick = random.nextInt(fresh.length + previous.length);
          final RID to = pick < fresh.length ? fresh[pick] : previous[pick - fresh.length];
          if (!to.equals(fresh[i]))
            batch.newEdge(fresh[i], "Link", to, "w", random.nextInt(10));
        }
    }
    return fresh;
  }

  private void deleteOldestRound(final int round) {
    final long below = Math.max(0, round - WINDOW + 1) * (long) NODES;
    if (below > 0)
      database.transaction(() -> database.command("sql", "DELETE FROM Node WHERE id < ?", below).close());
  }

  /** Every edge the bucket holds is a whole record whose endpoints know it: nothing was written over another record. */
  private void assertEveryEdgeReadsBack() {
    final Set<RID> seen = new HashSet<>();
    database.iterateType("Link", false).forEachRemaining(record -> {
      final Edge edge = record.asEdge();
      assertThat(seen.add(edge.getIdentity())).isTrue();
      assertThat(edge.getOutVertex().getIdentity()).isNotNull();
      assertThat(edge.getInVertex().getIdentity()).isNotNull();
      final Document out = edge.getOutVertex().asVertex();
      boolean linked = false;
      for (final Edge e : out.asVertex().getEdges(Vertex.DIRECTION.OUT, "Link"))
        if (e.getIdentity().equals(edge.getIdentity())) {
          linked = true;
          break;
        }
      assertThat(linked).as("edge %s is in the OUT list of %s", edge.getIdentity(), out.getIdentity()).isTrue();
    });
    assertThat(seen).hasSize((int) database.countType("Link", false));
  }

  private LocalBucket linkBucket() {
    return (LocalBucket) database.getSchema().getType("Link").getBuckets(false).get(0);
  }
}
