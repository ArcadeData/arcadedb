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
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10746: the edge flush of {@code database.batch()} wrote through {@code LocalBucket.createRecordsBulk}, which started every
 * flush on a fresh page and never looked at the room deleted records gave back, so under a steady insert/delete churn with a
 * flat live set the edge bucket grew by one page per flush (2,000 pages for ~5,300 live edges after 2,000 rounds, against 4 pages
 * through ordinary transactions). The bulk write now fills the room the bucket has before it appends.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10746GraphBatchSpaceReuseTest extends TestHelper {
  private static final int ROUNDS         = 150;
  private static final int WINDOW         = 20;
  private static final int NODES          = 30;
  private static final int EDGES_PER_NODE = 3;

  @Test
  void aChurnThroughGraphBatchKeepsTheEdgeBucketFlat() {
    database.getSchema().createVertexType("Node", 1).createProperty("id", Type.LONG);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Node", "id");
    database.getSchema().createEdgeType("Link", 1);

    RID[] previous = new RID[0];
    for (int round = 0; round < ROUNDS; round++) {
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
      previous = fresh;

      final long oldest = Math.max(0, round - WINDOW + 1) * (long) NODES;
      if (oldest > 0)
        database.transaction(() -> database.command("sql", "DELETE FROM Node WHERE id < ?", oldest).close());
    }

    // THE LIVE SET IS ~WINDOW ROUNDS OF EDGES: A FEW PAGES. ONE PAGE PER FLUSH WAS ROUNDS PAGES
    final long liveEdges = database.countType("Link", false);
    assertThat(liveEdges).isGreaterThan(0);
    assertThat(linkPages()).as("pages of the edge bucket for %d live edges after %d flushes", liveEdges, ROUNDS)
        .isLessThan(ROUNDS / 5);

    // EVERY EDGE LEFT IS INTACT AND CONNECTS LIVE VERTICES
    try (final ResultSet resultSet = database.query("sql", "SELECT @out.id AS src, @in.id AS dst, w FROM Link")) {
      long count = 0;
      while (resultSet.hasNext()) {
        final Result row = resultSet.next();
        assertThat(row.<Long>getProperty("src")).isNotNull();
        assertThat(row.<Long>getProperty("dst")).isNotNull();
        assertThat(row.<Integer>getProperty("w")).isBetween(0, 9);
        ++count;
      }
      assertThat(count).isEqualTo(liveEdges);
    }
    try (final ResultSet check = database.command("sql", "CHECK DATABASE")) {
      final Result result = check.next();
      assertThat(result.<Long>getProperty("totalErrors")).isZero();
    }
  }

  @Test
  void anEdgeLargerThanAPageIsWrittenAsAChunkChain() {
    database.getSchema().createVertexType("Node", 1);
    database.getSchema().createEdgeType("Link", 1);

    final String big = "x".repeat(200_000);
    final RID[] vertices;
    try (final GraphBatch batch = database.batch().build()) {
      vertices = batch.createVertices("Node", 3);
      batch.newEdge(vertices[0], "Link", vertices[1], "w", 1);
      batch.newEdge(vertices[1], "Link", vertices[2], "payload", big);
      batch.newEdge(vertices[2], "Link", vertices[0], "w", 3);
    }

    try (final ResultSet resultSet = database.query("sql", "SELECT w, payload FROM Link")) {
      int small = 0;
      int large = 0;
      while (resultSet.hasNext()) {
        final Result row = resultSet.next();
        if (row.getProperty("payload") != null) {
          assertThat(row.<String>getProperty("payload")).isEqualTo(big);
          ++large;
        } else
          ++small;
      }
      assertThat(small).isEqualTo(2);
      assertThat(large).isEqualTo(1);
    }
  }

  private int linkPages() {
    int pages = 0;
    for (final Bucket bucket : database.getSchema().getType("Link").getBuckets(false))
      pages += ((LocalBucket) bucket).getTotalPages();
    return pages;
  }
}
