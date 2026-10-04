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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9123: {@code MATCH p = allShortestPaths(...)} collected every path before it emitted the first row. With parallel
 * relationships the path count is the product of the parallel relationships per hop, so a LIMIT (or any consumer that stops
 * reading) had to wait for, and hold in memory, millions of paths. The paths are now produced on demand.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9123AllShortestPathsStreamingTest extends TestHelper {

  // 12 hops of 4 parallel relationships: 4^12 = 16.7M co-shortest paths between the two ends
  private static final int HOPS = 12;

  @Override
  public void beginTest() {
    database.getSchema().createVertexType("Chain");
    database.getSchema().createEdgeType("P");
    database.transaction(() -> {
      MutableVertex previous = database.newVertex("Chain").set("id", 0).save();
      for (int i = 1; i <= HOPS; i++) {
        final MutableVertex next = database.newVertex("Chain").set("id", i).save();
        for (int k = 0; k < 4; k++)
          previous.newEdge("P", next, "eid", i * 10 + k);
        previous = next;
      }
    });
  }

  private int rows(final String query) {
    int n = 0;
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext()) {
        final List<?> path = rs.next().getProperty("p");
        assertThat(path).hasSize(HOPS * 2 + 1);
        ++n;
      }
    }
    return n;
  }

  // @Timeout is a hang detector here: materializing 16.7M paths does not return in any reasonable time or fits no heap
  @Test
  @Timeout(60)
  void limitStopsTheEnumerationOfAnUnconstrainedPattern() {
    assertThat(rows("MATCH (a:Chain {id: 0}), (b:Chain {id: " + HOPS + "}) MATCH p = allShortestPaths((a)-[:P*]->(b)) RETURN p LIMIT 5"))
        .isEqualTo(5);
  }

  @Test
  @Timeout(60)
  void limitStopsTheEnumerationOfAConstrainedPattern() {
    assertThat(rows("MATCH (a:Chain {id: 0}), (b:Chain {id: " + HOPS
        + "}) MATCH p = allShortestPaths((a)-[r:P* WHERE r.eid > 0]->(b)) RETURN p LIMIT 5")).isEqualTo(5);
  }

  @Test
  @Timeout(60)
  void streamedPathsAreDistinctAndWalkTheChainInOrder() {
    final Set<List<Object>> seen = new HashSet<>();
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:Chain {id: 0}), (b:Chain {id: " + HOPS + "}) MATCH p = allShortestPaths((a)-[:P*]->(b)) "
            + "RETURN [r IN relationships(p) | r.eid] AS eids LIMIT 2000")) {
      while (rs.hasNext()) {
        final List<Object> eids = List.copyOf(rs.next().<List<?>>getProperty("eids"));
        assertThat(eids).hasSize(HOPS);
        for (int i = 0; i < HOPS; i++)
          assertThat(((Number) eids.get(i)).intValue() / 10).as("hop %d", i).isEqualTo(i + 1);
        assertThat(seen.add(eids)).as("%s repeated", eids).isTrue();
      }
    }
    assertThat(seen).hasSize(2000);
  }
}
