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
import com.arcadedb.database.RID;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.graph.olap.GraphAnalyticalViewRegistry;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9486: the partitioned triangle count push-down (LSQB q3) listed a self loop in both the out and the in adjacency of
 * its vertex, so every hop matched it twice, and nothing checked that the three relationships differ (relationship
 * uniqueness inside one MATCH clause). One loop answered 8 where the row pipeline answers 0. The oracle is the same text
 * behind a WITH, which runs the row pipeline.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9486PartitionedTriangleSelfLoopTest extends TestHelper {
  private static final String HEAD = "MATCH (t:Tag) MATCH (p1:P)-[:IN]->(t) MATCH (p2:P)-[:IN]->(t) MATCH (p3:P)-[:IN]->(t) ";
  private static final String TAIL = "MATCH (p1)-[:K]-(p2)-[:K]-(p3)-[:K]-(p1) ";
  private static final String VARS = "t, p1, p2, p3";

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE VERTEX TYPE Tag");
    database.command("sql", "CREATE EDGE TYPE K");
    database.command("sql", "CREATE EDGE TYPE IN");
  }

  @Test
  void onlySelfLoops() throws InterruptedException {
    for (int k = 1; k <= 4; k++) {
      reset();
      final int loops = k;
      database.transaction(() -> {
        final RID p = database.newVertex("P").set("id", 1).save().getIdentity();
        final RID tag = database.newVertex("Tag").set("id", 1).save().getIdentity();
        p.asVertex().newEdge("IN", tag);
        for (int i = 0; i < loops; i++)
          p.asVertex().newEdge("K", p);
      });
      final long expected = (long) k * (k - 1) * (k - 2);
      assertThat(count(HEAD + TAIL + "RETURN count(p1) AS n")).as(k + " loops, count(p1)").isEqualTo(expected);
      assertThat(count(HEAD + "WITH " + VARS + " " + TAIL + "RETURN count(*) AS n")).as(k + " loops, WITH").isEqualTo(expected);
      assertAllViewsAgree(expected);
    }
  }

  @Test
  void loopsNextToRealTrianglesAndParallelEdges() throws InterruptedException {
    final RID[] p = new RID[4];
    database.transaction(() -> {
      final RID tag = database.newVertex("Tag").set("id", 1).save().getIdentity();
      for (int i = 0; i < p.length; i++) {
        p[i] = database.newVertex("P").set("id", i).save().getIdentity();
        p[i].asVertex().newEdge("IN", tag);
      }
      // triangle 0-1-2, a loop on 0 and two on 1, a parallel edge 0-1 and a pendant 3
      p[0].asVertex().newEdge("K", p[1]);
      p[1].asVertex().newEdge("K", p[2]);
      p[2].asVertex().newEdge("K", p[0]);
      p[0].asVertex().newEdge("K", p[0]);
      p[1].asVertex().newEdge("K", p[1]);
      p[1].asVertex().newEdge("K", p[1]);
      p[0].asVertex().newEdge("K", p[1]);
      p[3].asVertex().newEdge("K", p[0]);
    });
    final long expected = count(HEAD + "WITH " + VARS + " " + TAIL + "RETURN count(*) AS n");
    assertThat(count(HEAD + TAIL + "RETURN count(p1) AS n")).isEqualTo(expected);
    assertAllViewsAgree(expected);
  }

  @Test
  void randomGraphsWithLoopsAgreeWithTheRowPipeline() throws InterruptedException {
    randomGraphs(false);
  }

  @Test
  void randomGraphsWithLoopsAndAmbiguousPartitionsAgreeWithTheRowPipeline() throws InterruptedException {
    randomGraphs(true);
  }

  private void randomGraphs(final boolean ambiguous) throws InterruptedException {
    final Random random = new Random(9486L);
    for (int seed = 0; seed < 12; seed++) {
      reset();
      final int persons = 3 + random.nextInt(4);
      final int edges = 4 + random.nextInt(10);
      final int loopBias = random.nextInt(3);
      database.transaction(() -> {
        final RID tag = database.newVertex("Tag").set("id", 1).save().getIdentity();
        final RID otherTag = database.newVertex("Tag").set("id", 2).save().getIdentity();
        final RID[] p = new RID[persons];
        for (int i = 0; i < persons; i++) {
          p[i] = database.newVertex("P").set("id", i).save().getIdentity();
          p[i].asVertex().newEdge("IN", tag);
          // a person in two tags makes the partition chain ambiguous, which takes the weighted path
          if (ambiguous && random.nextBoolean())
            p[i].asVertex().newEdge("IN", otherTag);
        }
        for (int e = 0; e < edges; e++) {
          final int from = random.nextInt(persons);
          final int to = random.nextInt(3) < loopBias ? from : random.nextInt(persons);
          p[from].asVertex().newEdge("K", p[to]);
        }
      });
      final long expected = count(HEAD + "WITH " + VARS + " " + TAIL + "RETURN count(*) AS n");
      assertThat(count(HEAD + TAIL + "RETURN count(*) AS n")).as("seed " + seed + " no view").isEqualTo(expected);
      if (seed < 4)
        assertAllViewsAgree(expected);
    }
  }

  private void reset() {
    database.transaction(() -> {
      database.command("sql", "DELETE FROM K");
      database.command("sql", "DELETE FROM P");
      database.command("sql", "DELETE FROM Tag");
    });
  }

  private void assertAllViewsAgree(final long expected) throws InterruptedException {
    assertThat(count(HEAD + TAIL + "RETURN count(*) AS n")).as("no view").isEqualTo(expected);

    createView("narrow", "VERTEX TYPES (P) EDGE TYPES (K) PROPERTIES (id) UPDATE MODE OFF");
    assertThat(count(HEAD + TAIL + "RETURN count(*) AS n")).as("narrow view").isEqualTo(expected);
    database.command("sql", "DROP GRAPH ANALYTICAL VIEW narrow");

    createView("wide", "VERTEX TYPES (P, Tag) EDGE TYPES (K, IN) PROPERTIES (id) UPDATE MODE OFF");
    assertThat(count(HEAD + TAIL + "RETURN count(*) AS n")).as("wide view").isEqualTo(expected);
    database.command("sql", "DROP GRAPH ANALYTICAL VIEW wide");
  }

  private void createView(final String name, final String definition) throws InterruptedException {
    database.command("sql", "CREATE GRAPH ANALYTICAL VIEW " + name + " " + definition);
    final GraphAnalyticalView view = GraphAnalyticalViewRegistry.get(database, name);
    final long deadline = System.currentTimeMillis() + 60_000;
    while (!view.isReady() && System.currentTimeMillis() < deadline)
      Thread.sleep(20);
    assertThat(view.isReady()).isTrue();
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }
}
