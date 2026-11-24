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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A supertype label whose vertices all live in its subtypes is costed with the vertices MATCH (n:Label) reads, its subtypes' too
 * (issue #12280). It was estimated at 0 rows, so a free scan of it became the outer side of a join and every vertex of the label
 * drove an index seek. Each plan is compared with the plan of a label F that holds the same vertices with no hierarchy, and every
 * answer with F's.
 */
class Issue12280SupertypeCardinalityTest extends TestHelper {
  private static final int VERTICES = 2_000;
  private static final int GROUPS   = 20;

  @Override
  protected void beginTest() {
    // V declares the properties and indexes, A and B hold the vertices; F holds the same vertices with no hierarchy
    for (final String label : List.of("V", "F")) {
      database.command("sql", "CREATE VERTEX TYPE " + label + " BUCKETS 2");
      database.command("sql", "CREATE PROPERTY " + label + ".k INTEGER");
      database.command("sql", "CREATE PROPERTY " + label + ".g INTEGER");
      database.command("sql", "CREATE INDEX ON " + label + " (k) UNIQUE");
      database.command("sql", "CREATE INDEX ON " + label + " (g) NOTUNIQUE");
    }
    database.command("sql", "CREATE VERTEX TYPE A EXTENDS V BUCKETS 2");
    database.command("sql", "CREATE VERTEX TYPE B EXTENDS V BUCKETS 2");
    database.command("sql", "CREATE EDGE TYPE L");
    database.command("sql", "CREATE PROPERTY L.w INTEGER");
    database.command("sql", "CREATE INDEX ON L (w) NOTUNIQUE");

    database.transaction(() -> {
      final List<MutableVertex> hierarchy = new ArrayList<>(VERTICES);
      final List<MutableVertex> flat = new ArrayList<>(VERTICES);
      for (int i = 0; i < VERTICES; i++) {
        hierarchy.add(database.newVertex(i % 2 == 0 ? "A" : "B").set("k", i).set("g", i % GROUPS).save());
        flat.add(database.newVertex("F").set("k", i).set("g", i % GROUPS).save());
      }
      for (int i = 0; i < VERTICES; i++) {
        final int j = (i * 31 + 7) % VERTICES;
        hierarchy.get(i).newEdge("L", hierarchy.get(j), "w", i % 50).save();
        flat.get(i).newEdge("L", flat.get(j), "w", i % 50).save();
      }
    });
  }

  @Test
  void theSupertypeIsEstimatedWithItsSubtypesVertices() {
    assertThat(explain("MATCH (n:V) RETURN count(n)")).doesNotContain("rows=0]").contains("rows=" + VERTICES + "]");
  }

  @Test
  void aJoinOnTheSupertypeStartsFromTheSeekLikeTheFlatLabel() {
    final String query = "MATCH (a:%s {k: 1}), (b:%s) WHERE b.g = a.g RETURN count(b) AS c";
    final String plan = explain(query.formatted("V", "V"));
    assertThat(plan).as("issue #12280:%n%s", plan).doesNotContain("NodeByLabelScan(b:V)").contains("NodeIndexSeek(a:V)");
    assertSameCount(query);
  }

  @Test
  void anEdgeIndexSeedIsChosenOverTheSupertypeScan() {
    final String query = "MATCH (a:%s)-[r:L]->(b:%s) WHERE r.w = 3 RETURN count(*) AS c";
    final String plan = explain(query.formatted("V", "V"));
    assertThat(plan).as("issue #12280:%n%s", plan).doesNotContain("NodeByLabelScan(a:V)");
    assertSameCount(query);
  }

  @Test
  void otherShapesAnswerAsTheFlatLabel() {
    assertSameCount("MATCH (a:%s)-[:L]->(b:%s) WHERE a.k < 100 RETURN count(*) AS c");
    assertSameCount("MATCH (a:%s {g: 3}) WITH a MATCH (b:%s {g: a.g}) RETURN count(b) AS c");
    assertSameCount("MATCH (a:%s), (b:%s) WHERE a.k = b.k AND a.g = 4 RETURN count(*) AS c");
  }

  private void assertSameCount(final String query) {
    final long hierarchy = count(query.formatted("V", "V"));
    assertThat(hierarchy).as(query).isEqualTo(count(query.formatted("F", "F"))).isPositive();
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private String explain(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }
}
