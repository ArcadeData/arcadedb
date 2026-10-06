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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #9290: the COUNT ANTI-JOIN CHAIN push-down answered more than the row pipeline for chains with more than
 * two hops of one type, an inequality between other nodes than the negated pattern's, no inequality, or the negated pattern away from
 * the first node. Every shape must now agree with the row pipeline (the same WHERE after a WITH) and with the expected count, and the
 * shape the push-down is written for (LSQB Q9) must keep it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9290AntiJoinChainShapesTest {
  private static final String DB_PATH = "./target/databases/issue9290";

  private static final String P3 = "MATCH (p0:Person)-[:KNOWS]-(p1:Person)-[:KNOWS]-(p2:Person)-[:HAS_INTEREST]->(t:Tag) ";
  private static final String P4 = "MATCH (p0:Person)-[:KNOWS]-(p1:Person)-[:KNOWS]-(p2:Person)-[:KNOWS]-(p3:Person) ";

  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    for (final String ddl : new String[] { "CREATE VERTEX TYPE Person", "CREATE VERTEX TYPE Tag", "CREATE EDGE TYPE KNOWS",
        "CREATE EDGE TYPE HAS_INTEREST" })
      database.command("sql", ddl);
    database.begin();
    final RID[] p = new RID[4];
    for (int i = 0; i < 4; i++) {
      final MutableVertex v = database.newVertex("Person").set("id", (long) i);
      v.save();
      p[i] = v.getIdentity();
    }
    final MutableVertex tag = database.newVertex("Tag");
    tag.save();
    for (final int[] e : new int[][] { { 0, 1 }, { 1, 2 }, { 2, 3 }, { 0, 2 } })
      p[e[0]].asVertex().newEdge("KNOWS", p[e[1]]).save();
    for (int i = 0; i < 4; i++)
      p[i].asVertex().newEdge("HAS_INTEREST", tag.getIdentity()).save();
    database.commit();
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void controlLsqbQ9ShapeKeepsPushDown() {
    assertSame(P3, "p0, p1, p2, t", "NOT (p0)-[:KNOWS]-(p2) AND p0 <> p2", 4, true);
    assertSame(P3, "p0, p1, p2, t", "NOT (p2)-[:KNOWS]-(p0) AND p2 <> p0", 4, true);
  }

  @Test
  void unlabelledMiddleNodeWithFirstNodeAsSourceKeepsPushDown() {
    assertSame("MATCH (p0:Person)-[:KNOWS]-(p1)-[:KNOWS]-(p2:Person)-[:HAS_INTEREST]->(t:Tag) ", "p0, p1, p2, t",
        "NOT (p0)-[:KNOWS]-(p2) AND p0 <> p2", 4, true);
  }

  @Test
  void moreThanTwoHopsOfOneType() {
    assertSame(P4, "p0, p1, p2, p3", "NOT (p0)-[:KNOWS]-(p2) AND p0 <> p2", 2, false);
  }

  @Test
  void inequalityBetweenOtherNodes() {
    assertSame(P3, "p0, p1, p2, t", "NOT (p0)-[:KNOWS]-(p2) AND p1 <> p2", 4, false);
  }

  @Test
  void noInequality() {
    assertSame(P3, "p0, p1, p2, t", "NOT (p0)-[:KNOWS]-(p2)", 4, false);
  }

  @Test
  void negatedPatternAwayFromFirstNode() {
    assertSame(P4, "p0, p1, p2, p3", "NOT (p1)-[:KNOWS]-(p3) AND p1 <> p3", 2, false);
  }

  @Test
  void unlabelledMiddleNodeWithFirstNodeAsTarget() {
    assertSame("MATCH (p0:Person)-[:KNOWS]-(p1)-[:KNOWS]-(p2:Person)-[:HAS_INTEREST]->(t:Tag) ", "p0, p1, p2, t",
        "NOT (p2)-[:KNOWS]-(p0) AND p2 <> p0", 4, false);
  }

  private void assertSame(final String chain, final String vars, final String where, final long expected, final boolean pushedDown) {
    final String text = chain + "WHERE " + where + " RETURN count(*) AS n";
    assertThat(count(text)).as("as written: " + text).isEqualTo(expected);
    assertThat(count(chain + "WITH " + vars + " WHERE " + where + " RETURN count(*) AS n")).as("row pipeline").isEqualTo(expected);
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + text)) {
      final String plan = rs.getExecutionPlan().map(x -> x.prettyPrint(0, 2)).orElse("");
      assertThat(plan.contains("ANTI-JOIN")).as("push-down in plan: " + plan).isEqualTo(pushedDown);
    }
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return ((Number) rs.next().getProperty("n")).longValue();
    }
  }
}
