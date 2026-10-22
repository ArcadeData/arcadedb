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
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10559, found by a differential of the quantified path pattern shapes against Neo4j 2026.08.1:
 * <ol>
 *   <li>an undirected hop inside a group matched a self-loop twice, once from each list of its vertex, where the plain pattern and the
 *   variable-length pattern match the one relationship once</li>
 *   <li>an undefined variable in the inner {@code WHERE} of a group was accepted, and answered as if the predicate were null</li>
 *   <li>an aggregate function in the inner {@code WHERE} of a group was accepted</li>
 * </ol>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherQuantifiedPathPatternSelfLoopScopeIssue10559Test {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypher-qpp-selfloop-10559");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("L");
    database.getSchema().createEdgeType("R");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /** The graph of the report: a loop on a and an edge a to b. */
  private void loopAndEdge() {
    database.transaction(() -> database.command("cypher", "CREATE (a:L {id:0}), (b:L {id:1}), (a)-[:R]->(a), (a)-[:R]->(b)"));
  }

  @Test
  void anUndirectedHopOfAGroupMatchesASelfLoopOnce() {
    loopAndEdge();

    // Neo4j answers the same as the plain and the variable-length pattern: the loop once, the edge once
    assertThat(ids("MATCH (a:L {id:0})-[:R]-(b) RETURN b.id AS v")).containsExactlyInAnyOrder(0L, 1L);
    assertThat(ids("MATCH (a:L {id:0})-[:R*1]-(b) RETURN b.id AS v")).containsExactlyInAnyOrder(0L, 1L);
    assertThat(ids("MATCH (a:L {id:0}) ((x)-[:R]-(y)){1} (b) RETURN b.id AS v")).containsExactlyInAnyOrder(0L, 1L);

    assertThat(count("MATCH (a:L {id:0}) ((x)-[:R]-(y)){1,2} (b) RETURN count(*) AS n")).isEqualTo(3L);

    final List<Result> perNode = rows("MATCH (a:L) RETURN a.id AS id, COUNT { (a) ((x)-[:R]-(y)){1} (b) } AS c ORDER BY id");
    assertThat(perNode).hasSize(2);
    assertThat((Long) perNode.get(0).getProperty("c")).isEqualTo(2L);
    assertThat((Long) perNode.get(1).getProperty("c")).isEqualTo(1L);
  }

  @Test
  void theDirectedAndTheOtherQuantifiersKeepTheirAnswers() {
    loopAndEdge();

    assertThat(ids("MATCH (a:L {id:0}) ((x)-[:R]->(y)){1} (b) RETURN b.id AS v")).containsExactlyInAnyOrder(0L, 1L);
    assertThat(count("MATCH (a:L {id:0}) ((x)-[:R]->(y)){1,2} (b) RETURN count(*) AS n")).isEqualTo(3L);
    // {0,1}: the zero repetition is a again, then the loop and the edge
    assertThat(count("MATCH (a:L {id:0}) ((x)-[:R]-(y)){0,1} (b) RETURN count(*) AS n")).isEqualTo(3L);
  }

  /**
   * A group repeated k times is the same as the k hops written out: relationship uniqueness holds across the repetitions of the group,
   * as it does across the hops of a plain pattern. Over a graph with loops, parallel edges and a reciprocal pair, so each shape that
   * crosses a loop is there.
   */
  @Test
  void aGroupCountsTheSameAsTheHopsWrittenOut() {
    database.transaction(() -> database.command("cypher", """
        CREATE (n0:L {id:0}), (n1:L {id:1}), (n2:L {id:2}), (n3:L {id:3}), (n4:L {id:4}), (n5:L {id:5}), (n6:L {id:6}), (n7:L {id:7}),
        (n0)-[:R]->(n1), (n1)-[:R]->(n2), (n2)-[:R]->(n3), (n3)-[:R]->(n4), (n4)-[:R]->(n5), (n5)-[:R]->(n6), (n6)-[:R]->(n7),
        (n2)-[:R]->(n2), (n3)-[:R]->(n4), (n5)-[:R]->(n4), (n6)-[:R]->(n6), (n6)-[:R]->(n6), (n7)-[:R]->(n0)
        """));

    final String[] unrolled = {
        "(a)-[r1:R]-(b)",
        "(a)-[r1:R]-(m1)-[r2:R]-(b)",
        "(a)-[r1:R]-(m1)-[r2:R]-(m2)-[r3:R]-(b)" };
    for (int hops = 1; hops <= 3; hops++) {
      final long expected = count("MATCH " + unrolled[hops - 1] + " RETURN count(*) AS n");
      assertThat(count("MATCH (a:L) ((x)-[:R]-(y)){" + hops + "} (b) RETURN count(*) AS n"))
          .as("an undirected group repeated %d times", hops).isEqualTo(expected);
    }

    // a range is the sum of its repetition counts
    final long oneAndTwo = count("MATCH (a)-[:R]-(b) RETURN count(*) AS n")
        + count("MATCH (a)-[r1:R]-(m)-[r2:R]-(b) RETURN count(*) AS n");
    assertThat(count("MATCH (a:L) ((x)-[:R]-(y)){1,2} (b) RETURN count(*) AS n")).isEqualTo(oneAndTwo);
  }

  @Test
  void anUndefinedVariableInTheInnerWhereOfAGroupIsRefused() {
    loopAndEdge();

    final String plain = "MATCH (a:L {id:0})-[:R]->(y) WHERE zz.id > 0 RETURN count(*) AS n";
    assertThatThrownBy(() -> drain(database.query("cypher", plain))).isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("UndefinedVariable");

    for (final String query : new String[] {
        "MATCH (a:L {id:0}) ((x)-[:R]->(y) WHERE zz.id > 0){1,2} (b) RETURN count(*) AS n",
        "MATCH (a:L {id:0}) ((x)-[:R]->(y) WHERE x.id = 0 AND zz.id > 0){1,2} (b) RETURN count(*) AS n",
        "OPTIONAL MATCH (a:L {id:0}) ((x)-[:R]->(y) WHERE zz.id > 0){1,2} (b) RETURN count(*) AS n" }) {
      assertThatThrownBy(() -> drain(database.query("cypher", query))).as(query).isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("UndefinedVariable").hasMessageContaining("'zz'");
      assertThatThrownBy(() -> drain(database.command("cypher", query))).as(query).isInstanceOf(CommandSemanticException.class)
          .hasMessageContaining("UndefinedVariable");
    }
  }

  @Test
  void theVariablesAGroupCanReadAreNotRefused() {
    loopAndEdge();

    // the variables written inside the group, the ones of the enclosing pattern and the ones of an earlier clause
    assertThat(count("MATCH (a:L {id:0}) ((x)-[:R]->(y) WHERE x.id = 0 AND y.id >= 0){1,2} (b) RETURN count(*) AS n")).isEqualTo(3L);
    assertThat(count("MATCH (a:L {id:0}) ((x)-[:R]->(y) WHERE y.id >= a.id){1,2} (b) RETURN count(*) AS n")).isEqualTo(3L);
    assertThat(count("WITH 0 AS floor MATCH (a:L {id:0}) ((x)-[:R]->(y) WHERE y.id >= floor){1,2} (b) RETURN count(*) AS n")).isEqualTo(3L);
    assertThat(count("MATCH (a:L {id:0}) ((x)-[r:R]->(y) WHERE r IS NOT NULL){1} (b) RETURN count(*) AS n")).isEqualTo(2L);
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "MATCH (a:L {id:0}) ((x)-[:R]->(y) WHERE count(x) > 0){1,2} (b) RETURN count(*) AS n",
      "MATCH (a:L {id:0}) ((x)-[:R]->(y) WHERE x.id = 0 AND count(y) > 0){1,2} (b) RETURN count(*) AS n",
      "MATCH (a:L {id:0}) ((x)-[:R]->(y) WHERE sum(x.id) > 0){1,2} (b) RETURN count(*) AS n",
      "OPTIONAL MATCH (a:L {id:0}) ((x)-[:R]->(y) WHERE count(x) > 0){1,2} (b) RETURN count(*) AS n" })
  void anAggregateInTheInnerWhereOfAGroupIsRefused(final String query) {
    loopAndEdge();

    final String plain = "MATCH (a:L {id:0})-[:R]->(y) WHERE count(a) > 0 RETURN count(*) AS n";
    assertThatThrownBy(() -> drain(database.query("cypher", plain))).isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("InvalidAggregation");

    assertThatThrownBy(() -> drain(database.query("cypher", query))).isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("InvalidAggregation");
    assertThatThrownBy(() -> drain(database.command("cypher", query))).isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("InvalidAggregation");
  }

  /** Every predicate the grammar allows in a WHERE is held to the scope, not only the comparisons. */
  @ParameterizedTest
  @ValueSource(strings = {
      "zz.name =~ 'A.*'",
      "zz.name STARTS WITH 'A'",
      "zz.name ENDS WITH 'A'",
      "zz.name CONTAINS 'A'",
      "x.name STARTS WITH zz.name",
      "zz.id IS NULL",
      "zz.id IS NOT NULL",
      "zz.id IN [0, 1]",
      "zz IS :: INTEGER",
      "zz IS NORMALIZED",
      "NOT zz.flag",
      "x.id = 0 OR zz.id = 1" })
  void everyKindOfPredicateOfTheInnerWhereIsHeldToTheScope(final String predicate) {
    loopAndEdge();

    final String group = "MATCH (a:L {id:0}) ((x)-[:R]->(y) WHERE " + predicate + "){1,2} (b) RETURN count(*) AS n";
    assertThatThrownBy(() -> drain(database.query("cypher", group))).as(predicate).isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("UndefinedVariable").hasMessageContaining("'zz'");
    // the same predicate in the WHERE of the clause, which the group is held to
    final String clause = "MATCH (a:L {id:0})-[:R]->(y) WHERE " + predicate.replace("x.", "a.") + " RETURN count(*) AS n";
    assertThatThrownBy(() -> drain(database.query("cypher", clause))).as(clause).isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("UndefinedVariable");
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "count(x) IS NOT NULL",
      "count(x) IS NULL",
      "count(x) =~ '1'",
      "toString(count(x)) STARTS WITH '1'",
      "count(x) IS :: INTEGER",
      "count(x)",
      "x.id = 0 AND sum(y.id) IS NOT NULL" })
  void anAggregateUnderAnyKindOfPredicateOfTheInnerWhereIsRefused(final String predicate) {
    loopAndEdge();

    final String group = "MATCH (a:L {id:0}) ((x)-[:R]->(y) WHERE " + predicate + "){1,2} (b) RETURN count(*) AS n";
    assertThatThrownBy(() -> drain(database.query("cypher", group))).as(predicate).isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("InvalidAggregation");
  }

  /**
   * The inner WHERE can read the variable of the right boundary, which is bound to the vertex the whole group ends on: no repetition
   * but the last knows it, so every repetition is judged against it when a row is emitted. It used to read null, and so matched no row.
   */
  @Test
  void theInnerWhereCanReadTheVariableOfTheRightBoundary() {
    loopAndEdge();

    // the paths of the group: [loop] ends on a, [a->b] ends on b, [loop, a->b] ends on b
    assertThat(count("MATCH (a:L {id:0}) ((x)-[:R]->(y) WHERE b.id >= 0){1,2} (b) RETURN count(*) AS n")).isEqualTo(3L);
    // y is below the end of the group at every repetition
    assertThat(count("MATCH (a:L {id:0}) ((x)-[:R]->(y) WHERE y.id <= b.id){1,2} (b) RETURN count(*) AS n")).isEqualTo(3L);
    // [loop, a->b] has y = a (0) in its first repetition, below the end b (1): out
    assertThat(count("MATCH (a:L {id:0}) ((x)-[:R]->(y) WHERE y.id >= b.id){1,2} (b) RETURN count(*) AS n")).isEqualTo(2L);
    assertThat(ids("MATCH (a:L {id:0}) ((x)-[:R]->(y) WHERE y.id >= b.id){1,2} (b) RETURN b.id AS v")).containsExactlyInAnyOrder(0L, 1L);
    // a bound that no row meets
    assertThat(count("MATCH (a:L {id:0}) ((x)-[:R]->(y) WHERE b.id > 5){1,2} (b) RETURN count(*) AS n")).isZero();
    // a predicate that does not read the boundary keeps being judged repetition by repetition
    assertThat(count("MATCH (a:L {id:0}) ((x)-[:R]->(y) WHERE y.id >= 0 AND a.id = 0){1,2} (b) RETURN count(*) AS n")).isEqualTo(3L);
    // zero repetitions end where they started and have nothing to judge
    assertThat(count("MATCH (a:L {id:0}) ((x)-[:R]->(y) WHERE b.id > 5){0,1} (b) RETURN count(*) AS n")).isEqualTo(1L);
  }

  private long count(final String query) {
    return ((Number) rows(query).getFirst().getProperty("n")).longValue();
  }

  private List<Long> ids(final String query) {
    final List<Long> ids = new ArrayList<>();
    for (final Result row : rows(query))
      ids.add(((Number) row.getProperty("v")).longValue());
    return ids;
  }

  private List<Result> rows(final String query) {
    return drain(database.query("cypher", query));
  }

  private static List<Result> drain(final ResultSet resultSet) {
    final List<Result> rows = new ArrayList<>();
    try (resultSet) {
      while (resultSet.hasNext())
        rows.add(resultSet.next());
    }
    return rows;
  }
}
