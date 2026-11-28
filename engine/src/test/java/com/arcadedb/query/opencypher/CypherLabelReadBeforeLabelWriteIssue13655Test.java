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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13655: a per-row read of a node's labels (or of its identity, which a label write changes too, since ArcadeDB
 * rewrites the vertex under another type) planted after an aggregation boundary, and followed by a label write of the same
 * node, saw the write of the rows of the first pull batch. Cypher runs a clause for every row before the next one starts,
 * so every row must read the labels the node had before the write, as Neo4j's {@code Eager} makes it.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class CypherLabelReadBeforeLabelWriteIssue13655Test extends TestHelper {
  private static final String SETUP_1 = "CREATE (s:l4:l7:l10:l9:l6:l0:l3:l2:l8 {k2: true, k6: '\\''})-[:rt1 {k0: 'AAAAAAAAAA', k1: true}]->"
      + "(n0:l10)-[:rt5]->(n1:l0:l10:l2:l3:l4:l5:l6:l7:l9 {k3: 'i8c', id: 90})";
  private static final String SETUP_2 = "UNWIND range(1, 5) AS i CREATE (n2 {k11: true, idx: i})-[:rt1]->(n3 {k3: 'vWW', k7: 1674497568, idx: i})";
  private static final String SETUP_3 = "UNWIND range(1, 118) AS i CREATE (:Dummy {id: i})";

  private static final String MATCH = "OPTIONAL MATCH (:l4:l7:l10:l9:l6:l0:l3:l2:l8 {k2: true, k6: '\\''}) -[{k0: 'AAAAAAAAAA', k1: true}]-> (n0) "
      + "-[r0:rt5]-> (n1 {k3: 'i8c'}), p0 = (n2 {k11: true}) -[]-> (n3 {k3: 'vWW', k7: 1674497568}), () WHERE true\n"
      + "WITH * WHERE n1 IS NOT NULL\n";

  private static final String LEFT = MATCH
      + "WITH *, labels(n1) AS labelsAtBoundary REMOVE n1:l9 RETURN labelsAtBoundary, count(*) AS occurrences";

  private static final String RIGHT = MATCH + """
      WITH collect({n0: n0, n1: n1, n2: n2, n3: n3, r0: r0, p0: p0}) AS rows
      UNWIND CASE WHEN size(rows) = 0 THEN [] ELSE range(0, size(rows) - 1) END AS i
      WITH rows[i] AS row, i
      WITH row.n0 AS n0, row.n1 AS n1, row.n2 AS n2, row.n3 AS n3, row.r0 AS r0, row.p0 AS p0 ORDER BY i
      FOREACH (n IN CASE WHEN n0 IS NULL THEN [] ELSE [n0] END | SET n.tmp = 'x' REMOVE n.tmp)
      FOREACH (n IN CASE WHEN n1 IS NULL THEN [] ELSE [n1] END | SET n.tmp = 'x' REMOVE n.tmp)
      FOREACH (n IN CASE WHEN n2 IS NULL THEN [] ELSE [n2] END | SET n.tmp = 'x' REMOVE n.tmp)
      FOREACH (n IN CASE WHEN n3 IS NULL THEN [] ELSE [n3] END | SET n.tmp = 'x' REMOVE n.tmp)
      WITH *, labels(n1) AS labelsAtBoundary REMOVE n1:l9 RETURN labelsAtBoundary, count(*) AS occurrences""";

  // the node all 150 rows share, read after an aggregation boundary: no MATCH is in flight in front of the write. The FOREACH
  // hands each row on with the node redirected to the record a label write of an earlier row replaced it with, as in the issue
  private static final String SHARED = "MATCH (s:Src) WITH collect(s) AS ns UNWIND range(1, 150) AS i WITH ns[0] AS n, i "
      + "FOREACH (x IN [1] | SET n.tmp = i) ";

  @Test
  void issueQueriesAgree() {
    database.transaction(() -> {
      database.command("opencypher", SETUP_1).close();
      database.command("opencypher", SETUP_2).close();
      database.command("opencypher", SETUP_3).close();
    });
    final List<String> left = new ArrayList<>();
    database.transaction(() -> left.addAll(groups(LEFT)));
    assertThat(left).hasSize(1);
    assertThat(left.getFirst()).contains("l9").endsWith("=655");

    // the LEFT query took l9 off: put it back
    database.transaction(() -> database.command("opencypher", "MATCH (n {k3: 'i8c'}) SET n:l9").close());

    final List<String> right = new ArrayList<>();
    database.transaction(() -> right.addAll(groups(RIGHT)));
    assertThat(right).containsExactlyElementsOf(left);
  }

  @Test
  void labelsProjectionSeesTheLabelsBeforeTheRemove() {
    assertThat(sharedNodeGroups("WITH n, labels(n) AS before REMOVE n:B RETURN before AS g, count(*) AS c"))
        .containsExactly("[A, B, Src]=150");
  }

  @Test
  void labelPredicateProjectionSeesTheLabelsBeforeTheRemove() {
    assertThat(sharedNodeGroups("WITH n, n:B AS before REMOVE n:B RETURN before AS g, count(*) AS c")).containsExactly("true=150");
  }

  @Test
  void labelFilterSeesTheLabelsBeforeTheRemove() {
    assertThat(sharedNodeGroups("WITH n, i WHERE n:B REMOVE n:B RETURN true AS g, count(*) AS c")).containsExactly("true=150");
  }

  @Test
  void labelPredicateProjectionSeesTheLabelsBeforeTheSet() {
    assertThat(sharedNodeGroups("WITH n, n:C AS before SET n:C RETURN before AS g, count(*) AS c")).containsExactly("false=150");
  }

  @Test
  void identityProjectionSeesTheNodeBeforeTheRemove() {
    // the label write moves the vertex to another type, so it changes its id: every row reads the id it had before
    assertThat(sharedNodeGroups("WITH n, elementId(n) AS before REMOVE n:B RETURN before AS g, count(*) AS c")).hasSize(1)
        .allMatch(group -> group.endsWith("=150"));
  }

  @Test
  void labelsProjectionSeesTheLabelsBeforeAForeachRemove() {
    assertThat(sharedNodeGroups("WITH n, labels(n) AS before FOREACH (x IN [1] | REMOVE n:B) RETURN before AS g, count(*) AS c"))
        .containsExactly("[A, B, Src]=150");
  }

  @Test
  void labelsProjectionSeesTheLabelsBeforeAForeachSet() {
    assertThat(sharedNodeGroups("WITH n, labels(n) AS before FOREACH (x IN [1] | SET n:C) RETURN before AS g, count(*) AS c"))
        .containsExactly("[A, B, Src]=150");
  }

  @Test
  void labelsProjectionSeesTheLabelsBeforeAMergeActionSet() {
    assertThat(sharedNodeGroups("WITH n, labels(n) AS before MERGE (n)-[:R]->(:T) ON MATCH SET n:C ON CREATE SET n:C "
        + "RETURN before AS g, count(*) AS c")).containsExactly("[A, B, Src]=150");
  }

  @Test
  void aLabelReadOfTheRowsOwnNodeKeepsStreaming() {
    database.transaction(() -> database.command("opencypher", "UNWIND range(1, 150) AS i CREATE (:Item:B {i: i})").close());
    // n is a grouping key: a different node on every row, so the write of a row cannot reach the read of another one
    final String query = "MATCH (n:Item) WITH n, count(*) AS c WITH n, labels(n) AS before REMOVE n:B RETURN count(*) AS c";
    try (final ResultSet rs = database.command("opencypher", "EXPLAIN " + query)) {
      assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).doesNotContain("EAGER");
    }
    // a label read through another variable than the one written does conflict
    final String other = "MATCH (n:Item) WITH n, count(*) AS c WITH n, n AS m WITH n, labels(m) AS before REMOVE n:B RETURN count(*) AS c";
    try (final ResultSet rs = database.command("opencypher", "EXPLAIN " + other)) {
      assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).contains("EAGER");
    }
  }

  @Test
  void mergeStartsFromTheNodeAnEarlierRowRelabelled() {
    // every row holds the node as it was collected: the ON CREATE label write of the first row replaced it, and the MERGE of
    // the next row has to start from the replacement instead of failing on the deleted record
    database.transaction(() -> database.command("opencypher", "CREATE (:Src:A:B)").close());
    database.transaction(() -> database.command("opencypher", "MATCH (s:Src) WITH collect(s) AS ns UNWIND range(1, 150) AS i "
        + "WITH ns[0] AS n, i MERGE (n)-[:R]->(:T) ON CREATE SET n:C ON MATCH SET n:C RETURN count(*)").close());
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:Src)-[r:R]->(t:T) RETURN labels(n) AS l, count(*) AS c")) {
      final Result row = rs.next();
      assertThat(sortedLabels(row.getProperty("l"))).isEqualTo(List.of("A", "B", "C", "Src"));
      assertThat(row.<Long>getProperty("c")).isEqualTo(1L);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  private List<String> sharedNodeGroups(final String tail) {
    database.transaction(() -> database.command("opencypher", "CREATE (:Src:A:B)").close());
    final List<String> groups = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", SHARED + tail)) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          groups.add(sortedLabels(r.getProperty("g")) + "=" + r.getProperty("c"));
        }
      }
    });
    return groups;
  }

  private List<String> groups(final String query) {
    final List<String> groups = new ArrayList<>();
    try (final ResultSet rs = database.command("opencypher", query)) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        groups.add(sortedLabels(r.getProperty("labelsAtBoundary")) + "=" + r.getProperty("occurrences"));
      }
    }
    return groups;
  }

  private static Object sortedLabels(final Object value) {
    if (value instanceof List<?> list) {
      final List<String> sorted = new ArrayList<>();
      for (final Object label : list)
        sorted.add(String.valueOf(label));
      sorted.sort(null);
      return sorted;
    }
    return value;
  }
}
