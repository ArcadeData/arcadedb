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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12520: a {@code MERGE} followed by a {@code CALL (*) { ... SET ... }} that rewrites the property the MERGE matches on
 * saw the subquery's writes of the rows before it: the MERGE of the second row no longer matched the node the first row had
 * created (its {@code k6} was already rewritten), so it created another one. A Cypher clause runs for all its rows before the
 * next one starts, so the MERGE of every row sees the same graph, whichever way the statement is spelled: with or without a
 * pass-through {@code WITH p2} between the two clauses, and inside or outside a {@code CALL () { ... UNION ... }} body.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherUnionCallMergeSetIssue12520Test extends TestHelper {

  private static final String BODY_WITHOUT_WITH = "CREATE (:l5) UNWIND range(1, 101) AS duplicateRow "
      + "MERGE p2 = (:A)-[:R]->(:B {k6: false}) CALL (*) { UNWIND nodes(p2) AS e SET e.k6 = true RETURN count(*) AS alias36 } "
      + "RETURN p2 AS p1";

  private static final String BODY_WITH_WITH = "CREATE (:l5) UNWIND range(1, 101) AS duplicateRow "
      + "MERGE p2 = (:A)-[:R]->(:B {k6: false}) WITH p2 CALL (*) { UNWIND nodes(p2) AS e SET e.k6 = true RETURN count(*) AS alias36 } "
      + "RETURN p2 AS p1";

  @Test
  void reportedStatementsAgree() {
    final long left = rows("CALL () { UNWIND [] AS __empty RETURN null AS p1 UNION " + BODY_WITHOUT_WITH + " } RETURN p1");
    final long leftNodes = nodeCount();
    final long leftRels = relationshipCount();
    clear();
    final long right = rows("CALL () { UNWIND [] AS __empty RETURN null AS p1 UNION " + BODY_WITH_WITH + " } RETURN p1");

    // UNION de-duplicates the 101 identical paths the one MERGEd pattern is bound to
    assertThat(right).as("with the pass-through WITH").isEqualTo(1L);
    assertThat(left).as("without it").isEqualTo(right);
    assertThat(leftNodes).as("nodes without the WITH").isEqualTo(nodeCount());
    assertThat(leftRels).as("relationships without the WITH").isEqualTo(relationshipCount());
    assertThat(nodeCount()).as("the l5 node and the one A-B pair").isEqualTo(3L);
    assertThat(relationshipCount()).isEqualTo(1L);
  }

  @Test
  void withoutUnionOrCallBody() {
    assertThat(rows(BODY_WITHOUT_WITH)).as("one row per UNWIND element").isEqualTo(101L);
    assertThat(nodeCount()).isEqualTo(3L);
    assertThat(relationshipCount()).isEqualTo(1L);
    clear();
    assertThat(rows(BODY_WITH_WITH)).isEqualTo(101L);
    assertThat(nodeCount()).isEqualTo(3L);
    assertThat(relationshipCount()).isEqualTo(1L);
  }

  @ParameterizedTest
  @ValueSource(ints = { 1, 2, 99, 100, 101, 250, 1000 })
  void everyInputSizeAnswersLikeTheClauseAtATimeRule(final int rows) {
    // the pull batches carry 100 rows: the write of the first batch must not reach the MERGE of the second one
    final String body = "UNWIND range(1, " + rows + ") AS i MERGE p2 = (:A)-[:R]->(:B {k6: false}) "
        + "CALL (*) { UNWIND nodes(p2) AS e SET e.k6 = true RETURN count(*) AS c } RETURN p2";
    assertThat(rows(body)).isEqualTo(rows);
    assertThat(nodeCount()).as("one A-B pair for %d rows", rows).isEqualTo(2L);
    assertThat(relationshipCount()).isEqualTo(1L);
  }

  @Test
  void aBodyThatRemovesOrRewritesTheMatchedPropertyGetsTheSameBarrier() {
    assertThat(rows("UNWIND range(1, 150) AS i MERGE (n:Item {key: 'v'}) CALL (n) { REMOVE n.key RETURN 1 AS x } RETURN n")).isEqualTo(150L);
    assertThat(nodeCount()).as("REMOVE of the merged key in the body").isEqualTo(1L);
    clear();
    assertThat(rows("UNWIND range(1, 150) AS i MERGE (n:Item {key: 'v'}) CALL (n) { SET n.key = 'w' RETURN 1 AS x } RETURN n")).isEqualTo(150L);
    assertThat(nodeCount()).as("SET of the merged key in the body").isEqualTo(1L);
  }

  @Test
  void aWriteNestedInAUnionBranchOrAnInnerSubqueryGetsTheSameBarrier() {
    assertThat(rows("UNWIND range(1, 150) AS i MERGE (n:Item {key: 'v'}) "
        + "CALL (n) { SET n.key = 'a' RETURN 1 AS x UNION ALL SET n.key = 'b' RETURN 2 AS x } RETURN n")).isEqualTo(300L);
    assertThat(nodeCount()).as("UNION ALL branches").isEqualTo(1L);
    clear();
    assertThat(rows("UNWIND range(1, 150) AS i MERGE (n:Item {key: 'v'}) "
        + "CALL (n) { CALL (n) { SET n.key = 'a' RETURN 1 AS y } RETURN y AS x } RETURN n")).isEqualTo(150L);
    assertThat(nodeCount()).as("an inner subquery").isEqualTo(1L);
    clear();
    assertThat(rows("UNWIND range(1, 150) AS i MERGE (n:Item {key: 'v'}) "
        + "CALL (n) { FOREACH (j IN [1] | SET n.key = 'a') RETURN 1 AS x } RETURN n")).isEqualTo(150L);
    assertThat(nodeCount()).as("a FOREACH of the body").isEqualTo(1L);
  }

  @Test
  void aBodyThatWritesAnUnrelatedKeyStillMergesPerRow() {
    // nothing the MERGE matches on is written: the result is the same either way, and the rows are all there
    assertThat(rows("UNWIND range(1, 150) AS i MERGE (n:Item {key: 'v'}) CALL (n) { SET n.other = 1 RETURN 1 AS x } RETURN n")).isEqualTo(150L);
    assertThat(nodeCount()).isEqualTo(1L);
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:Item) RETURN n.other AS o")) {
      assertThat(((Number) rs.next().getProperty("o")).intValue()).isEqualTo(1);
    }
  }

  @Test
  void subqueryWriteIsAppliedToEveryRow() {
    rows(BODY_WITHOUT_WITH);
    try (final ResultSet rs = database.query("opencypher", "MATCH (n) WHERE n:A OR n:B RETURN n.k6 AS k")) {
      int count = 0;
      while (rs.hasNext()) {
        assertThat((Object) rs.next().getProperty("k")).isEqualTo(true);
        ++count;
      }
      assertThat(count).isEqualTo(2);
    }
  }

  private long rows(final String cypher) {
    long count = 0;
    try (final ResultSet rs = database.command("opencypher", cypher)) {
      while (rs.hasNext()) {
        rs.next();
        ++count;
      }
    }
    return count;
  }

  private long nodeCount() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (n) RETURN count(n) AS c")) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private long relationshipCount() {
    try (final ResultSet rs = database.query("opencypher", "MATCH ()-[r]->() RETURN count(r) AS c")) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private void clear() {
    database.command("opencypher", "MATCH (n) DETACH DELETE n").close();
  }
}
