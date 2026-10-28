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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issues #10886, #10889, #10890, #10891 and #10892.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class OpenCypherIssues10886To10892Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-issues10886-10892").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      if (database.isOpen())
        database.drop();
      database = null;
    }
  }

  private List<Result> command(final String query) {
    final List<Result> rows = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", query)) {
        while (rs.hasNext())
          rows.add(rs.next());
      }
    });
    return rows;
  }

  private List<Result> commandNoTx(final String query) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.command("opencypher", query)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }

  private long countNodes(final String label) {
    final List<Result> rows = command("MATCH (n:" + label + ") RETURN count(n) AS nodes");
    return ((Number) rows.getFirst().getProperty("nodes")).longValue();
  }

  // #10892
  @Test
  void limitZeroAfterAggregatingWithReturnsNoRow() {
    assertThat(command("UNWIND [1] AS x WITH collect(x) AS q LIMIT 0 RETURN q")).isEmpty();
  }

  // #10892
  @Test
  void limitZeroAfterAggregatingWithFeedsNoRowToCreate() {
    assertThat(command("UNWIND [1] AS x WITH collect(x) AS q LIMIT 0 CREATE (:Item) RETURN q")).isEmpty();
    assertThat(countNodes("Item")).isEqualTo(0L);
  }

  // #10892
  @Test
  void limitZeroOnPlainWithAndReturn() {
    assertThat(command("UNWIND [1, 2] AS x WITH x LIMIT 0 RETURN x")).isEmpty();
    assertThat(command("UNWIND [1, 2] AS x RETURN count(x) AS c LIMIT 0")).isEmpty();
    assertThat(command("UNWIND [1, 2] AS x WITH x, count(*) AS c LIMIT 0 RETURN x")).isEmpty();
    assertThat(command("RETURN 1 AS x LIMIT 0")).isEmpty();
  }

  // #10891
  @Test
  void withStarInCallSubqueryImportsOuterVariables() {
    command("CREATE (:Item {value: 1})");

    final List<Result> star = command("UNWIND [1] AS a CALL { WITH * MATCH (n:Item) WHERE n.value = a RETURN 1 AS y } RETURN 1 AS x");
    assertThat(star).hasSize(1);
    assertThat(((Number) star.getFirst().getProperty("x")).intValue()).isEqualTo(1);

    final List<Result> explicit = command("UNWIND [1] AS a CALL { WITH a MATCH (n:Item) WHERE n.value = a RETURN 1 AS y } RETURN 1 AS x");
    assertThat(explicit).hasSize(1);

    final List<Result> scoped = command("UNWIND [1] AS a CALL (a) { MATCH (n:Item) WHERE n.value = a RETURN 1 AS y } RETURN 1 AS x");
    assertThat(scoped).hasSize(1);

    // A non-matching outer value must still filter.
    assertThat(command("UNWIND [2] AS a CALL { WITH * MATCH (n:Item) WHERE n.value = a RETURN 1 AS y } RETURN 1 AS x")).isEmpty();
  }

  // #10891
  @Test
  void withStarInCallSubqueryImportsOuterVariableIntoInlinePattern() {
    command("CREATE (:Item {value: 1})");
    final List<Result> rows = command("UNWIND [1, 2] AS a CALL { WITH * MATCH (n:Item {value: a}) RETURN n.value AS v } RETURN a, v");
    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.getFirst().getProperty("a")).intValue()).isEqualTo(1);
  }

  // #10890
  @Test
  void foreachSetIsVisibleThroughExistingPathBinding() {
    command("CREATE (:Item {id: 1, value: 10})");

    final List<Result> rows = command(
        "MATCH p = (n:Item {id: 1}) FOREACH (item IN nodes(p) | SET item.value = 20) RETURN [item IN nodes(p) | item.value] AS values");
    assertThat(rows).hasSize(1);
    assertThat((List<Object>) rows.getFirst().getProperty("values")).containsExactly(20L);

    final List<Result> after = command("MATCH (n:Item {id: 1}) RETURN n.value AS value");
    assertThat(((Number) after.getFirst().getProperty("value")).intValue()).isEqualTo(20);
  }

  // #10890
  @Test
  void setIsVisibleThroughExistingPathBindingAndRelationships() {
    command("CREATE (:Item {id: 1, value: 10})-[:REL {w: 1}]->(:Item {id: 2, value: 10})");

    final List<Result> rows = command(
        "MATCH p = (a:Item {id: 1})-[r:REL]->(b:Item) SET a.value = 30, b.value = 40, r.w = 5 "
            + "RETURN [x IN nodes(p) | x.value] AS values, [x IN relationships(p) | x.w] AS ws");
    assertThat(rows).hasSize(1);
    assertThat((List<Object>) rows.getFirst().getProperty("values")).containsExactly(30L, 40L);
    assertThat((List<Object>) rows.getFirst().getProperty("ws")).containsExactly(5L);
  }

  // #10889: Cypher clause-at-a-time semantics (Neo4j inserts an Eager barrier between a MERGE and a following SET that
  // writes a property the MERGE pattern reads), so both forms see the MERGE of every row before any SET and produce
  // the same graph: one node.
  @Test
  void mergeFollowedBySetOfMergeKeyIsIndependentOfWithProjections() {
    final List<Result> a = command("UNWIND [1, 2] AS x MERGE (n:Item:Tagged {id: 1, key: 'value'}) SET n.key = null RETURN x, n");
    assertThat(a).hasSize(2);
    final long nodesA = countNodes("Item");

    command("MATCH (n) DETACH DELETE n");

    final List<Result> b = command(
        "UNWIND [1, 2] AS x WITH x MERGE (n:Item:Tagged {id: 1, key: 'value'}) WITH x, n SET n.key = null WITH x, n RETURN x, n");
    assertThat(b).hasSize(2);
    final long nodesB = countNodes("Item");

    assertThat(nodesA).isEqualTo(nodesB);
    assertThat(nodesA).isEqualTo(1L);
  }

  // #10889: the WITH form gave one node only while the input fit one pull batch of 100 rows
  @Test
  void mergeFollowedBySetOfMergeKeyDoesNotDependOnTheBatchSize() {
    final List<Result> rows = command(
        "UNWIND range(1, 250) AS x WITH x MERGE (n:Item {id: 1, key: 'value'}) WITH x, n SET n.key = null RETURN x, n");
    assertThat(rows).hasSize(250);
    assertThat(countNodes("Item")).isEqualTo(1L);
  }

  // #10889: the same conflict through REMOVE, a FOREACH body and a later MERGE's ON MATCH SET
  @Test
  void mergeFollowedByOtherWritesOfMergeKey() {
    command("UNWIND range(1, 3) AS x MERGE (n:Item {id: 1, key: 'value'}) REMOVE n.key");
    assertThat(countNodes("Item")).isEqualTo(1L);
    command("MATCH (n) DETACH DELETE n");

    command("UNWIND range(1, 3) AS x MERGE (n:Item {id: 1, key: 'value'}) FOREACH (i IN [1] | SET n.key = null)");
    assertThat(countNodes("Item")).isEqualTo(1L);
    command("MATCH (n) DETACH DELETE n");

    command("UNWIND range(1, 3) AS x MERGE (n:Item {id: 1, key: 'value'}) SET n += {key: 'other'}");
    assertThat(countNodes("Item")).isEqualTo(1L);
    command("MATCH (n) DETACH DELETE n");

    command("UNWIND range(1, 3) AS x MERGE (n:Item {id: 1, key: 'value'}) MERGE (m:Other {id: x}) ON CREATE SET n.key = 'gone'");
    assertThat(countNodes("Item")).isEqualTo(1L);
  }

  // #10889: a write the MERGE does not match on, and a MERGE that starts the statement, keep streaming
  @Test
  void mergeFollowedByNonConflictingSetPlansNoBarrier() {
    final String nonConflicting = "UNWIND range(1, 3) AS x MERGE (n:Item {id: x}) SET n.name = 'a'";
    assertThat(explain(nonConflicting)).doesNotContain("EAGER");
    command(nonConflicting);
    assertThat(countNodes("Item")).isEqualTo(3L);

    final String standalone = "MERGE (n:Single {id: 1, key: 'value'}) SET n.key = null";
    assertThat(explain(standalone)).doesNotContain("EAGER");
    command(standalone);
    assertThat(countNodes("Single")).isEqualTo(1L);

    assertThat(explain("UNWIND range(1, 3) AS x MERGE (n:Item {id: 1, key: 'value'}) SET n.key = null")).contains("EAGER");
  }

  // #10892: SKIP of an aggregating WITH was dropped as well
  @Test
  void skipAfterAggregatingWith() {
    final List<Result> grouped = command("UNWIND [1, 2, 3] AS x WITH x, count(*) AS c SKIP 1 RETURN x");
    assertThat(grouped).hasSize(2);
    assertThat(command("UNWIND [1, 2, 3] AS x WITH collect(x) AS q SKIP 1 RETURN q")).isEmpty();
    final List<Result> ordered = command("UNWIND [1, 2, 3] AS x WITH x, count(*) AS c ORDER BY x SKIP 1 LIMIT 1 RETURN x");
    assertThat(ordered).hasSize(1);
    assertThat(((Number) ordered.getFirst().getProperty("x")).intValue()).isEqualTo(2);
    final List<Result> limited = command("UNWIND [1, 2, 3] AS x WITH collect(x) AS q LIMIT 1 RETURN q");
    assertThat(limited).hasSize(1);
  }

  private String explain(final String query) {
    try (final ResultSet rs = database.command("opencypher", "EXPLAIN " + query)) {
      return rs.getExecutionPlan().get().prettyPrint(0, 2);
    }
  }

  // #10886
  @Test
  void callInTransactionsKeepsProcessingAfterAnEmptyBatch() {
    final List<Result> rows = commandNoTx(
        "UNWIND range(1, 20) AS i CALL (i) { WITH i WHERE i > 10 CREATE (:TxProbe {i: i}) RETURN i AS output } "
            + "IN TRANSACTIONS OF 10 ROWS RETURN count(output) AS outputRows");
    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.getFirst().getProperty("outputRows")).longValue()).isEqualTo(10L);
    assertThat(countNodes("TxProbe")).isEqualTo(10L);
  }

  // #10886
  @Test
  void callInTransactionsKeepsProcessingAfterSeveralEmptyBatches() {
    final List<Result> rows = commandNoTx(
        "UNWIND range(1, 50) AS i CALL (i) { WITH i WHERE i % 20 = 0 CREATE (:TxProbe {i: i}) RETURN i AS output } "
            + "IN TRANSACTIONS OF 3 ROWS RETURN i, output");
    assertThat(rows).hasSize(2);
    assertThat(countNodes("TxProbe")).isEqualTo(2L);
  }
}
