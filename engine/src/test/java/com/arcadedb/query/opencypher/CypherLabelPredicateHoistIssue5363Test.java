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
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #5363: a label predicate expressed in the WHERE clause
 * ({@code MATCH (n) WHERE n:Parent}) forced the traditional (non-optimized) execution path and
 * scanned every vertex type in the database, while the equivalent pattern form
 * ({@code MATCH (n:Parent)}) used a NodeByLabelScan.
 * <p>
 * Neo4j solves the {@code HasLabels} predicate with a NodeByLabelScan in both forms, so the two
 * spellings must produce the same plan.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherLabelPredicateHoistIssue5363Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/cypher-label-predicate-hoist-5363").create();
    database.getSchema().createVertexType("Parent");
    database.getSchema().getType("Parent").createProperty("id", Type.LONG);
    database.getSchema().createVertexType("Child").addSuperType("Parent");
    database.getSchema().createVertexType("Other");
    database.getSchema().createVertexType("Company");

    database.transaction(() -> {
      final MutableVertex parent = database.newVertex("Parent");
      parent.set("id", 1L);
      parent.save();

      final MutableVertex child = database.newVertex("Child");
      child.set("id", 2L);
      child.save();

      for (int i = 0; i < 500; i++) {
        final MutableVertex other = database.newVertex("Other");
        other.set("id", (long) i);
        other.save();
      }
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void labelPredicateInWhereUsesLabelScan() {
    final String plan = explain("MATCH (n) WHERE n:Parent RETURN n.id AS id ORDER BY id");

    assertThat(plan).contains("Using Cost-Based Query Optimizer");
    assertThat(plan).contains("NodeByLabelScan(n:Parent)");
  }

  @Test
  void labelPredicateInWhereReturnsSameRowsAsPatternForm() {
    assertThat(ids("MATCH (n:Parent) RETURN n.id AS id ORDER BY id")).containsExactly(1L, 2L);
    assertThat(ids("MATCH (n) WHERE n:Parent RETURN n.id AS id ORDER BY id")).containsExactly(1L, 2L);
  }

  @Test
  void labelPredicateCombinedWithPropertyFilterStillFilters() {
    assertThat(ids("MATCH (n) WHERE n:Parent AND n.id = 2 RETURN n.id AS id ORDER BY id")).containsExactly(2L);
  }

  @Test
  void labelPredicateOnPatternWithExistingLabelIsIntersected() {
    // Parent AND Child -> only the Child vertex qualifies
    assertThat(ids("MATCH (n:Parent) WHERE n:Child RETURN n.id AS id ORDER BY id")).containsExactly(2L);
  }

  @Test
  void labelDisjunctionInWhereIsNotHoistedIntoUnrelatedLabel() {
    // n:Parent OR n:Other is not a top-level conjunct: results must still be correct
    final List<Long> ids = ids("MATCH (n) WHERE n:Parent OR n:Company RETURN n.id AS id ORDER BY id");
    assertThat(ids).containsExactly(1L, 2L);
  }

  @Test
  void negatedLabelPredicateIsNotHoisted() {
    final ResultSet rs = database.query("opencypher", "MATCH (n) WHERE NOT n:Other RETURN count(n) AS c");
    final long count = ((Number) rs.next().getProperty("c")).longValue();
    rs.close();
    assertThat(count).isEqualTo(2L);
  }

  @Test
  void unknownLabelPredicateReturnsEmptyWithoutError() {
    final ResultSet rs = database.query("opencypher", "MATCH (n) WHERE n:DoesNotExist RETURN count(n) AS c");
    final long count = ((Number) rs.next().getProperty("c")).longValue();
    rs.close();
    assertThat(count).isEqualTo(0L);
  }

  @Test
  void labelPredicateInOptionalMatchWhereKeepsOptionalSemantics() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (p:Parent) WHERE p.id = 1 OPTIONAL MATCH (n) WHERE n:DoesNotExist RETURN p.id AS id, n");
    final Result row = rs.next();
    assertThat(row.<Long>getProperty("id")).isEqualTo(1L);
    assertThat((Object) row.getProperty("n")).isNull();
    assertThat(rs.hasNext()).isFalse();
    rs.close();
  }

  @Test
  void labelPredicateOnRelationshipVariableIsNotHoistedIntoNode() {
    database.getSchema().createEdgeType("KNOWS");
    database.getSchema().createEdgeType("LIKES");
    database.transaction(() -> {
      database.command("opencypher", "MATCH (a:Parent {id: 1}), (b:Child {id: 2}) CREATE (a)-[:KNOWS]->(b)").close();
    });

    final ResultSet rs = database.query("opencypher", "MATCH (a)-[r]->(b) WHERE r:KNOWS RETURN count(r) AS c");
    final long count = ((Number) rs.next().getProperty("c")).longValue();
    rs.close();
    assertThat(count).isEqualTo(1L);
  }

  private String explain(final String query) {
    final ResultSet rs = database.query("opencypher", "EXPLAIN " + query);
    final String plan = rs.getExecutionPlan().get().prettyPrint(0, 2);
    rs.close();
    return plan;
  }

  private List<Long> ids(final String query) {
    final List<Long> ids = new ArrayList<>();
    final ResultSet rs = database.query("opencypher", query);
    while (rs.hasNext()) {
      final Object id = rs.next().getProperty("id");
      ids.add(id == null ? null : ((Number) id).longValue());
    }
    rs.close();
    return ids;
  }
}
