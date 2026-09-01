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
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reproduction for GitHub issue #740 (Locstat).
 * <p>
 * OpenCypher queries that filter on an edge type's indexed properties via a WHERE clause do not use
 * the composite index on the edge type - they fall back to a full vertex scan + edge expansion, while
 * the equivalent SQL query uses the index.
 * <pre>
 *   MATCH ()-[t:TRANSFER]->()
 *   WHERE t.transactionId='74529884' and t.date=date('2026-02-28')
 *   RETURN t
 * </pre>
 */
class Issue740EdgeIndexWhereClauseTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/issue-740-edge-index-where").create();

    database.getSchema().createVertexType("Account");
    final var transfer = database.getSchema().createEdgeType("TRANSFER");
    transfer.createProperty("transactionId", Type.STRING);
    transfer.createProperty("date", Type.DATE);
    // Composite index on the edge type, mirroring the client's TRANSFER_PK on (transactionId, date).
    database.getSchema().buildTypeIndex("TRANSFER", new String[] { "transactionId", "date" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();

    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Account").set("name", "A").save();
      final MutableVertex b = database.newVertex("Account").set("name", "B").save();

      // The one edge the client expects to find.
      a.newEdge("TRANSFER", b).set("transactionId", "74529884").set("date", "2026-02-28").save();

      // A pile of noise edges between the same two accounts and other accounts, so a full scan is
      // clearly distinguishable from a single-key index lookup by the profiled row counts.
      for (int i = 0; i < 500; i++) {
        final MutableVertex c = database.newVertex("Account").set("name", "N" + i).save();
        a.newEdge("TRANSFER", c).set("transactionId", "T" + i).set("date", "2026-01-" + (1 + (i % 28))).save();
        c.newEdge("TRANSFER", b).set("transactionId", "S" + i).set("date", "2026-03-" + (1 + (i % 28))).save();
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
  void whereClauseOnEdgeReturnsCorrectResult() {
    // Correctness first: the query must return exactly the one matching edge.
    try (final ResultSet rs = database.query("opencypher",
        "MATCH ()-[t:TRANSFER]->() WHERE t.transactionId='74529884' AND t.date=date('2026-02-28') RETURN t")) {
      int count = 0;
      while (rs.hasNext()) {
        final Result r = rs.next();
        assertThat((String) ((Edge) r.getProperty("t")).get("transactionId")).isEqualTo("74529884");
        count++;
      }
      assertThat(count).isEqualTo(1);
    }
  }

  @Test
  void whereClauseOnEdgeUsesIndex() {
    final ResultSet rs = database.query("opencypher",
        "PROFILE MATCH ()-[t:TRANSFER]->() WHERE t.transactionId='74529884' AND t.date=date('2026-02-28') RETURN t");
    while (rs.hasNext())
      rs.next();
    final ExecutionPlan plan = rs.getExecutionPlan().get();
    final String planString = plan.prettyPrint(0, 2);
    rs.close();

    // The plan must be driven from the edge type's index, not a full vertex scan + edge expansion.
    assertThat(planString).contains("MATCH EDGE BY INDEX");
    assertThat(planString).contains("index: ");
    // And it must NOT fall back to expanding every edge (the pre-fix behaviour showed a MATCH
    // RELATIONSHIP scanning 1001 edges).
    assertThat(planString).doesNotContain("traversal: standard");
  }

  @Test
  void inlineDirectionsAndCorrectnessMatchWhereForm() {
    // Sanity across the shapes the optimization must not silently change: the incoming-direction form
    // must return the same single edge (found from its IN endpoint) as the SQL reference.
    try (final ResultSet rs = database.query("opencypher",
        "MATCH ()<-[t:TRANSFER]-() WHERE t.transactionId='74529884' AND t.date=date('2026-02-28') RETURN t")) {
      int count = 0;
      while (rs.hasNext()) {
        assertThat((String) ((Edge) rs.next().getProperty("t")).get("transactionId")).isEqualTo("74529884");
        count++;
      }
      assertThat(count).isEqualTo(1);
    }
  }

  @Test
  void parameterizedValuesUseIndexAndReturnCorrectResult() {
    // Parameters are row-independent, so they must drive the same index seek as literals.
    final java.util.Map<String, Object> params = new java.util.HashMap<>();
    params.put("tx", "74529884");
    try (final ResultSet rs = database.query("opencypher",
        "MATCH ()-[t:TRANSFER]->() WHERE t.transactionId=$tx AND t.date=date('2026-02-28') RETURN t", params)) {
      int count = 0;
      while (rs.hasNext()) {
        assertThat((String) ((Edge) rs.next().getProperty("t")).get("transactionId")).isEqualTo("74529884");
        count++;
      }
      assertThat(count).isEqualTo(1);
    }
  }

  @Test
  void unindexedEdgePropertyStillReturnsCorrectResultViaFallback() {
    // A filter on a non-indexed edge property cannot use the seek and must fall back to the normal
    // scan+expand plan, still returning the correct rows. TRANSFER has no index on 'note'.
    database.getSchema().getType("TRANSFER").createProperty("note", Type.STRING);
    // tag the one known edge with a note
    database.transaction(() ->
        database.command("sql", "UPDATE TRANSFER SET note='flagged' WHERE transactionId='74529884'"));

    final ResultSet rs = database.query("opencypher",
        "PROFILE MATCH ()-[t:TRANSFER]->() WHERE t.note='flagged' RETURN t");
    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    final String planString = rs.getExecutionPlan().get().prettyPrint(0, 2);
    rs.close();
    assertThat(count).isEqualTo(1);
    // No edge index covers 'note', so the edge-index seed must NOT be used.
    assertThat(planString).doesNotContain("MATCH EDGE BY INDEX");
  }

  @Test
  void sqlEquivalentUsesIndexForReference() {
    // Reference: the equivalent SQL query DOES use the edge-type index.
    final ResultSet rs = database.query("sql",
        "SELECT FROM TRANSFER WHERE transactionId='74529884' AND date='2026-02-28'");
    final ExecutionPlan plan = rs.getExecutionPlan().get();
    final String planString = plan.prettyPrint(0, 2);
    rs.close();

    assertThat(planString.toUpperCase()).contains("INDEX");
  }
}
