/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.engine.Bucket;
import com.arcadedb.graph.Edge;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Regression test for a client HA-cluster crash: a {@code WHERE NOT EXISTS ((a)-[:T]->(t))} pattern
 * predicate threw {@code RecordNotFoundException} from {@code PatternPredicateExpression} when the
 * vertex's edge segment held a ghost pointer (the backing edge record had been deleted/never
 * replicated). After a manual leader-to-follower database copy the new leader can hold such dangling
 * edge pointers; evaluating the predicate called {@code edge.getIn()} which lazily loads the missing
 * edge record and crashed the whole Cypher command (and, on the leader's Raft apply path, destabilized
 * the cluster).
 * <p>
 * Mirrors the precedent set by {@code Issue394Test} (ghost edges in MERGE): the per-edge body of the
 * pattern-predicate loops now skips ghost edges instead of propagating the exception.
 */
class PatternPredicateGhostEdgeTest {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/patternPredicateGhostEdge");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.transaction(() -> {
      database.getSchema().createVertexType("Account");
      database.getSchema().createVertexType("Transaction");
      database.getSchema().createEdgeType("INITIATED");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  /**
   * The client query: MATCH both ends, then WHERE NOT EXISTS the relationship, then CREATE it.
   * With a ghost INITIATED edge in the Account's segment, the predicate must skip the ghost
   * (not throw) and conclude the relationship does not exist, so CREATE proceeds.
   */
  @Test
  void whereNotExistsSkipsGhostOutgoingEdge() {
    database.transaction(() ->
        database.command("opencypher",
            "CREATE (a:Account {number: 'ACC-1'})-[:INITIATED {transaction_id: 'TX-1'}]->(t:Transaction {id: 'TX-1'})"));

    // Capture the edge RID, then delete only its backing record - leaving a ghost pointer in
    // Account's edge segment (simulating the dangling edge seen after a manual HA db copy).
    final RID edgeRID;
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:Account {number: 'ACC-1'})-[r:INITIATED]->(t:Transaction {id: 'TX-1'}) RETURN r")) {
      assertThat(rs.hasNext()).isTrue();
      edgeRID = ((Edge) rs.next().getProperty("r")).getIdentity();
    }
    database.transaction(() -> {
      final Bucket bucket = database.getSchema().getBucketById(edgeRID.getBucketId());
      bucket.deleteRecord(edgeRID);
    });

    // The client's command. Before the fix this threw RecordNotFoundException at
    // PatternPredicateExpression.checkRelationshipExists -> edge.getIn().
    assertThatCode(() -> database.transaction(() ->
        database.command("opencypher",
            """
            MATCH (a:Account {number: $accountNumber}), (t:Transaction {id: $id}) \
            WHERE NOT EXISTS ((a)-[:INITIATED {transaction_id: $id}]->(t)) \
            CREATE (a)-[:INITIATED {transaction_id: $id, channel: $channel}]->(t)""",
            Map.of("accountNumber", "ACC-1", "id", "TX-1", "channel", "web")))
    ).doesNotThrowAnyException();

    // The predicate skipped the ghost, so NOT EXISTS held and a fresh edge (channel='web') was created.
    // We assert on that created edge specifically rather than the total edge count: deleting the edge
    // record frees its slot, which CREATE may reuse, so the total is an allocation artifact. That a
    // genuinely-dangling ghost never surfaces as a result row is proven by matchExpandSkipsGhostEdge.
    try (final ResultSet rs = database.query("opencypher",
        """
        MATCH (a:Account {number: 'ACC-1'})-[r:INITIATED]->(t:Transaction {id: 'TX-1'}) \
        WHERE r.channel = 'web' RETURN count(r) AS c""")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("c")).intValue()).isGreaterThanOrEqualTo(1);
    }
  }

  /**
   * Same hardening for the open-ended form WHERE NOT EXISTS ((a)-[:INITIATED]->()) where the end
   * node is not bound: checkAnyRelationshipExists must skip a ghost edge rather than throw.
   */
  @Test
  void whereNotExistsSkipsGhostEdgeWithUnboundEnd() {
    database.transaction(() ->
        database.command("opencypher",
            "CREATE (a:Account {number: 'ACC-2'})-[:INITIATED {transaction_id: 'TX-2'}]->(t:Transaction {id: 'TX-2'})"));

    final RID edgeRID;
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:Account {number: 'ACC-2'})-[r:INITIATED]->(t:Transaction {id: 'TX-2'}) RETURN r")) {
      assertThat(rs.hasNext()).isTrue();
      edgeRID = ((Edge) rs.next().getProperty("r")).getIdentity();
    }
    database.transaction(() ->
        database.getSchema().getBucketById(edgeRID.getBucketId()).deleteRecord(edgeRID));

    assertThatCode(() -> {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:Account {number: 'ACC-2'}) WHERE NOT EXISTS ((a)-[:INITIATED]->()) RETURN a")) {
        // A ghost is the only edge: it is skipped, so the predicate holds and the row survives.
        assertThat(rs.hasNext()).isTrue();
      }
    }).doesNotThrowAnyException();
  }

  /**
   * The plain MATCH expand path (ExpandAll / MatchRelationshipStep) must also skip a ghost edge
   * rather than throw. Returning the relationship (RETURN r) and filtering on an edge property both
   * force the edge record to be materialized, which is exactly when a ghost pointer used to crash.
   * The ghost must contribute no row.
   */
  @Test
  void matchExpandSkipsGhostEdge() {
    database.transaction(() ->
        database.command("opencypher",
            "CREATE (a:Account {number: 'ACC-3'})-[:INITIATED {transaction_id: 'TX-3'}]->(t:Transaction {id: 'TX-3'})"));

    final RID edgeRID;
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:Account {number: 'ACC-3'})-[r:INITIATED]->(t:Transaction {id: 'TX-3'}) RETURN r")) {
      assertThat(rs.hasNext()).isTrue();
      edgeRID = ((Edge) rs.next().getProperty("r")).getIdentity();
    }
    database.transaction(() ->
        database.getSchema().getBucketById(edgeRID.getBucketId()).deleteRecord(edgeRID));

    // Returning the edge forces it to be loaded; the ghost must be skipped, not crash, and yield no row.
    assertThatCode(() -> {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:Account {number: 'ACC-3'})-[r:INITIATED]->(t) RETURN r")) {
        assertThat(rs.hasNext()).isFalse();
      }
    }).doesNotThrowAnyException();

    // Filtering on an edge property also forces a load and must skip the ghost without throwing.
    assertThatCode(() -> {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:Account {number: 'ACC-3'})-[r:INITIATED]->(t) WHERE r.transaction_id = 'TX-3' RETURN t")) {
        assertThat(rs.hasNext()).isFalse();
      }
    }).doesNotThrowAnyException();
  }

  /**
   * Pattern comprehension - [(a)-[:INITIATED]->(t) | t] and its variable-length form - routes through
   * PatternComprehensionExpression (a different path from the WHERE predicate above). A ghost edge in
   * the comprehension must be skipped, not throw, and contribute no element to the produced list.
   */
  @Test
  void patternComprehensionSkipsGhostEdge() {
    database.transaction(() ->
        database.command("opencypher",
            "CREATE (a:Account {number: 'ACC-PC'})-[:INITIATED {transaction_id: 'TX-PC'}]->(t:Transaction {id: 'TX-PC'})"));

    final RID edgeRID;
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:Account {number: 'ACC-PC'})-[r:INITIATED]->(t:Transaction {id: 'TX-PC'}) RETURN r")) {
      edgeRID = ((Edge) rs.next().getProperty("r")).getIdentity();
    }
    database.transaction(() ->
        database.getSchema().getBucketById(edgeRID.getBucketId()).deleteRecord(edgeRID));

    // Single-hop comprehension: the ghost is the only edge, so the list is empty (no throw).
    assertThatCode(() -> {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:Account {number: 'ACC-PC'}) RETURN [(a)-[:INITIATED]->(t) | t] AS targets")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat((List<?>) rs.next().getProperty("targets")).isEmpty();
      }
    }).doesNotThrowAnyException();

    // Variable-length comprehension exercises the traverser inside PatternComprehensionExpression.
    assertThatCode(() -> {
      try (final ResultSet rs = database.query("opencypher",
          "MATCH (a:Account {number: 'ACC-PC'}) RETURN [(a)-[:INITIATED*1..3]->(t) | t] AS targets")) {
        assertThat(rs.hasNext()).isTrue();
        assertThat((List<?>) rs.next().getProperty("targets")).isEmpty();
      }
    }).doesNotThrowAnyException();
  }
}
