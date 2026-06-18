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
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Regression test for issue #471: RecordNotFoundException in NOT EXISTS pattern predicate
 * when an edge record itself is missing (ghost edge left by HA recovery).
 *
 * During HA manual recovery (copying leader DB to followers and wiping Raft logs), edge
 * records can go missing while the vertex's edge segment list still references them. The
 * NOT EXISTS predicate must skip such ghost edges gracefully instead of propagating
 * RecordNotFoundException.
 */
class CypherNotExistsGhostEdgeTest {

  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypher-not-exists-ghost-edge");
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
  void teardown() {
    if (database != null)
      database.drop();
  }

  /**
   * Reproduces the exact scenario from issue #471: a ghost INITIATED edge (its record was
   * deleted directly, bypassing the graph engine, to simulate HA data loss) causes
   * RecordNotFoundException in checkRelationshipExists when the NOT EXISTS predicate
   * iterates the Account's outgoing edge list and calls edge.getIn().
   */
  @Test
  void notExistsWithGhostEdgeDoesNotThrow() {
    final RID[] ghostEdgeRid = new RID[1];

    database.transaction(() -> {
      final MutableVertex account = database.newVertex("Account").set("number", "ACC-001").save();
      final MutableVertex ghostTarget = database.newVertex("Transaction").set("id", "TXN-GHOST").save();
      // Create an INITIATED edge that will become a ghost
      final MutableEdge ghostEdge = account.newEdge("INITIATED", ghostTarget, new Object[0]).set("transaction_id", "TXN-001").save();
      ghostEdgeRid[0] = ghostEdge.getIdentity();
    });

    // Delete only the edge record directly from the bucket, leaving the edge reference
    // in Account's edge segment list intact. This simulates an HA recovery state where
    // the edge record is missing but the vertex still references it.
    database.transaction(() ->
        database.getSchema()
            .getBucketById(ghostEdgeRid[0].getBucketId())
            .deleteRecord(ghostEdgeRid[0])
    );

    // Create a different Transaction node - the one we'll look up in the NOT EXISTS query
    final RID[] txnRid = new RID[1];
    database.transaction(() -> {
      final MutableVertex txn = database.newVertex("Transaction").set("id", "TXN-001").save();
      txnRid[0] = txn.getIdentity();
    });

    // Before the fix this threw RecordNotFoundException from edge.getIn() when the NOT
    // EXISTS predicate iterated the ghost edge entry in Account's edge list.
    assertThatCode(() -> {
      final ResultSet rs = database.query("opencypher",
          "MATCH (a:Account {number: 'ACC-001'}), (t:Transaction {id: 'TXN-001'}) " +
          "WHERE NOT EXISTS ((a)-[:INITIATED]->(t)) " +
          "RETURN a.number AS account, t.id AS txn");
      // The ghost edge points to a deleted record, not to TXN-001; NOT EXISTS is true
      assertThat(rs.hasNext()).isTrue();
      final var row = rs.next();
      assertThat(row.<String>getProperty("account")).isEqualTo("ACC-001");
      assertThat(row.<String>getProperty("txn")).isEqualTo("TXN-001");
    }).doesNotThrowAnyException();
  }

  /**
   * Same ghost-edge scenario but with an unbound end node (checkAnyRelationshipExists path):
   * WHERE NOT EXISTS ((a)-[:INITIATED]->()) must also skip ghost edges gracefully.
   */
  @Test
  void notExistsUnboundEndWithGhostEdgeDoesNotThrow() {
    final RID[] ghostEdgeRid = new RID[1];

    database.transaction(() -> {
      final MutableVertex account = database.newVertex("Account").set("number", "ACC-002").save();
      final MutableVertex ghostTarget = database.newVertex("Transaction").set("id", "TXN-GHOST-2").save();
      final MutableEdge ghostEdge = account.newEdge("INITIATED", ghostTarget, new Object[0]).save();
      ghostEdgeRid[0] = ghostEdge.getIdentity();
    });

    // Delete the edge record directly to create a ghost edge
    database.transaction(() ->
        database.getSchema()
            .getBucketById(ghostEdgeRid[0].getBucketId())
            .deleteRecord(ghostEdgeRid[0])
    );

    // NOT EXISTS with unbound end node - uses checkAnyRelationshipExists code path
    assertThatCode(() -> {
      final ResultSet rs = database.query("opencypher",
          "MATCH (a:Account {number: 'ACC-002'}) " +
          "WHERE NOT EXISTS ((a)-[:INITIATED]->()) " +
          "RETURN a.number AS account");
      // The only INITIATED edge is a ghost; NOT EXISTS should return true
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("account")).isEqualTo("ACC-002");
    }).doesNotThrowAnyException();
  }

  /**
   * Sanity check: when a real (non-ghost) edge exists, NOT EXISTS correctly returns false
   * and the row is filtered out.
   */
  @Test
  void notExistsWithRealEdgeFiltersRow() {
    database.transaction(() -> {
      final MutableVertex account = database.newVertex("Account").set("number", "ACC-003").save();
      final MutableVertex txn = database.newVertex("Transaction").set("id", "TXN-REAL").save();
      account.newEdge("INITIATED", txn, new Object[0]).save();
    });

    final ResultSet rs = database.query("opencypher",
        "MATCH (a:Account {number: 'ACC-003'}), (t:Transaction {id: 'TXN-REAL'}) " +
        "WHERE NOT EXISTS ((a)-[:INITIATED]->(t)) " +
        "RETURN a.number AS account");
    // Real edge exists, NOT EXISTS is false, row should be filtered out
    assertThat(rs.hasNext()).isFalse();
  }
}
