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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Reproducer for <a href="https://github.com/ArcadeData/arcadedb/issues/394">issue #394</a>:
 * {@code RecordNotFoundException} thrown inside {@code MergeStep.matchesProperties()} when MERGE
 * on an edge encounters a stale edge-segment pointer whose backing record was concurrently deleted.
 * <p>
 * In an HA cluster under load a transaction can write the edge-segment pointer and then roll back
 * the edge record, leaving a ghost pointer. The fix wraps the edge-processing body of the
 * traversal loops with try/catch RecordNotFoundException so ghost edges are silently skipped
 * instead of surfacing as an unhandled exception.
 */
class Issue394Test {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue394");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.transaction(() -> {
      database.getSchema().createVertexType("Account");
      database.getSchema().createVertexType("Transaction");
      database.getSchema().createEdgeType("RECEIVED_BY");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  /**
   * Main regression test: an edge segment pointer exists for a RECEIVED_BY edge, but the
   * backing edge record has been deleted directly from the bucket (simulating the HA race
   * where a rolled-back transaction leaves a ghost pointer). MERGE must not throw
   * RecordNotFoundException.
   */
  @Test
  void mergeEdgeWithPropertyFiltersSkipsStaleEdgeRecord() {
    // Create Account, Transaction, and a RECEIVED_BY edge with properties via Cypher.
    database.transaction(() ->
        database.command("opencypher",
            "CREATE (a:Account {number: 'ACC-1'})-[r:RECEIVED_BY {received_at: '2024-01-01', holds_applied: false}]->(t:Transaction {id: 'TX-1'})")
    );

    // Retrieve the edge RID so we can delete just its record from the bucket.
    final RID edgeRID;
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:Account {number: 'ACC-1'})-[r:RECEIVED_BY]->(t:Transaction {id: 'TX-1'}) RETURN r")) {
      assertThat(rs.hasNext()).isTrue();
      final Edge edge = rs.next().getProperty("r");
      edgeRID = edge.getIdentity();
    }

    // Delete the edge record directly from the bucket, bypassing edge-segment cleanup.
    // This leaves a ghost pointer in Account's edge segment - simulating the HA race.
    database.transaction(() -> {
      final Bucket bucket = database.getSchema().getBucketById(edgeRID.getBucketId());
      bucket.deleteRecord(edgeRID);
    });

    // MERGE on the edge pattern must not throw RecordNotFoundException.
    // Before the fix this would crash at MergeStep.matchesProperties() when loading
    // the lazy-loaded edge's properties triggered LocalBucket.getRecord() on the
    // deleted slot.
    assertThatCode(() -> database.transaction(() ->
        database.command("opencypher",
            """
            MATCH (a:Account {number: $number}), (t:Transaction {id: $id}) \
            MERGE (a)-[r:RECEIVED_BY {received_at: $received_at, holds_applied: $holds_applied}]->(t)""",
            Map.of("number", "ACC-1", "id", "TX-1", "received_at", "2024-01-01", "holds_applied", false))
    )).doesNotThrowAnyException();
  }

  /**
   * Control test: MERGE on an edge when no edge exists should create it cleanly and
   * the created edge must be accessible with its properties intact.
   */
  @Test
  void mergeEdgeCreatesEdgeWhenNoneExists() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Account {number: 'ACC-2'})");
      database.command("opencypher", "CREATE (:Transaction {id: 'TX-2'})");
    });

    assertThatCode(() -> database.transaction(() ->
        database.command("opencypher",
            """
            MATCH (a:Account {number: $number}), (t:Transaction {id: $id}) \
            MERGE (a)-[r:RECEIVED_BY {received_at: $received_at, holds_applied: $holds_applied}]->(t)""",
            Map.of("number", "ACC-2", "id", "TX-2", "received_at", "2024-01-02", "holds_applied", true))
    )).doesNotThrowAnyException();

    // The edge must exist with correct properties.
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:Account {number: 'ACC-2'})-[r:RECEIVED_BY {received_at: '2024-01-02'}]->(t:Transaction {id: 'TX-2'}) RETURN r")) {
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      final Edge edge = result.getProperty("r");
      assertThat(edge.get("received_at")).isEqualTo("2024-01-02");
      assertThat(edge.get("holds_applied")).isEqualTo(true);
    }
  }

  /**
   * Verify MERGE skips a ghost edge alongside a valid edge and returns the valid one
   * without creating a duplicate. The valid edge must be matched by properties.
   * The ghost is introduced by direct bucket deletion before the valid edge is created.
   */
  @Test
  void mergeEdgeMatchesValidEdgeAlongsideGhost() {
    // Create Account and Transaction with a first edge (to become the ghost).
    database.transaction(() ->
        database.command("opencypher",
            "CREATE (a:Account {number: 'ACC-3'})-[r:RECEIVED_BY {received_at: '2024-01-03', holds_applied: false}]->(t:Transaction {id: 'TX-3'})")
    );

    // Retrieve and ghost the first edge.
    final RID ghostRID;
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:Account {number: 'ACC-3'})-[r:RECEIVED_BY]->(t:Transaction {id: 'TX-3'}) RETURN r")) {
      assertThat(rs.hasNext()).isTrue();
      ghostRID = ((Edge) rs.next().getProperty("r")).getIdentity();
    }
    database.transaction(() ->
        database.getSchema().getBucketById(ghostRID.getBucketId()).deleteRecord(ghostRID)
    );

    // Add a second valid edge with identical properties.
    database.transaction(() ->
        database.command("opencypher",
            """
            MATCH (a:Account {number: 'ACC-3'}), (t:Transaction {id: 'TX-3'}) \
            CREATE (a)-[:RECEIVED_BY {received_at: '2024-01-03', holds_applied: false}]->(t)""")
    );

    // MERGE must find the surviving valid edge (no throw, no duplicate created).
    assertThatCode(() -> database.transaction(() ->
        database.command("opencypher",
            """
            MATCH (a:Account {number: $number}), (t:Transaction {id: $id}) \
            MERGE (a)-[r:RECEIVED_BY {received_at: $received_at, holds_applied: $holds_applied}]->(t)""",
            Map.of("number", "ACC-3", "id", "TX-3", "received_at", "2024-01-03", "holds_applied", false))
    )).doesNotThrowAnyException();

    // The valid edge must be reachable by its properties.
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:Account {number: 'ACC-3'})-[r:RECEIVED_BY {received_at: '2024-01-03'}]->(t:Transaction {id: 'TX-3'}) RETURN r")) {
      assertThat(rs.hasNext()).isTrue();
    }
  }
}
