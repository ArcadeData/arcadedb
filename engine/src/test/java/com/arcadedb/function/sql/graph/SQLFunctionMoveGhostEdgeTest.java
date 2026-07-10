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
package com.arcadedb.function.sql.graph;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Regression test for the SQL edge-to-vertex functions (outV/inV/bothV, via {@code SQLFunctionMove.e2v}):
 * when chained off {@code outE()}, a ghost edge (segment pointer whose backing edge record is gone)
 * flows into e2v and must be skipped instead of throwing {@code RecordNotFoundException}.
 * <p>
 * Note: {@code out()/in()/both()} are already ghost-safe (they resolve neighbors via getVertices(),
 * reading the target RID from the segment without loading the edge record), and {@code outE()/inE()}
 * return the edge lazily without dereferencing - so e2v is the only function-level deref to harden.
 */
class SQLFunctionMoveGhostEdgeTest {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/sqlMoveGhostEdge");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void inVOverGhostEdgeIsSkipped() {
    // A -[LINK]-> B (to be ghosted), A -[LINK]-> C (valid)
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      a.newEdge("LINK", b);
      a.newEdge("LINK", c);
    });

    // Ghost the A->B edge: delete only its record, leaving the segment pointer.
    final RID ghostRID;
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (a:Node {name:'A'})-[r:LINK]->(b:Node {name:'B'}) RETURN r")) {
      ghostRID = ((Edge) rs.next().getProperty("r")).getIdentity();
    }
    database.transaction(() -> {
      final Bucket bucket = database.getSchema().getBucketById(ghostRID.getBucketId());
      bucket.deleteRecord(ghostRID);
    });

    // outE('LINK').inV() routes every out-edge (including the ghost) through e2v. Must not throw.
    assertThatCode(() -> {
      try (final ResultSet rs = database.query("sql",
          "SELECT expand(outE('LINK').inV()) FROM Node WHERE name = 'A'")) {
        while (rs.hasNext())
          rs.next();
      }
    }).doesNotThrowAnyException();

    // out('LINK') uses getVertices() and is inherently ghost-safe; must also not throw.
    assertThatCode(() -> {
      try (final ResultSet rs = database.query("sql",
          "SELECT expand(out('LINK')) FROM Node WHERE name = 'A'")) {
        while (rs.hasNext())
          rs.next();
      }
    }).doesNotThrowAnyException();
  }
}
