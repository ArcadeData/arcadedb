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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Vertex;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end SQL-level tests for RESTORE DOCUMENT/VERTEX/EDGE, exercising the full ANTLR grammar -> AST builder ->
 * statement -> engine primitive pipeline (unlike {@code RestoreRecordAtPositionTest}/{@code RestoreVertexAtTest},
 * which call the Java API directly).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RestoreSqlStatementTest extends TestHelper {

  private static final String VERTEX_TYPE = "SqlPerson";
  private static final String EDGE_TYPE   = "SqlKnows";
  private static final String DOC_TYPE    = "SqlLog";

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // restoreEdgeRefusesAMissingEndpoint deliberately leaves a dangling edge behind (the refused restore must not
    // have "fixed" it) - the post-test integrity check would correctly, but irrelevantly, flag that.
    return false;
  }

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createVertexType(VERTEX_TYPE);
      database.getSchema().createEdgeType(EDGE_TYPE);
      database.getSchema().createDocumentType(DOC_TYPE);
    });
  }

  @Test
  void restoreDocumentViaSql() {
    final DatabaseInternal db = (DatabaseInternal) database;
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument(DOC_TYPE).set("name", "original").save().getIdentity());

    final LocalBucket bucket = (LocalBucket) db.getSchema().getBucketById(rid[0].getBucketId());
    database.transaction(() -> bucket.deleteRecord(rid[0]));
    assertThat(bucket.existsRecord(rid[0])).isFalse();

    database.transaction(() -> {
      final ResultSet rs = database.command("sql",
          "RESTORE DOCUMENT " + DOC_TYPE + " RID " + rid[0] + " SET name = 'restored-via-sql'");
      final Result row = rs.next();
      assertThat(row.<String>getProperty("operation")).isEqualTo("restore document");
      assertThat(row.<String>getProperty("record")).isEqualTo(rid[0].toString());
    });

    database.transaction(
        () -> assertThat(database.lookupByRID(rid[0], true).asDocument().getString("name")).isEqualTo("restored-via-sql"));
  }

  @Test
  void restoreVertexViaSqlReconnectsEdges() {
    final DatabaseInternal db = (DatabaseInternal) database;
    final RID[] hub = new RID[1];
    final RID[] leaves = new RID[2];

    database.transaction(() -> {
      final var h = database.newVertex(VERTEX_TYPE).set("name", "hub").save();
      hub[0] = h.getIdentity();
      leaves[0] = database.newVertex(VERTEX_TYPE).set("name", "leaf0").save().getIdentity();
      leaves[1] = database.newVertex(VERTEX_TYPE).set("name", "leaf1").save().getIdentity();
      h.newEdge(EDGE_TYPE, leaves[0]).save();
      database.lookupByRID(leaves[1], true).asVertex().newEdge(EDGE_TYPE, hub[0]).save();
    });

    final LocalBucket bucket = (LocalBucket) db.getSchema().getBucketById(hub[0].getBucketId());
    database.transaction(() -> bucket.deleteRecord(hub[0]));
    database.transaction(() -> assertThatThrownBy(() -> database.lookupByRID(hub[0], true)).isInstanceOf(RecordNotFoundException.class));

    database.transaction(() -> {
      final ResultSet rs = database.command("sql",
          "RESTORE VERTEX " + VERTEX_TYPE + " RID " + hub[0] + " SET name = 'hub-restored'");
      final Result row = rs.next();
      assertThat(row.<String>getProperty("operation")).isEqualTo("restore vertex");
      assertThat((Long) row.getProperty("reconnectedOutEdges")).isEqualTo(1L);
      assertThat((Long) row.getProperty("reconnectedInEdges")).isEqualTo(1L);
    });

    database.transaction(() -> {
      final Vertex h = database.lookupByRID(hub[0], true).asVertex();
      assertThat(h.getString("name")).isEqualTo("hub-restored");

      final Set<RID> outNeighbours = new HashSet<>();
      for (final Vertex v : h.getVertices(Vertex.DIRECTION.OUT, EDGE_TYPE))
        outNeighbours.add(v.getIdentity());
      assertThat(outNeighbours).containsExactly(leaves[0]);

      final Set<RID> inNeighbours = new HashSet<>();
      for (final Vertex v : h.getVertices(Vertex.DIRECTION.IN, EDGE_TYPE))
        inNeighbours.add(v.getIdentity());
      assertThat(inNeighbours).containsExactly(leaves[1]);
    });
  }

  @Test
  void restoreEdgeViaSqlWithoutTouchingVertices() {
    final DatabaseInternal db = (DatabaseInternal) database;
    final RID[] v1 = new RID[1];
    final RID[] v2 = new RID[1];
    final RID[] edgeRid = new RID[1];

    database.transaction(() -> {
      final var a = database.newVertex(VERTEX_TYPE).set("name", "a").save();
      final var b = database.newVertex(VERTEX_TYPE).set("name", "b").save();
      v1[0] = a.getIdentity();
      v2[0] = b.getIdentity();
      edgeRid[0] = a.newEdge(EDGE_TYPE, b).set("since", 2020).save().getIdentity();
    });

    // Raw-delete ONLY the edge record - the vertices' adjacency lists still reference it, exactly the scenario this
    // statement is for.
    final LocalBucket edgeBucket = (LocalBucket) db.getSchema().getBucketById(edgeRid[0].getBucketId());
    database.transaction(() -> edgeBucket.deleteRecord(edgeRid[0]));
    assertThat(edgeBucket.existsRecord(edgeRid[0])).isFalse();

    database.transaction(() -> {
      final ResultSet rs = database.command("sql",
          "RESTORE EDGE " + EDGE_TYPE + " RID " + edgeRid[0] + " FROM " + v1[0] + " TO " + v2[0] + " SET since = 2020");
      final Result row = rs.next();
      assertThat(row.<String>getProperty("operation")).isEqualTo("restore edge");
    });

    database.transaction(() -> {
      final Vertex a = database.lookupByRID(v1[0], true).asVertex();
      final Set<RID> outNeighbours = new HashSet<>();
      for (final Vertex v : a.getVertices(Vertex.DIRECTION.OUT, EDGE_TYPE))
        outNeighbours.add(v.getIdentity());
      assertThat(outNeighbours).containsExactly(v2[0]);

      assertThat(database.lookupByRID(edgeRid[0], true).asEdge().getInteger("since")).isEqualTo(2020);
    });
  }

  @Test
  void restoreEdgeRefusesAMissingEndpoint() {
    final DatabaseInternal db = (DatabaseInternal) database;
    final RID[] v1 = new RID[1];
    final RID[] edgeRid = new RID[1];

    database.transaction(() -> {
      final var a = database.newVertex(VERTEX_TYPE).set("name", "a").save();
      final var b = database.newVertex(VERTEX_TYPE).set("name", "b").save();
      v1[0] = a.getIdentity();
      edgeRid[0] = a.newEdge(EDGE_TYPE, b).save().getIdentity();
    });

    final LocalBucket edgeBucket = (LocalBucket) db.getSchema().getBucketById(edgeRid[0].getBucketId());
    // A RID that was never a real vertex.
    final RID fakeVertex = new RID(v1[0].getBucketId(), 999_999L);
    database.transaction(() -> edgeBucket.deleteRecord(edgeRid[0]));

    assertThatThrownBy(() -> database.transaction(() -> database.command("sql",
        "RESTORE EDGE " + EDGE_TYPE + " RID " + edgeRid[0] + " FROM " + v1[0] + " TO " + fakeVertex))).isInstanceOf(
        CommandSQLParsingException.class).hasMessageContaining("does not exist");
  }

  @Test
  void restoreRefusesAnOccupiedSlotViaSql() {
    final DatabaseInternal db = (DatabaseInternal) database;
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newVertex(VERTEX_TYPE).set("name", "still-here").save().getIdentity());

    assertThatThrownBy(() -> database.transaction(
        () -> database.command("sql", "RESTORE VERTEX " + VERTEX_TYPE + " RID " + rid[0]))).isInstanceOf(
        com.arcadedb.exception.DatabaseOperationException.class).hasMessageContaining("occupied");

    database.transaction(
        () -> assertThat(database.lookupByRID(rid[0], true).asVertex().getString("name")).isEqualTo("still-here"));
  }
}
