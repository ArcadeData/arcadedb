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
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;

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
  void restoreEdgeUpdatesBucketRecordCount() {
    // #6069: the same missing +1 as RESTORE DOCUMENT/VERTEX, exercised on RESTORE EDGE's own bucket - flagged as
    // untested by the PR #6082 code review even though the fix is the identical one-line change routed through the
    // same RestoreStatementSupport.restoreRecordAndUpdateCount() helper as RestoreDocumentStatement.
    final DatabaseInternal db = (DatabaseInternal) database;
    final RID[] v1 = new RID[1];
    final RID[] v2 = new RID[1];
    final RID[] edgeRid = new RID[3];

    database.transaction(() -> {
      final var a = database.newVertex(VERTEX_TYPE).set("name", "a").save();
      final var b = database.newVertex(VERTEX_TYPE).set("name", "b").save();
      v1[0] = a.getIdentity();
      v2[0] = b.getIdentity();
      for (int i = 0; i < 3; i++)
        edgeRid[i] = a.newEdge(EDGE_TYPE, b).set("i", i).save().getIdentity();
    });

    final LocalBucket edgeBucket = (LocalBucket) db.getSchema().getBucketById(edgeRid[1].getBucketId());
    database.transaction(() -> database.command("sql", "DELETE FROM " + edgeRid[1]));
    assertThat(edgeBucket.count()).isEqualTo(2);

    database.transaction(() -> database.command("sql",
        "RESTORE EDGE " + EDGE_TYPE + " RID " + edgeRid[1] + " FROM " + v1[0] + " TO " + v2[0] + " SET i = 1"));

    assertThat(edgeBucket.count()).isEqualTo(3);
  }

  @Test
  void restoreDocumentUpdatesBucketRecordCount() {
    // #6069: RESTORE DOCUMENT put the record back but never folded the bucket's cached-count delta the way a
    // normal INSERT does, so count(*) kept reporting one fewer than a full scan - and the drift is persisted in
    // statistics.json, so it survives a reopen too. A normal SQL DELETE (unlike the raw bucket.deleteRecord() the
    // other tests in this class use to simulate an out-of-band delete) DOES fold its -1 correctly, so this test
    // isolates RESTORE's own missing +1 rather than conflating it with delete-path bookkeeping.
    final RID[] rid = new RID[3];
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        rid[i] = database.newDocument(DOC_TYPE).set("i", i).save().getIdentity();
    });

    database.transaction(() -> database.command("sql", "DELETE FROM " + rid[1]));
    assertThat(countByQuery()).isEqualTo(2);
    assertThat(countByScan()).isEqualTo(2);

    database.transaction(
        () -> database.command("sql", "RESTORE DOCUMENT " + DOC_TYPE + " RID " + rid[1] + " SET i = 1"));

    assertThat(countByScan()).isEqualTo(3);
    assertThat(countByQuery()).isEqualTo(3);

    reopenDatabase();

    assertThat(countByScan()).isEqualTo(3);
    assertThat(countByQuery()).isEqualTo(3);
  }

  @Test
  void restoreDocumentInTheSameTransactionAsTheDelete() {
    // #6096: DELETE and RESTORE of the same RID inside ONE transaction. The two folds cancel out (-1 then +1) so
    // count(*) still reports 1, but the record itself was written at a bogus page offset and a full scan returned
    // 0 rows. The only live record of the page is the deleted one, so every slot of the page is a hole - the case
    // findContentInsertionOffset() mistook for "a record starting at offset 0".
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument(DOC_TYPE).set("i", 7).save().getIdentity());

    database.transaction(() -> {
      database.command("sql", "DELETE FROM " + rid[0]);
      database.command("sql", "RESTORE DOCUMENT " + DOC_TYPE + " RID " + rid[0] + " SET i = 7");
    });

    assertThat(countByScan()).isEqualTo(1);
    assertThat(countByQuery()).isEqualTo(1);
    database.transaction(() -> assertThat(database.lookupByRID(rid[0], true).asDocument().<Integer>get("i")).isEqualTo(7));

    reopenDatabase();

    assertThat(countByScan()).isEqualTo(1);
    assertThat(countByQuery()).isEqualTo(1);
  }

  @Test
  void restoreDocumentWhenEverySlotOfThePageIsAHole() {
    // #6096, separate-transaction variant of the same page shape: the bucket's only record is deleted and
    // committed, then restored. The record table is all holes, so the restore must start the content right after
    // the page header instead of deriving an offset from a hole.
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument(DOC_TYPE).set("i", 7).save().getIdentity());

    database.transaction(() -> database.command("sql", "DELETE FROM " + rid[0]));
    assertThat(countByScan()).isEqualTo(0);
    assertThat(countByQuery()).isEqualTo(0);

    database.transaction(() -> database.command("sql", "RESTORE DOCUMENT " + DOC_TYPE + " RID " + rid[0] + " SET i = 7"));

    assertThat(countByScan()).isEqualTo(1);
    assertThat(countByQuery()).isEqualTo(1);

    reopenDatabase();

    assertThat(countByScan()).isEqualTo(1);
    assertThat(countByQuery()).isEqualTo(1);
  }

  @Test
  void restoreDocumentReindexesTheRestoredRecord() {
    // #6120: RESTORE put the record back in its bucket but never re-added its index entries, so a query resolved
    // through an index missed it while a full scan returned it - the same "wrong answer indistinguishable from a
    // right one" shape as #6069/#6096, but on every indexed query instead of on a counter.
    final String type = "IndexedLog";
    database.transaction(() -> {
      final DocumentType t = database.getSchema().createDocumentType(type, 1);
      t.createProperty("k", Type.STRING);
      t.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "k");
    });

    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument(type).set("k", "a").save().getIdentity());
    database.transaction(() -> database.command("sql", "DELETE FROM " + rid[0]));
    database.transaction(() -> database.command("sql", "RESTORE DOCUMENT " + type + " RID " + rid[0] + " SET k = 'a'"));

    assertThat(countByIndexedLookup(type)).as("indexed lookup must see the restored record").isEqualTo(1);
    assertThat(countByScan(type)).isEqualTo(1);

    reopenDatabase();

    assertThat(countByIndexedLookup(type)).isEqualTo(1);
    assertThat(countByScan(type)).isEqualTo(1);
  }

  @Test
  void restoreDocumentEnforcesTheUniqueIndexOnTheRestoredKey() {
    // #6120: with the restored record indexed, a restore that would introduce a duplicate on a UNIQUE index is
    // rejected the same way an INSERT of that key is, instead of silently creating a second record carrying a key
    // the index believes belongs to another one.
    final String type = "UniqueLog";
    database.transaction(() -> {
      final DocumentType t = database.getSchema().createDocumentType(type, 1);
      t.createProperty("k", Type.STRING);
      t.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "k");
    });

    final RID[] rid = new RID[1];
    database.transaction(() -> {
      rid[0] = database.newDocument(type).set("k", "a").save().getIdentity();
      database.newDocument(type).set("k", "b").save();
    });

    database.transaction(() -> database.command("sql", "DELETE FROM " + rid[0]));

    // 'b' is still live and owned by another record: restoring the freed slot under that key must be refused.
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("sql", "RESTORE DOCUMENT " + type + " RID " + rid[0] + " SET k = 'b'"))).isInstanceOf(
        DuplicatedKeyException.class);

    // The original key is free, so the very same restore under 'a' must succeed.
    database.transaction(() -> database.command("sql", "RESTORE DOCUMENT " + type + " RID " + rid[0] + " SET k = 'a'"));
    assertThat(countByIndexedLookup(type)).isEqualTo(1);
  }

  @Test
  void restoreVertexReindexesTheRestoredRecord() {
    // #6120 on the RESTORE VERTEX arm, which additionally rewires the surviving edges: the reconnected vertex must
    // also be findable through its own index, not just through a full scan and its edges.
    final String type = "IndexedPerson";
    database.transaction(() -> {
      final VertexType t = database.getSchema().createVertexType(type, 1);
      t.createProperty("name", Type.STRING);
      t.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name");
    });

    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newVertex(type).set("name", "hub").save().getIdentity());
    database.transaction(() -> database.command("sql", "DELETE FROM " + rid[0]));
    database.transaction(() -> database.command("sql", "RESTORE VERTEX " + type + " RID " + rid[0] + " SET name = 'hub'"));

    assertThat(countByIndexedLookup(type, "name", "hub")).isEqualTo(1);
    assertThat(countByScan(type)).isEqualTo(1);
  }

  @Test
  void restoreEdgeReindexesTheRestoredRecord() {
    // #6120 on the RESTORE EDGE arm: an edge property index is maintained exactly like a document one.
    final String type = "IndexedKnows";
    database.transaction(() -> {
      final EdgeType t = database.getSchema().createEdgeType(type, 1);
      t.createProperty("since", Type.STRING);
      t.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "since");
    });

    final RID[] v1 = new RID[1];
    final RID[] v2 = new RID[1];
    final RID[] edgeRid = new RID[1];
    database.transaction(() -> {
      final var a = database.newVertex(VERTEX_TYPE).set("name", "a").save();
      final var b = database.newVertex(VERTEX_TYPE).set("name", "b").save();
      v1[0] = a.getIdentity();
      v2[0] = b.getIdentity();
      edgeRid[0] = a.newEdge(type, b).set("since", "2020").save().getIdentity();
    });

    database.transaction(() -> database.command("sql", "DELETE FROM " + edgeRid[0]));
    database.transaction(() -> database.command("sql",
        "RESTORE EDGE " + type + " RID " + edgeRid[0] + " FROM " + v1[0] + " TO " + v2[0] + " SET since = '2020'"));

    assertThat(countByIndexedLookup(type, "since", "2020")).isEqualTo(1);
  }

  private long countByIndexedLookup(final String type) {
    return countByIndexedLookup(type, "k", "a");
  }

  private long countByIndexedLookup(final String type, final String property, final String value) {
    try (ResultSet rs = database.query("sql", "SELECT FROM " + type + " WHERE " + property + " = '" + value + "'")) {
      return rs.stream().count();
    }
  }

  private long countByScan(final String type) {
    long scanned = 0;
    try (ResultSet rs = database.query("sql", "SELECT FROM " + type)) {
      while (rs.hasNext()) {
        rs.next();
        scanned++;
      }
    }
    return scanned;
  }

  private long countByQuery() {
    try (ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + DOC_TYPE)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private long countByScan() {
    return countByScan(DOC_TYPE);
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
