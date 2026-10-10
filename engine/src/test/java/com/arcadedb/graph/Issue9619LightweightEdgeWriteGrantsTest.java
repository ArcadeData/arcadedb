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
package com.arcadedb.graph;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #9619: a LIGHTWEIGHT edge allocates no record, so {@code LocalBucket.createRecord}/{@code deleteRecord} - where
 * {@code CREATE_RECORD} and {@code DELETE_RECORD} are enforced for every other record - never run for it, and both grants
 * were inert on such a type. The user below resolves permissions exactly as the server does: the grant map is keyed on the
 * type's INVOLVED buckets only, so the vertex type stays fully writable and only the edge type is refused.
 */
class Issue9619LightweightEdgeWriteGrantsTest {
  private static final String PATH = "target/databases/Issue9619LightweightEdgeWriteGrantsTest";

  private DatabaseFactory factory;
  private Database        database;
  private RID             a;
  private RID             b;

  @BeforeEach
  void setUp() {
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE EDGE TYPE LW LIGHTWEIGHT");
    database.command("sql", "CREATE EDGE TYPE HE");

    database.transaction(() -> {
      a = database.newVertex("V").set("id", "a").save().getIdentity();
      b = database.newVertex("V").set("id", "b").save().getIdentity();
    });
  }

  @AfterEach
  void tearDown() {
    unbindUser();
    if (database.isTransactionActive())
      database.rollback();
    database.drop();
    factory.close();
  }

  // ---------------------------------------------------------------- DELETE

  @Test
  void sqlDeleteOfALightweightEdgeIsRefused() {
    connectLW();
    bindUserRefusing(SecurityDatabaseUser.ACCESS.DELETE_RECORD, "LW");

    database.begin();
    assertThat(catchThrowable(() -> database.command("sql", "DELETE FROM LW"))).isInstanceOf(SecurityException.class);
    database.commit();

    unbindUser();
    assertLWConnected(1);
  }

  @Test
  void apiDeleteOfALightweightEdgeIsRefused() {
    connectLW();
    bindUserRefusing(SecurityDatabaseUser.ACCESS.DELETE_RECORD, "LW");

    database.begin();
    final Edge edge = a.asVertex().getEdges(Vertex.DIRECTION.OUT, "LW").iterator().next();
    assertThat(edge).isInstanceOf(LightEdge.class);
    assertThat(catchThrowable(edge::delete)).isInstanceOf(SecurityException.class);
    database.commit();

    unbindUser();
    assertLWConnected(1);
  }

  @Test
  void cypherDeleteOfALightweightEdgeIsRefused() {
    connectLW();
    bindUserRefusing(SecurityDatabaseUser.ACCESS.DELETE_RECORD, "LW");

    database.begin();
    assertThat(catchThrowable(() -> database.command("opencypher", "MATCH (:V)-[r:LW]->(:V) DELETE r"))).isInstanceOf(
        SecurityException.class);
    if (database.isTransactionActive())
      database.rollback();

    unbindUser();
    assertLWConnected(1);
  }

  /** The cascade from a vertex delete reaches the lightweight edge the same way it reaches a record-backed one. */
  @Test
  void vertexDeleteCascadingToARefusedLightweightEdgeIsRefused() {
    connectLW();
    bindUserRefusing(SecurityDatabaseUser.ACCESS.DELETE_RECORD, "LW");

    database.begin();
    assertThat(catchThrowable(() -> database.deleteRecord(a.asVertex()))).isInstanceOf(SecurityException.class);
    if (database.isTransactionActive())
      database.rollback();

    unbindUser();
    assertThat(database.countType("V", false)).isEqualTo(2);
    assertLWConnected(1);
  }

  /** The record-backed control in the same shape, to pin that both edge kinds now answer identically. */
  @Test
  void sqlDeleteOfARecordBackedEdgeIsRefusedToo() {
    database.transaction(() -> a.asVertex().newEdge("HE", b));
    bindUserRefusing(SecurityDatabaseUser.ACCESS.DELETE_RECORD, "HE");

    database.begin();
    assertThat(catchThrowable(() -> database.command("sql", "DELETE FROM HE"))).isInstanceOf(SecurityException.class);
    database.commit();

    unbindUser();
    assertThat(database.countType("HE", false)).isEqualTo(1);
  }

  @Test
  void grantedLightweightDeleteStillWorks() {
    connectLW();
    // refused on another type only: LW stays deletable
    bindUserRefusing(SecurityDatabaseUser.ACCESS.DELETE_RECORD, "HE");

    database.transaction(() -> database.command("sql", "DELETE FROM LW"));

    unbindUser();
    assertLWConnected(0);
  }

  // ---------------------------------------------------------------- CREATE

  @Test
  void sqlCreateOfALightweightEdgeIsRefused() {
    bindUserRefusing(SecurityDatabaseUser.ACCESS.CREATE_RECORD, "LW");

    database.begin();
    assertThat(catchThrowable(() -> database.command("sql", "CREATE EDGE LW FROM " + a + " TO " + b))).isInstanceOf(
        SecurityException.class);
    database.commit();

    unbindUser();
    assertLWConnected(0);
  }

  @Test
  void apiNewEdgeOnALightweightTypeIsRefused() {
    bindUserRefusing(SecurityDatabaseUser.ACCESS.CREATE_RECORD, "LW");

    database.begin();
    assertThat(catchThrowable(() -> a.asVertex().modify().newEdge("LW", b))).isInstanceOf(SecurityException.class);
    database.commit();

    unbindUser();
    assertLWConnected(0);
  }

  /** The deprecated per-call light edge, on a type that does not declare LIGHTWEIGHT: still gated by the type's grant. */
  @Test
  @SuppressWarnings("deprecation")
  void deprecatedNewLightEdgeIsRefused() {
    bindUserRefusing(SecurityDatabaseUser.ACCESS.CREATE_RECORD, "HE");

    database.begin();
    assertThat(catchThrowable(() -> a.asVertex().modify().newLightEdge("HE", b))).isInstanceOf(SecurityException.class);
    database.commit();

    unbindUser();
    assertThat(a.asVertex().countEdges(Vertex.DIRECTION.OUT, "HE")).isZero();
  }

  @Test
  void cypherCreateOfALightweightEdgeIsRefused() {
    bindUserRefusing(SecurityDatabaseUser.ACCESS.CREATE_RECORD, "LW");

    database.begin();
    assertThat(catchThrowable(() -> database.command("opencypher",
        "MATCH (x:V {id:'a'}), (y:V {id:'b'}) CREATE (x)-[:LW]->(y)"))).isInstanceOf(SecurityException.class);
    if (database.isTransactionActive())
      database.rollback();

    unbindUser();
    assertLWConnected(0);
  }

  @Test
  void graphBatchLightweightEdgeIsRefused() {
    bindUserRefusing(SecurityDatabaseUser.ACCESS.CREATE_RECORD, "LW");

    try (final GraphBatch batch = GraphBatch.builder(database).withBatchSize(10).build()) {
      assertThat(catchThrowable(() -> batch.newEdge(a, "LW", b))).isInstanceOf(SecurityException.class);
    }

    unbindUser();
    assertLWConnected(0);
  }

  /** A grant checked for one user must not carry over to the next user bound to the same batch's thread. */
  @Test
  void graphBatchRechecksTheGrantWhenTheUserChanges() {
    try (final GraphBatch batch = GraphBatch.builder(database).withBatchSize(10).build()) {
      batch.newEdge(a, "LW", b);

      bindUserRefusing(SecurityDatabaseUser.ACCESS.CREATE_RECORD, "LW");
      assertThat(catchThrowable(() -> batch.newEdge(b, "LW", a))).isInstanceOf(SecurityException.class);
      unbindUser();
    }

    assertThat(countLW()).isEqualTo(1);
    assertThat(a.asVertex().countEdges(Vertex.DIRECTION.OUT, "LW")).isEqualTo(1);
    assertThat(b.asVertex().countEdges(Vertex.DIRECTION.OUT, "LW")).isZero();
  }

  @Test
  void grantedLightweightCreateStillWorks() {
    bindUserRefusing(SecurityDatabaseUser.ACCESS.CREATE_RECORD, "HE");

    database.transaction(() -> database.command("sql", "CREATE EDGE LW FROM " + a + " TO " + b));
    try (final GraphBatch batch = GraphBatch.builder(database).withBatchSize(10).build()) {
      batch.newEdge(b, "LW", a);
    }

    unbindUser();
    assertThat(countLW()).isEqualTo(2);
  }

  // ---------------------------------------------------------------- MULTI-BUCKET

  /**
   * A lightweight type with several buckets, created after an indexed type so that index files sit between bucket files
   * and the type's buckets are not the dense low ids of a fresh database. Pins the two assumptions the checks rest on: the
   * RID's bucket id is the file id the grant is keyed on, and the type's first bucket answers for all of them.
   */
  @Test
  void multiBucketLightweightTypeIsGatedOnCreateAndDelete() {
    database.command("sql", "CREATE VERTEX TYPE Indexed");
    database.command("sql", "CREATE PROPERTY Indexed.k STRING");
    database.command("sql", "CREATE INDEX ON Indexed (k) UNIQUE");
    database.command("sql", "CREATE EDGE TYPE LW4 LIGHTWEIGHT BUCKETS 4");

    final DocumentType lw4 = database.getSchema().getType("LW4");
    assertThat(lw4.getBuckets(false)).hasSize(4);
    for (final Bucket bucket : lw4.getBuckets(false))
      assertThat(database.getSchema().getBucketById(bucket.getFileId()).getFileId()).isEqualTo(bucket.getFileId());

    bindUserRefusing(SecurityDatabaseUser.ACCESS.CREATE_RECORD, "LW4");
    database.begin();
    assertThat(catchThrowable(() -> database.command("sql", "CREATE EDGE LW4 FROM " + a + " TO " + b))).isInstanceOf(
        SecurityException.class);
    database.commit();
    try (final GraphBatch batch = GraphBatch.builder(database).withBatchSize(10).build()) {
      assertThat(catchThrowable(() -> batch.newEdge(a, "LW4", b))).isInstanceOf(SecurityException.class);
    }
    unbindUser();
    assertThat(a.asVertex().countEdges(Vertex.DIRECTION.OUT, "LW4")).isZero();

    database.transaction(() -> database.command("sql", "CREATE EDGE LW4 FROM " + a + " TO " + b));
    final Edge edge = a.asVertex().getEdges(Vertex.DIRECTION.OUT, "LW4").iterator().next();
    assertThat(edge).isInstanceOf(LightEdge.class);
    assertThat(edge.getIdentity().getBucketId()).isEqualTo(lw4.getFirstBucketId());

    bindUserRefusing(SecurityDatabaseUser.ACCESS.DELETE_RECORD, "LW4");
    database.begin();
    assertThat(catchThrowable(() -> database.command("sql", "DELETE FROM LW4"))).isInstanceOf(SecurityException.class);
    database.commit();
    unbindUser();
    assertThat(a.asVertex().countEdges(Vertex.DIRECTION.OUT, "LW4")).isEqualTo(1);
  }

  // ---------------------------------------------------------------- helpers

  private void connectLW() {
    database.transaction(() -> database.command("sql", "CREATE EDGE LW FROM " + a + " TO " + b));
    assertLWConnected(1);
  }

  private void assertLWConnected(final long expected) {
    assertThat(countLW()).isEqualTo(expected);
    assertThat(a.asVertex().countEdges(Vertex.DIRECTION.OUT, "LW")).isEqualTo(expected);
    assertThat(b.asVertex().countEdges(Vertex.DIRECTION.IN, "LW")).isEqualTo(expected);
  }

  private long countLW() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM LW")) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private void unbindUser() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
  }

  /** Same resolution as ServerSecurityDatabaseUser: only the type's involved bucket ids are listed, the rest is default-allow. */
  private void bindUserRefusing(final SecurityDatabaseUser.ACCESS refused, final String typeName) {
    final DocumentType type = database.getSchema().getType(typeName);
    final Set<Integer> gated = new HashSet<>();
    for (final Bucket bucket : type.getInvolvedBuckets())
      gated.add(bucket.getFileId());
    assertThat(gated).isNotEmpty();

    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "restricted";
      }

      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return access != refused || !gated.contains(fileId);
      }

      @Override
      public boolean requestAccessOnType(final String name, final ACCESS access) {
        return access != refused || !typeName.equals(name);
      }

      @Override
      public long getResultSetLimit() {
        return -1L;
      }

      @Override
      public long getReadTimeout() {
        return -1L;
      }
    });
  }
}
