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
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.InternalBucketNaming;
import com.arcadedb.security.SecurityDatabaseUser;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The update grant of the vertex types a {@link GraphBatch} connects is asked before an edge is buffered, and again before a
 * flush or a drain of the deferred incoming edges writes anything: a user who may create the edge type and the vertices but
 * not update the vertex type leaves no edge record and no edge-list segment behind, as the same edge created one at a time
 * through {@code Vertex.newEdge()} would not.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class GraphBatchEndpointGrantTest {
  private static final String PATH = "target/databases/GraphBatchEndpointGrantTest";

  private static final int EDGES = 40;

  private DatabaseFactory factory;
  private Database        database;
  private RID[]           sources;
  private RID[]           targets;

  @BeforeEach
  void setUp() {
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.command("sql", "CREATE VERTEX TYPE Src");
    database.command("sql", "CREATE VERTEX TYPE Dst");
    database.command("sql", "CREATE EDGE TYPE Link");
    database.command("sql", "CREATE EDGE TYPE OneWay UNIDIRECTIONAL");
    database.command("sql", "CREATE EDGE TYPE Thin LIGHTWEIGHT");

    sources = new RID[EDGES];
    targets = new RID[EDGES];
    database.transaction(() -> {
      for (int i = 0; i < EDGES; i++) {
        sources[i] = database.newVertex("Src").set("id", i).save().getIdentity();
        targets[i] = database.newVertex("Dst").set("id", i).save().getIdentity();
      }
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

  /** The grants the load is held to are the ones the same edge needs when it is created on its own, which refuses it too. */
  @Test
  void edgeCreatedOnItsOwnNeedsTheSameGrant() {
    bindUserRefusingUpdateOn("Src");

    assertThat(catchThrowable(() -> database.transaction(() -> sources[0].asVertex().newEdge("Link", targets[0])))).isInstanceOf(
        SecurityException.class);

    unbindUser();
    assertNothingWritten("Link");
  }

  /** The source vertex type cannot be updated: the load is refused and no edge record or edge-list segment is written. */
  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  void sourceTypeWithoutUpdateGrantLeavesNothingBehind(final boolean parallelFlush) {
    bindUserRefusingUpdateOn("Src");

    assertThat(catchThrowable(() -> load("Link", parallelFlush))).isInstanceOf(SecurityException.class);

    unbindUser();
    assertNothingWritten("Link");
  }

  /** The destination vertex type cannot be updated: its incoming edge lists would be written, so the load is refused. */
  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  void destinationTypeWithoutUpdateGrantLeavesNothingBehind(final boolean parallelFlush) {
    bindUserRefusingUpdateOn("Dst");

    final Throwable thrown = catchThrowable(() -> load("Link", parallelFlush));
    assertThat(thrown).isInstanceOf(SecurityException.class);

    unbindUser();
    assertNothingWritten("Link");
  }

  /** An edge that is not stored as a record is asked the same grants as one that is. */
  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  void lightEdgesAskTheSameGrants(final boolean parallelFlush) {
    bindUserRefusingUpdateOn("Src");
    assertThat(catchThrowable(() -> load("Thin", parallelFlush))).isInstanceOf(SecurityException.class);
    unbindUser();
    assertNothingWritten("Thin");

    bindUserRefusingUpdateOn("Dst");
    assertThat(catchThrowable(() -> load("Thin", parallelFlush))).isInstanceOf(SecurityException.class);
    unbindUser();
    assertNothingWritten("Thin");
  }

  /** The refusal comes from the call that buffers the edge, so nothing is left for a later flush or close to write. */
  @Test
  void refusedEdgeIsNotBuffered() {
    bindUserRefusingUpdateOn("Src");

    try (final GraphBatch batch = GraphBatch.builder(database).withBatchSize(EDGES * 2).build()) {
      assertThat(catchThrowable(() -> batch.newEdge(sources[0], "Link", targets[0]))).isInstanceOf(SecurityException.class);
      assertThat(batch.getBufferedEdgeCount()).isZero();
    }

    unbindUser();
    assertNothingWritten("Link");
  }

  /** A type that keeps no incoming side never updates its destination vertices, whose grant is therefore not asked. */
  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  void unidirectionalTypeDoesNotAskTheDestinationGrant(final boolean parallelFlush) {
    bindUserRefusingUpdateOn("Dst");

    load("OneWay", parallelFlush);

    unbindUser();
    assertThat(count("OneWay")).isEqualTo(EDGES);
    for (int i = 0; i < EDGES; i++)
      assertThat(sources[i].asVertex().countEdges(Vertex.DIRECTION.OUT, "OneWay")).isEqualTo(1);
  }

  /**
   * The user bound to the thread can change between the call that buffers an edge and the flush that writes it: what was
   * buffered is asked again before anything is written.
   */
  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  void flushAsksTheSourceGrantAgainBeforeWriting(final boolean parallelFlush) {
    final GraphBatch batch = GraphBatch.builder(database).withBatchSize(EDGES * 2).withParallelFlush(parallelFlush).build();
    try {
      for (int i = 0; i < EDGES; i++)
        batch.newEdge(sources[i], "Link", targets[i]);

      bindUserRefusingUpdateOn("Src");
      assertThat(catchThrowable(batch::flush)).isInstanceOf(SecurityException.class);
      unbindUser();

      assertNothingWritten("Link");
    } finally {
      unbindUser();
      closeQuietly(batch);
    }
  }

  /** Same for the destination vertices of a flush that drains the deferred incoming edges itself. */
  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  void flushAsksTheDestinationGrantAgainBeforeWriting(final boolean parallelFlush) {
    final GraphBatch batch = GraphBatch.builder(database)
        .withBatchSize(EDGES * 2)
        .withMaxDeferredIncomingEdges(1)
        .withParallelFlush(parallelFlush)
        .build();
    try {
      for (int i = 0; i < EDGES; i++)
        batch.newEdge(sources[i], "Link", targets[i]);

      bindUserRefusingUpdateOn("Dst");
      assertThat(catchThrowable(batch::flush)).isInstanceOf(SecurityException.class);
      unbindUser();

      assertNothingWritten("Link");
    } finally {
      unbindUser();
      closeQuietly(batch);
    }
  }

  /**
   * The incoming side of edges flushed under a grant is connected when the batch closes, by whichever user is bound then: the
   * grant of the destination vertices is asked again before the first incoming edge-list segment is written.
   */
  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  void deferredIncomingEdgesAskTheDestinationGrantAgain(final boolean parallelFlush) {
    final GraphBatch batch = GraphBatch.builder(database).withBatchSize(EDGES * 2).withParallelFlush(parallelFlush).build();
    try {
      for (int i = 0; i < EDGES; i++)
        batch.newEdge(sources[i], "Link", targets[i]);
      batch.flush();
      assertThat(batch.getDeferredIncomingEdgeCount()).isEqualTo(EDGES);

      bindUserRefusingUpdateOn("Dst");
      assertThat(catchThrowable(batch::close)).isInstanceOf(SecurityException.class);
      unbindUser();

      assertThat(edgeListRecords("Dst")).isZero();
    } finally {
      unbindUser();
      closeQuietly(batch);
    }
  }

  /** The grants a load needs are the ones the same edges would need one at a time: with them the whole load goes through. */
  @ParameterizedTest
  @ValueSource(booleans = { false, true })
  void userWithTheUpdateGrantsLoadsEveryEdge(final boolean parallelFlush) {
    bindUserRefusing(SecurityDatabaseUser.ACCESS.DELETE_RECORD, "Src", "Dst", "Link");

    load("Link", parallelFlush);

    unbindUser();
    assertThat(count("Link")).isEqualTo(EDGES);
    for (int i = 0; i < EDGES; i++) {
      assertThat(sources[i].asVertex().countEdges(Vertex.DIRECTION.OUT, "Link")).isEqualTo(1);
      assertThat(targets[i].asVertex().countEdges(Vertex.DIRECTION.IN, "Link")).isEqualTo(1);
    }
  }

  private void load(final String edgeType, final boolean parallelFlush) {
    try (final GraphBatch batch = GraphBatch.builder(database).withBatchSize(EDGES * 2).withParallelFlush(parallelFlush).build()) {
      for (int i = 0; i < EDGES; i++)
        batch.newEdge(sources[i], edgeType, targets[i]);
    }
  }

  private void closeQuietly(final GraphBatch batch) {
    try {
      batch.close();
    } catch (final RuntimeException ignored) {
      // what a refused call left behind is not what the test is about
    }
  }

  private void assertNothingWritten(final String edgeType) {
    assertThat(count(edgeType)).isZero();
    assertThat(edgeListRecords("Src")).isZero();
    assertThat(edgeListRecords("Dst")).isZero();
    for (int i = 0; i < EDGES; i++) {
      assertThat(sources[i].asVertex().countEdges(Vertex.DIRECTION.OUT, edgeType)).isZero();
      assertThat(targets[i].asVertex().countEdges(Vertex.DIRECTION.IN, edgeType)).isZero();
    }
  }

  private long count(final String typeName) {
    return database.countType(typeName, false);
  }

  /** The records held by the edge-list buckets of a vertex type: the segments its vertices link their edges through. */
  private long edgeListRecords(final String vertexTypeName) {
    long records = 0;
    for (final Bucket bucket : database.getSchema().getType(vertexTypeName).getInvolvedBuckets())
      if (InternalBucketNaming.isEdgeListBucketName(bucket.getName()))
        records += ((LocalBucket) bucket).count();
    return records;
  }

  private void unbindUser() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
  }

  private void bindUserRefusingUpdateOn(final String... typeNames) {
    bindUserRefusing(SecurityDatabaseUser.ACCESS.UPDATE_RECORD, typeNames);
  }

  /** A user that resolves permissions as the server does: the grant is keyed on the buckets of the type, edge lists included. */
  private void bindUserRefusing(final SecurityDatabaseUser.ACCESS refused, final String... typeNames) {
    final Set<Integer> gated = new HashSet<>();
    final Set<String> gatedTypes = new HashSet<>();
    for (final String typeName : typeNames) {
      final DocumentType type = database.getSchema().getType(typeName);
      gatedTypes.add(typeName);
      for (final Bucket bucket : type.getInvolvedBuckets())
        gated.add(bucket.getFileId());
    }
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
        return access != refused || !gatedTypes.contains(name);
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
