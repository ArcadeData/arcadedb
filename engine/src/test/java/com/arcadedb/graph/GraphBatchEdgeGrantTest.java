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
 * The create grant of an edge type is asked of every edge a {@link GraphBatch} is handed, before the edge is buffered, and again
 * before a flush writes anything: a user who may read the database but not create the type has nothing of its edges written and
 * leaves the set of property names the database knows as it was.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class GraphBatchEdgeGrantTest {
  private static final String PATH = "target/databases/GraphBatchEdgeGrantTest";

  private static final int EDGES = 50;

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
    database.command("sql", "CREATE EDGE TYPE Guarded");
    database.command("sql", "CREATE EDGE TYPE Open");

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

  /** Every edge carries a property name the database has not seen: a refused batch leaves the names it knows as they were. */
  @Test
  void refusedEdgesLeaveNothingBehind() {
    final int namesKnownBefore = knownPropertyNames();
    bindUserRefusing(SecurityDatabaseUser.ACCESS.CREATE_RECORD, "Guarded");

    final Throwable thrown = catchThrowable(() -> {
      try (final GraphBatch batch = GraphBatch.builder(database).withBatchSize(EDGES * 2).build()) {
        for (int i = 0; i < EDGES; i++)
          batch.newEdge(a, "Guarded", b, "refusedProp" + i, i);
      }
    });
    assertThat(thrown).isInstanceOf(SecurityException.class);

    unbindUser();
    assertThat(knownPropertyNames()).isEqualTo(namesKnownBefore);
    assertThat(count("Guarded")).isZero();
    assertThat(a.asVertex().countEdges(Vertex.DIRECTION.OUT, "Guarded")).isZero();
  }

  /** The edge that fills the buffer flushes it: the refusal comes first, with the same outcome. */
  @Test
  void refusedEdgesOfAFullBufferLeaveNothingBehind() {
    final int namesKnownBefore = knownPropertyNames();
    bindUserRefusing(SecurityDatabaseUser.ACCESS.CREATE_RECORD, "Guarded");

    final Throwable thrown = catchThrowable(() -> {
      try (final GraphBatch batch = GraphBatch.builder(database).withBatchSize(EDGES).build()) {
        for (int i = 0; i < EDGES; i++)
          batch.newEdge(a, "Guarded", b, "fullBufferProp" + i, i);
      }
    });
    assertThat(thrown).isInstanceOf(SecurityException.class);

    unbindUser();
    assertThat(knownPropertyNames()).isEqualTo(namesKnownBefore);
    assertThat(count("Guarded")).isZero();
  }

  /** A batch that mixes a refused type with a granted one: the refused one stays out and so do its property names. */
  @Test
  void refusedTypeOfAMixedBatchLeavesNothingBehind() {
    final int namesKnownBefore = knownPropertyNames();
    bindUserRefusing(SecurityDatabaseUser.ACCESS.CREATE_RECORD, "Guarded");

    try (final GraphBatch batch = GraphBatch.builder(database).withBatchSize(EDGES * 4).build()) {
      for (int i = 0; i < EDGES; i++) {
        batch.newEdge(a, "Open", b, "mixedGranted", i);
        final int n = i;
        assertThat(catchThrowable(() -> batch.newEdge(a, "Guarded", b, "mixedRefused" + n, n))).isInstanceOf(
            SecurityException.class);
      }
    }

    unbindUser();
    assertThat(count("Guarded")).isZero();
    assertThat(count("Open")).isEqualTo(EDGES);
    // the granted type brought its one name, the refused one none of its names
    assertThat(knownPropertyNames()).isEqualTo(namesKnownBefore + 1);
    assertThat(database.getSchema().getDictionary().getIdByName("mixedRefused0", false)).isNegative();
  }

  /**
   * The user bound to the thread can change between the call that buffers an edge and the flush that writes it: what was
   * buffered under a grant is asked again before anything is written, so a refused flush leaves the names the database knows as
   * they were.
   */
  @Test
  void flushAsksTheGrantAgainBeforeWriting() {
    final int namesKnownBefore = knownPropertyNames();

    final GraphBatch batch = GraphBatch.builder(database).withBatchSize(EDGES * 2).build();
    try {
      for (int i = 0; i < EDGES; i++)
        batch.newEdge(a, "Guarded", b, "rebindProp" + i, i);

      bindUserRefusing(SecurityDatabaseUser.ACCESS.CREATE_RECORD, "Guarded");
      assertThat(catchThrowable(batch::flush)).isInstanceOf(SecurityException.class);
      unbindUser();

      assertThat(knownPropertyNames()).isEqualTo(namesKnownBefore);
    } finally {
      unbindUser();
      try {
        batch.close();
      } catch (final RuntimeException ignored) {
        // the buffer a refused flush left behind is not what this test is about
      }
    }
  }

  /** The grant of a type is the grant of that type only: another type loads, and brings the names its edges carry. */
  @Test
  void grantedTypeStillLoadsEdgesWithNewPropertyNames() {
    final int namesKnownBefore = knownPropertyNames();
    bindUserRefusing(SecurityDatabaseUser.ACCESS.CREATE_RECORD, "Guarded");

    try (final GraphBatch batch = GraphBatch.builder(database).withBatchSize(EDGES * 2).build()) {
      for (int i = 0; i < EDGES; i++)
        batch.newEdge(a, "Open", b, "grantedName" + i, i);
    }

    unbindUser();
    assertThat(count("Open")).isEqualTo(EDGES);
    assertThat(a.asVertex().countEdges(Vertex.DIRECTION.OUT, "Open")).isEqualTo(EDGES);
    assertThat(knownPropertyNames()).isEqualTo(namesKnownBefore + EDGES);
  }

  private int knownPropertyNames() {
    return database.getSchema().getDictionary().getDictionaryMap().size();
  }

  private long count(final String typeName) {
    return database.countType(typeName, false);
  }

  private void unbindUser() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
  }

  /** A user that resolves permissions as the server does: the grant is keyed on the buckets of the type only. */
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
