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
package com.arcadedb.query.sql;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10807: walking a vertex's adjacency instead of scanning the edge type loads the vertex, which a user may read the edges of
 * without being allowed to read. For that user the planner keeps the scan, which answers from the edges alone, rather than turning
 * a query the user may run into a refusal.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10807EndpointWalkNeedsVertexGrantTest {
  private static final String PATH = "target/databases/Issue10807EndpointWalkNeedsVertexGrantTest";
  private static final String WALK = "FETCH EDGES FROM VERTEX";

  private DatabaseFactory factory;
  private Database        database;
  private RID             start;

  @BeforeEach
  void setUp() {
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();

    database = factory.create();
    database.getSchema().createVertexType("V1");
    database.getSchema().createEdgeType("E");
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("V1").set("name", "a").save();
      final MutableVertex b = database.newVertex("V1").set("name", "b").save();
      a.newEdge("E", b).set("qty", 1).save();
      start = a.getIdentity();
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen()) {
      DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
      database.drop();
    }
    factory.close();
  }

  @Test
  void aUserWhoCannotReadTheVertexIsAnsweredByTheScan() {
    for (final String endpoint : new String[] { "out", "@out" })
      assertThat(plan("SELECT FROM E WHERE " + endpoint + " = " + start)).as("a user who may read both walks").contains(WALK);

    bindUserWithoutReadOnVertices();

    for (final String endpoint : new String[] { "out", "@out" }) {
      final String query = "SELECT FROM E WHERE " + endpoint + " = " + start;
      try (final ResultSet rs = database.query("sql", query)) {
        assertThat(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2)).as(query).doesNotContain(WALK);
        assertThat(rs.next().<Integer>getProperty("qty")).as(query).isEqualTo(1);
        assertThat(rs.hasNext()).isFalse();
      }
    }
  }

  private String plan(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  private void bindUserWithoutReadOnVertices() {
    final Set<Integer> vertexFiles = new HashSet<>();
    for (final Bucket bucket : database.getSchema().getType("V1").getBuckets(true))
      vertexFiles.add(bucket.getFileId());

    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "edgeReader";
      }

      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return false;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return !(access == ACCESS.READ_RECORD && vertexFiles.contains(fileId));
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
