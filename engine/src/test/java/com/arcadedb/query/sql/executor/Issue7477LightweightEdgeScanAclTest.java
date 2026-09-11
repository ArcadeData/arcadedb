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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #7477, reported as discussion #7473: scanning a LIGHTWEIGHT edge type now reads the vertices instead of
 * the edge type's (empty) bucket, so the per-type check that the bucket scan performed for free has to be performed
 * by the walk - on the edge type the caller named, and on every vertex type the walk opens.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7477LightweightEdgeScanAclTest {
  private static final String PATH = "target/databases/Issue7477LightweightEdgeScanAclTest";

  private DatabaseFactory factory;
  private Database        database;

  @BeforeEach
  void setUp() {
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();

    database = factory.create();

    database.command("sql", "CREATE VERTEX TYPE Work");
    database.command("sql", "CREATE VERTEX TYPE Secret");
    database.command("sql", "CREATE EDGE TYPE Cite LIGHTWEIGHT");

    database.transaction(() -> {
      final RID w0 = database.newVertex("Work").set("id", 0).save().getIdentity();
      final RID w1 = database.newVertex("Work").set("id", 1).save().getIdentity();
      final RID s0 = database.newVertex("Secret").set("id", 2).save().getIdentity();

      database.lookupByRID(w0, true).asVertex().modify().newEdge("Cite", w1);
      database.lookupByRID(s0, true).asVertex().modify().newEdge("Cite", w1);
    });
  }

  @AfterEach
  void tearDown() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
    database.drop();
    factory.close();
  }

  @Test
  void aUserDeniedTheEdgeTypeCannotReachItsEdgesThroughTheVertices() {
    bindUser(Set.of("Cite"));

    final Throwable thrown = catchThrowable(() -> database.query("sql", "SELECT FROM Cite").hasNext());
    assertThat(thrown).as("the walk replaces a bucket scan that was gated: the gate has to move with it")
        .isInstanceOf(SecurityException.class);
  }

  /**
   * A lightweight edge is only visible through the vertices that hold it, so the ones behind a denied vertex type
   * are not the caller's to see. Leaving that type out of the walk - rather than failing the whole statement over an
   * ACL on a type the query never named - keeps the edge type queryable for what the caller may read.
   */
  @Test
  void aDeniedVertexTypeIsLeftOutOfTheWalkRatherThanFailingIt() {
    bindUser(Set.of("Secret"));

    final List<String> pairs = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT FROM Cite")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        pairs.add(r.getEdge().get().getOut() + "->" + r.getEdge().get().getIn());
      }
    }

    assertThat(pairs).hasSize(1);
    assertThat(pairs.getFirst()).startsWith("#1:0->");
  }

  @Test
  void anUnrestrictedUserSeesEveryEdge() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) as c FROM Cite")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(2L);
    }
  }

  private void bindUser(final Set<String> deniedTypes) {
    final Set<Integer> deniedBucketIds = new HashSet<>();
    for (final String typeName : deniedTypes)
      deniedBucketIds.addAll(database.getSchema().getType(typeName).getBucketIds(false));

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
        return !deniedBucketIds.contains(fileId);
      }

      @Override
      public boolean requestAccessOnType(final String typeName, final ACCESS access) {
        return !deniedTypes.contains(typeName);
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
