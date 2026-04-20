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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression coverage for the {@link DatabaseRID} wrap introduced to keep {@link RID#asVertex()} and friends routing to the correct database when multiple
 * databases are open on the same thread.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class DatabaseRIDTest extends TestHelper {

  @Test
  void recordIdentityIsDatabaseBound() {
    final DatabaseInternal database2 = (DatabaseInternal) new DatabaseFactory(getDatabasePath() + "2").create();
    try {
      database.getSchema().createVertexType("V");
      database2.getSchema().createVertexType("V");

      final RID[] holder = new RID[1];
      database.transaction(() -> {
        final MutableVertex v = database.newVertex("V").set("db", 1).save();
        holder[0] = v.getIdentity();
      });

      assertThat(holder[0]).isInstanceOf(DatabaseRID.class);
      assertThat(((DatabaseRID) holder[0]).getBoundDatabase()).isSameAs(database);

      // Poison the thread-local active database by doing work in database2.
      database2.transaction(() -> database2.newVertex("V").set("db", 2).save());

      // asVertex() on the database-bound RID must still resolve against the origin database, independent of the thread-local state.
      final Vertex loaded = holder[0].asVertex();
      assertThat(loaded.<Integer>get("db")).isEqualTo(1);
      assertThat(loaded.getDatabase()).isSameAs(database);
    } finally {
      database2.drop();
    }
  }

  @Test
  void databaseRidIsInterchangeableWithPlainRid() {
    database.getSchema().createVertexType("V");

    final RID[] holder = new RID[1];
    database.transaction(() -> holder[0] = database.newVertex("V").save().getIdentity());

    final RID identity = holder[0];
    final RID plain = new RID(identity.getBucketId(), identity.getPosition());

    assertThat(identity).isInstanceOf(DatabaseRID.class);
    assertThat(identity).isEqualTo(plain);
    assertThat(plain).isEqualTo(identity);
    assertThat(identity.hashCode()).isEqualTo(plain.hashCode());

    // Interchangeable as keys.
    final Set<RID> set = new HashSet<>();
    set.add(identity);
    assertThat(set).contains(plain);

    final Map<RID, String> map = new HashMap<>();
    map.put(plain, "hit");
    assertThat(map.get(identity)).isEqualTo("hit");
  }

  @Test
  void newRidFactoryProducesDatabaseBoundRid() {
    database.getSchema().createVertexType("V");

    final RID[] holder = new RID[1];
    database.transaction(() -> holder[0] = database.newVertex("V").save().getIdentity());

    final RID fromFactory = database.newRID(holder[0].getBucketId(), holder[0].getPosition());
    assertThat(fromFactory).isInstanceOf(DatabaseRID.class);
    assertThat(((DatabaseRID) fromFactory).getBoundDatabase()).isSameAs(database);
    assertThat(fromFactory).isEqualTo(holder[0]);

    final RID fromString = database.newRID(holder[0].toString());
    assertThat(fromString).isInstanceOf(DatabaseRID.class);
    assertThat(fromString).isEqualTo(holder[0]);
  }

  @Test
  void queryResultRidProjectionIsDatabaseBound() {
    final DatabaseInternal database2 = (DatabaseInternal) new DatabaseFactory(getDatabasePath() + "2").create();
    try {
      database.getSchema().createVertexType("V");
      database2.getSchema().createVertexType("V");

      database.transaction(() -> database.newVertex("V").set("db", 1).save());
      database2.transaction(() -> database2.newVertex("V").set("db", 2).save());

      final RID[] holder = new RID[1];
      database.transaction(() -> {
        try (final ResultSet rs = database.query("sql", "SELECT @rid AS id FROM V")) {
          final Result row = rs.next();
          final Object id = row.getProperty("id");
          assertThat(id).isInstanceOf(DatabaseRID.class);
          holder[0] = (RID) id;
        }
      });

      // Switch thread-local to database2, then resolve the stashed RID: must still reach database1's vertex.
      database2.transaction(() -> {
        final Vertex loaded = holder[0].asVertex();
        assertThat(loaded.<Integer>get("db")).isEqualTo(1);
        assertThat(loaded.getDatabase()).isSameAs(database);
      });
    } finally {
      database2.drop();
    }
  }

  /**
   * Regression for: `SELECT FROM T WHERE indexedProp = ?` from an index cursor resolved through the thread-local active database, which could be the wrong
   * one when multiple databases were open on the same thread. The query must always resolve index RIDs through its own (command-context) database.
   */
  @Test
  void indexedSelectResolvesAgainstQueryDatabaseNotThreadLocal() {
    final DatabaseInternal database2 = (DatabaseInternal) new DatabaseFactory(getDatabasePath() + "2").create();
    try {
      // Different schemas (different bucket ids) to make a cross-database lookup fail loudly with SchemaException.
      final VertexType t1 = database.getSchema().createVertexType("T1");
      t1.createProperty("name", Type.STRING);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "T1", new String[] { "name" });

      database2.getSchema().createVertexType("Unrelated");

      database.transaction(() -> database.newVertex("T1").set("name", "brain-a").save());

      // Open a tx on database2 on the same thread so both databases sit in the thread-local context map when the query runs.
      database2.begin();
      try {
        database2.newVertex("Unrelated").save();

        database.transaction(() -> {
          try (final ResultSet rs = database.query("sql", "select from T1 where name = ?", "brain-a")) {
            assertThat(rs.hasNext()).isTrue();
            final Result row = rs.next();
            assertThat(row.toElement().<String>get("name")).isEqualTo("brain-a");
            assertThat(row.toElement().getDatabase()).isSameAs(database);
          }
        });
      } finally {
        database2.rollback();
      }
    } finally {
      database2.drop();
    }
  }

  /**
   * Regression for: a bare {@link RID} must keep resolving against the single active database on the thread, matching pre-26.4.1 user-visible behaviour.
   * Only multi-DB ambiguity or missing context should trigger the throw.
   */
  @Test
  void bareRidResolvesAgainstSingleActiveDatabase() {
    database.getSchema().createVertexType("V");

    final RID[] holder = new RID[1];
    database.transaction(() -> {
      final MutableVertex v = database.newVertex("V").set("flag", "single").save();
      // Strip the DatabaseRID wrapper to simulate user code that constructed a RID via `new RID("#x:y")` instead of database.newRID(...).
      holder[0] = new RID(v.getIdentity().getBucketId(), v.getIdentity().getPosition());
    });

    database.transaction(() -> {
      final Vertex loaded = holder[0].asVertex();
      assertThat(loaded.<String>get("flag")).isEqualTo("single");
    });
  }

  @Test
  void bareRidThrowsWhenNoActiveDatabaseContext() {
    database.getSchema().createVertexType("V");

    final RID[] holder = new RID[1];
    database.transaction(() -> {
      final MutableVertex v = database.newVertex("V").save();
      holder[0] = new RID(v.getIdentity().getBucketId(), v.getIdentity().getPosition());
    });

    // Close the database so no thread-local context is left on this thread. Operating on a bare RID must now fail loudly.
    database.close();
    try {
      assertThatThrownBy(() -> holder[0].asVertex())
          .isInstanceOf(DatabaseOperationException.class)
          .hasMessageContaining("asVertex")
          .hasMessageContaining("DatabaseRID");
    } finally {
      // Re-open for the @AfterEach teardown that TestHelper performs.
      database = (DatabaseInternal) new DatabaseFactory(getDatabasePath()).open();
    }
  }

  @AfterEach
  @BeforeEach
  void cleanupSecondary() {
    FileUtils.deleteRecursively(new File(getDatabasePath() + "2"));
  }
}
