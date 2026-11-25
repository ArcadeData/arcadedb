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
package com.arcadedb.gremlin;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.security.SecurityDatabaseUser;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.IntFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * A user who may not read a type is refused by a walk of its buckets, and by {@code g.V().hasLabel(X).has(k, p)} answered by an index of
 * {@code k}, with the same refusal for any value, for an equality and for every range alike. The read permission of the type and of its
 * subtypes is asked before the index is opened.
 * <p>
 * Every query is run with a value the denied type holds and with one it does not.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class IndexedFilterReadPermissionTest {
  private static final String PATH = "target/databases/IndexedFilterReadPermissionTest";

  /** A predicate on {@code id}, a value for which the denied type holds a match, and one for which nothing does. */
  private record Shape(String name, IntFunction<P<Integer>> predicate, int present, int absent) {
  }

  private static final List<Shape> SHAPES = List.of(
      new Shape("eq", P::eq, 3, 100),
      new Shape("gt", P::gt, 3, 100),
      new Shape("gte", P::gte, 5, 100),
      new Shape("lt", P::lt, 3, -100),
      new Shape("lte", P::lte, 1, -100));

  private DatabaseFactory factory;
  private Database        database;
  private ArcadeGraph     graph;

  @BeforeEach
  void setUp() {
    // without a security manager the engine asks no file permission at all, whatever user is bound
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.transaction(() -> {
      for (final String name : new String[] { "Open", "Secret", "Base" }) {
        final VertexType type = database.getSchema().createVertexType(name);
        type.createProperty("id", Integer.class);
        type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
      }
      database.getSchema().createVertexType("Sub").addSuperType("Base");

      final EdgeType edge = database.getSchema().createEdgeType("K");
      edge.createProperty("w", Integer.class);
      edge.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "w");
    });
    database.transaction(() -> {
      final MutableVertex first = database.newVertex("Open").set("id", 0).save();
      for (int i = 1; i <= 5; i++) {
        final MutableVertex open = database.newVertex("Open").set("id", i).save();
        first.newEdge("K", open, "w", i);
        database.newVertex("Secret").set("id", i).save();
      }
      // the keys 1 to 5 of the index of Base are held by Base itself, 6 to 10 by its subtype
      for (int i = 1; i <= 5; i++)
        database.newVertex("Base").set("id", i).save();
      for (int i = 6; i <= 10; i++)
        database.newVertex("Sub").set("id", i).save();
    });
    graph = ArcadeGraph.openShared(database);
  }

  @AfterEach
  void tearDown() {
    bindUser(null);
    if (database.isTransactionActive())
      database.rollback();
    graph.close();
    database.drop();
    factory.close();
  }

  @Test
  void aTypeTheUserMayNotReadRefusesTheQueryWhateverTheValueIs() {
    deny("Secret");

    // control: the walk of the type is refused
    assertThat(refused(() -> graph.traversal().V().hasLabel("Secret").toList())).isTrue();

    final List<String> notRefused = new ArrayList<>();
    for (final Shape shape : SHAPES)
      for (final int value : new int[] { shape.present(), shape.absent() })
        if (!refused(() -> graph.traversal().V().hasLabel("Secret").has("id", shape.predicate().apply(value)).toList()))
          notRefused.add(shape.name() + "(" + value + ")");
    assertThat(notRefused).as("a denied type refuses the query for any value").isEmpty();
  }

  @Test
  void aSubTypeTheUserMayNotReadRefusesTheQueryOfItsSuperTypeWhateverTheValueIs() {
    deny("Sub");

    // the index of Base holds the vertices of Sub, and the walk of Base opens the buckets of Sub: a key only Base holds is refused too
    assertThat(refused(() -> graph.traversal().V().hasLabel("Base").toList())).isTrue();
    for (final Shape shape : SHAPES)
      for (final int value : new int[] { shape.present(), shape.absent(), 8 })
        assertThat(refused(() -> graph.traversal().V().hasLabel("Base").has("id", shape.predicate().apply(value)).toList())).as(
            shape.name() + "(" + value + ")").isTrue();
  }

  @Test
  void anEdgeTypeTheUserMayNotReadRefusesTheQueryWhateverTheValueIs() {
    deny("K");

    assertThat(refused(() -> graph.traversal().E().hasLabel("K").toList())).isTrue();
    for (final Shape shape : SHAPES)
      for (final int value : new int[] { shape.present(), shape.absent() })
        assertThat(refused(() -> graph.traversal().E().hasLabel("K").has("w", shape.predicate().apply(value)).toList())).as(
            shape.name() + "(" + value + ")").isTrue();
  }

  @Test
  void aQueryContinuedFromAnEarlierStepIsRefusedToo() {
    deny("Secret");

    for (final int value : new int[] { 3, 100 })
      assertThat(refused(() -> graph.traversal().V().hasLabel("Open").has("id", 1).V().hasLabel("Secret").has("id", value).toList()))
          .as("id " + value).isTrue();
  }

  @Test
  void aTypeTheUserMayReadIsAnsweredWhileAnotherIsDenied() {
    deny("Secret");

    assertThat(graph.traversal().V().hasLabel("Open").has("id", 3).toList()).hasSize(1);
    assertThat(graph.traversal().V().hasLabel("Open").has("id", 100).toList()).isEmpty();
    assertThat(graph.traversal().V().hasLabel("Open").has("id", P.gt(3)).toList()).hasSize(2);
    // the ids 0 to 2: the vertex the edges start from has the id 0
    assertThat(graph.traversal().V().hasLabel("Open").has("id", P.lte(2)).toList()).hasSize(3);
    assertThat(graph.traversal().V().hasLabel("Base").has("id", 3).toList()).hasSize(1);
    assertThat(graph.traversal().V().hasLabel("Base").has("id", 8).toList()).hasSize(1);
    assertThat(graph.traversal().V().hasLabel("Base").has("id", 100).toList()).isEmpty();
    assertThat(graph.traversal().E().hasLabel("K").has("w", 3).toList()).hasSize(1);
    assertThat(graph.traversal().E().hasLabel("K").has("w", 100).toList()).isEmpty();
  }

  @Test
  void aUserWhoMayReadEveryTypeIsAnswered() {
    deny();

    assertThat(graph.traversal().V().hasLabel("Secret").has("id", 3).toList()).hasSize(1);
    assertThat(graph.traversal().V().hasLabel("Secret").has("id", 100).toList()).isEmpty();
    assertThat(graph.traversal().V().hasLabel("Secret").has("id", P.gt(3)).toList()).hasSize(2);
    assertThat(graph.traversal().V().hasLabel("Secret").has("id", P.lt(3)).toList()).hasSize(2);
    assertThat(graph.traversal().V().hasLabel("Base").has("id", 8).toList()).hasSize(1);
  }

  @Test
  void aQueryWithNoBoundUserIsAnswered() {
    // an embedded call has no user bound: nothing is denied
    bindUser(null);

    assertThat(graph.traversal().V().hasLabel("Secret").has("id", 3).toList()).hasSize(1);
    assertThat(graph.traversal().V().hasLabel("Secret").has("id", P.gte(4)).toList()).hasSize(2);
    assertThat(graph.traversal().V().hasLabel("Secret").has("id", 100).toList()).isEmpty();
  }

  private static boolean refused(final Runnable query) {
    Throwable failure = catchThrowable(query::run);
    while (failure != null) {
      if (failure instanceof SecurityException)
        return true;
      failure = failure.getCause();
    }
    return false;
  }

  private void bindUser(final SecurityDatabaseUser user) {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(user);
  }

  /** Denies the read of every record of the types, and grants everything else. */
  private void deny(final String... typeNames) {
    final Set<Integer> deniedBucketIds = new HashSet<>();
    for (final String typeName : typeNames)
      deniedBucketIds.addAll(database.getSchema().getType(typeName).getBucketIds(false));

    bindUser(new SecurityDatabaseUser() {
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
        return true;
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
