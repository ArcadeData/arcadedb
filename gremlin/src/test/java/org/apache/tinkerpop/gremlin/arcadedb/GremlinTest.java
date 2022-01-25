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
package org.apache.tinkerpop.gremlin.arcadedb;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.QueryParsingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.apache.tinkerpop.gremlin.arcadedb.structure.ArcadeGraph;
import org.apache.tinkerpop.gremlin.arcadedb.structure.ArcadeGremlin;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Tests execution of gremlin queries as text.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GremlinTest {
  @Test
  public void testGremlin() throws ExecutionException, InterruptedException {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testgremlin");
    try {

      graph.getDatabase().getSchema().createVertexType("Movie");
      graph.getDatabase().getSchema().createVertexType("Person");

      graph.getDatabase().transaction(() -> {
        for (int i = 0; i < 50; i++)
          graph.getDatabase().newVertex("Movie").set("name", UUID.randomUUID().toString()).save();

        for (int i = 0; i < 50; i++)
          graph.getDatabase().newVertex("Person").set("name", "Jay").set("age", i).save();
      });

      ResultSet result = graph.gremlin(
              "g.V().as('p').hasLabel('Person').where(__.choose(__.constant(p1), __.constant(p1), __.constant('  cypher.null')).is(neq('  cypher.null')).as('  GENERATED1').select('p').values('age').where(gte('  GENERATED1'))).select('p').project('p.name', 'p.age').by(__.choose(neq('  cypher.null'), __.choose(__.values('name'), __.values('name'), __.constant('  cypher.null')))).by(__.choose(neq('  cypher.null'), __.choose(__.values('age'), __.values('age'), __.constant('  cypher.null')))).order().by(__.select('p.age'), asc)")//
          .setParameter("p1", 25).execute();

      int i = 0;
      int lastAge = 0;
      for (; result.hasNext(); ++i) {
        final Result row = result.next();
        //System.out.println(row);

        Assertions.assertEquals("Jay", row.getProperty("p.name"));
        Assertions.assertTrue(row.getProperty("p.age") instanceof Number);
        Assertions.assertTrue((int) row.getProperty("p.age") > lastAge);

        lastAge = row.getProperty("p.age");
      }

      Assertions.assertEquals(25, i);

    } finally {
      graph.drop();
    }
  }

  @Test
  public void testGremlinFromDatabase() {
    final Database database = new DatabaseFactory("./target/testgremlin").create();
    try {

      database.getSchema().createVertexType("Person");

      database.transaction(() -> {
        for (int i = 0; i < 50; i++)
          database.newVertex("Person").set("name", "Jay").set("age", i).save();
      });

      ResultSet result = database.query("gremlin",
          "g.V().as('p').hasLabel('Person').where(__.choose(__.constant(p1), __.constant(p1), __.constant('  cypher.null')).is(neq('  cypher.null')).as('  GENERATED1').select('p').values('age').where(gte('  GENERATED1'))).select('p').project('p.name', 'p.age').by(__.choose(neq('  cypher.null'), __.choose(__.values('name'), __.values('name'), __.constant('  cypher.null')))).by(__.choose(neq('  cypher.null'), __.choose(__.values('age'), __.values('age'), __.constant('  cypher.null')))).order().by(__.select('p.age'), asc)",
          "p1", 25);

      int i = 0;
      int lastAge = 0;
      for (; result.hasNext(); ++i) {
        final Result row = result.next();
        //System.out.println(row);

        Assertions.assertFalse(row.isElement());
        Assertions.assertEquals("Jay", row.getProperty("p.name"));
        Assertions.assertTrue(row.getProperty("p.age") instanceof Number);
        Assertions.assertTrue((int) row.getProperty("p.age") > lastAge);

        lastAge = row.getProperty("p.age");
      }

      Assertions.assertEquals(25, i);

    } finally {
      if (database.isTransactionActive())
        database.rollback();
      database.drop();
    }
  }

  @Test
  public void testCypherSyntaxError() throws ExecutionException, InterruptedException {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testcypher");
    try {

      graph.getDatabase().getSchema().createVertexType("Person");

      try {
        graph.getDatabase().query("gremlin",
            "g.V().as('p').hasLabel22222('Person').where(__.choose(__.constant(p1), __.constant(p1), __.constant('  cypher.null')).is(neq('  cypher.null')).as('  GENERATED1').select('p').values('age').where(gte('  GENERATED1'))).select('p').project('p.name', 'p.age').by(__.choose(neq('  cypher.null'), __.choose(__.values('name'), __.values('name'), __.constant('  cypher.null')))).by(__.choose(neq('  cypher.null'), __.choose(__.values('age'), __.values('age'), __.constant('  cypher.null')))).order().by(__.select('p.age'), asc)",
            "p1", 25);
        Assertions.fail();
      } catch (QueryParsingException e) {
        // EXPECTED
      }

    } finally {
      graph.drop();
    }
  }

  @Test
  public void testGremlinParse() throws ExecutionException, InterruptedException {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testcypher");
    try {

      final ArcadeGremlin gremlinReadOnly = graph.gremlin(
          "g.V().as('p').hasLabel('Person').where(__.choose(__.constant(25), __.constant(25), __.constant('  cypher.null')).is(neq('  cypher.null')).as('  GENERATED1').select('p').values('age').where(gte('  GENERATED1'))).select('p').project('p.name', 'p.age').by(__.choose(neq('  cypher.null'), __.choose(__.values('name'), __.values('name'), __.constant('  cypher.null')))).by(__.choose(neq('  cypher.null'), __.choose(__.values('age'), __.values('age'), __.constant('  cypher.null')))).order().by(__.select('p.age'), asc)");

      Assertions.assertTrue(gremlinReadOnly.parse().isIdempotent());
      Assertions.assertFalse(gremlinReadOnly.parse().isDDL());

      final ArcadeGremlin gremlinWrite = graph.gremlin("g.V().addV('Person')");

      Assertions.assertFalse(gremlinWrite.parse().isIdempotent());
      Assertions.assertFalse(gremlinWrite.parse().isDDL());

    } finally {
      graph.drop();
    }
  }

  @Test
  public void testGremlinLists() throws ExecutionException, InterruptedException {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testlist");
    try {
      final ResultSet result = graph.gremlin("g.addV('Person').property( 'list', ['a', 'b'] )").execute();

      Assertions.assertTrue(result.hasNext());
      final Result v = result.next();
      Assertions.assertTrue(v.isVertex());
      final List list = (List) v.getVertex().get().get("list");
      Assertions.assertEquals(2, list.size());
      Assertions.assertTrue(list.contains("a"));
      Assertions.assertTrue(list.contains("b"));

    } finally {
      graph.drop();
    }
  }

  @Test
  public void testUseIndex() throws ExecutionException, InterruptedException {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testcypher");
    try {
      graph.getDatabase().getSchema().getOrCreateVertexType("Person").getOrCreateProperty("id", Type.STRING).getOrCreateIndex(Schema.INDEX_TYPE.LSM_TREE, true);

      final String uuid = UUID.randomUUID().toString();
      final Vertex v = graph.addVertex("Person");
      v.property("id", uuid);

      final ArcadeGremlin gremlinReadOnly = graph.gremlin("g.V().as('p').hasLabel('Person').has( 'id', eq('" + uuid + "'))");
      final ResultSet result = gremlinReadOnly.execute();

      Assertions.assertTrue(result.hasNext());
    } finally {
      graph.drop();
    }
  }

  @Test
  public void labelExists() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testLabel");
    try {
      graph.traversal().V().hasLabel("Car").forEachRemaining(System.out::println);
    } finally {
      graph.drop();
    }
  }

  // ISSUE: https://github.com/ArcadeData/arcadedb/issues/289
  @Disabled
  @Test
  public void infinityValue() throws ExecutionException, InterruptedException {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testInfinite");
    try {
      Vertex alice = graph.addVertex("person");
      alice.property("hair", Double.POSITIVE_INFINITY);

      Vertex bob = graph.addVertex("person");
      bob.property("hair", 500);

      final ArcadeGremlin gremlinReadOnly = graph.gremlin("g.V().has('hair', 500.00)");
      final ResultSet result = gremlinReadOnly.execute();

      Assertions.assertTrue(result.hasNext());

    } finally {
      graph.drop();
    }
  }

  // ISSUE: https://github.com/ArcadeData/arcadedb/issues/290
  @Test
  public void sort() throws ExecutionException, InterruptedException {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testOrder");
    try {
      graph.getDatabase().getSchema().getOrCreateVertexType("Person");
      graph.getDatabase().getSchema().getOrCreateEdgeType("FriendOf");

      Vertex alice = graph.addVertex("label", "Person", "name", "Alice");
      Vertex bob = graph.addVertex("label", "Person", "name", "Bob");
      Vertex steve = graph.addVertex("label", "Person", "name", "Steve");

      alice.addEdge("FriendOf", bob);
      alice.addEdge("FriendOf", steve);
      steve.addEdge("FriendOf", bob);

      final ArcadeGremlin gremlinReadOnly = graph.gremlin("g.E().order().by('name', asc)");
      final ResultSet result = gremlinReadOnly.execute();

      Assertions.assertTrue(result.hasNext());

    } finally {
      graph.drop();
    }
  }

  @BeforeEach
  @AfterEach
  public void clean() {
    FileUtils.deleteRecursively(new File("./target/testgremlin"));
  }
}
