/*
 * Copyright 2023 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.arcadedb.gremlin;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.graph.Edge;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.FileUtils;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.math.*;
import java.util.*;

/**
 * Tests execution of gremlin queries as text.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GremlinTest {
  @Test
  public void testGremlin() {
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

      final ResultSet result = graph.gremlin(
              "g.V().as('p').hasLabel('Person').where(__.choose(__.constant(p1), __.constant(p1), __.constant('  cypher.null')).is(neq('  cypher.null')).as('  GENERATED1').select('p').values('age').where(gte('  GENERATED1'))).select('p').project('p.name', 'p.age').by(__.choose(neq('  cypher.null'), __.choose(__.values('name'), __.values('name'), __.constant('  cypher.null')))).by(__.choose(neq('  cypher.null'), __.choose(__.values('age'), __.values('age'), __.constant('  cypher.null')))).order().by(__.select('p.age'), asc)")//
          .setParameter("p1", 25).execute();

      int i = 0;
      int lastAge = 0;
      for (; result.hasNext(); ++i) {
        final Result row = result.next();
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
  public void testGremlinTargetingBuckets() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testgremlin");
    try {

      graph.getDatabase().getSchema().buildVertexType().withName("Movie").withTotalBuckets(2).create();
      graph.getDatabase().getSchema().buildEdgeType().withName("LinkedTo").withTotalBuckets(2).create();

      graph.getDatabase().transaction(() -> {
        Vertex prev = null;
        for (int i = 0; i < 10; i++) {
          Vertex v = graph.addVertex("bucket:Movie_0");
          v.property("name", UUID.randomUUID().toString());
          if (prev != null)
            v.addEdge("bucket:LinkedTo_0", prev);
          prev = v;
        }

        prev = null;
        for (int i = 0; i < 10; i++) {
          Vertex v = graph.addVertex("bucket:Movie_1");
          v.property("name", UUID.randomUUID().toString());
          if (prev != null)
            v.addEdge("bucket:LinkedTo_1", prev);
          prev = v;
        }
      });

      ResultSet result = graph.gremlin("g.V().hasLabel('bucket:Movie_0')").execute();
      int total = 0;
      for (; result.hasNext(); ++total)
        result.next();
      Assertions.assertEquals(10, total);

      result = graph.gremlin("g.E().hasLabel('bucket:LinkedTo_1')").execute();
      total = 0;
      for (; result.hasNext(); ++total)
        result.next();
      Assertions.assertEquals(9, total);

    } finally {
      graph.drop();
    }
  }

  @Test
  public void testGremlinEmbeddedDocument() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testgremlin");
    try {

      graph.getDatabase().getSchema().buildVertexType().withName("Customer").withTotalBuckets(1).create();
      graph.getDatabase().getSchema().buildDocumentType().withName("Address").withTotalBuckets(0).create();

      graph.getDatabase().transaction(() -> {
        for (int i = 0; i < 10; i++) {
          ArcadeVertex v = graph.addVertex("Customer");
          v.property("name", UUID.randomUUID().toString());
          VertexProperty<EmbeddedDocument> address = v.embed("residence", "Address");
          address.property("street", "Via Roma, 10");
          address.property("city", "Rome");
          address.property("country", "Italy");
        }
      });

      ResultSet result = graph.gremlin("g.V().hasLabel('Customer')").execute();
      int total = 0;
      for (; result.hasNext(); ++total) {
        EmbeddedDocument address = result.next().getProperty("residence");
        Assertions.assertEquals("Via Roma, 10", address.getString("street"));
        Assertions.assertEquals("Rome", address.getString("city"));
        Assertions.assertEquals("Italy", address.getString("country"));
      }
      Assertions.assertEquals(10, total);

    } finally {
      graph.drop();
    }
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/500
   */
  @Test
  public void testGremlinIssue500() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testgremlin");
    try {

      graph.addVertex("vl1").property("vp1", 1);
      graph.addVertex("vl2").property("vp1", 1);
      graph.addVertex("vl3").property("vp1", 1);

      ResultSet result = graph.gremlin("g.V().has('vl1','vp1',lt(2)).count()").execute();
      Assertions.assertTrue(result.hasNext());
      Result row = result.next();
      Assertions.assertEquals(1L, (Long) row.getProperty("result"));

      result = graph.gremlin("g.V().has('vl1','vp1',lt(2)).hasLabel('vl1','vl2','vl3').count()").execute();
      Assertions.assertTrue(result.hasNext());
      row = result.next();
      Assertions.assertEquals(1L, (Long) row.getProperty("result"));

    } finally {
      graph.drop();
    }
  }

  @Test
  public void testGremlinLoadByRID() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testgremlin");
    try {

      ArcadeVertex v1 = graph.addVertex("vl1");
      ArcadeVertex v2 = graph.addVertex("vl2");

      ResultSet result = graph.gremlin("g.V('" + v1.id() + "').addE('FriendOf').to( V('" + v2.id() + "') )").execute();
      Assertions.assertTrue(result.hasNext());
      Result row = result.next();

      Assertions.assertTrue(row.isEdge());

      final Edge edge = row.getEdge().get();

      Assertions.assertEquals(v1.id(), edge.getOut().getIdentity().toString());
      Assertions.assertEquals(v2.id(), edge.getIn().getIdentity().toString());

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

      final ResultSet result = database.query("gremlin",
          "g.V().as('p').hasLabel('Person').where(__.choose(__.constant(p1), __.constant(p1), __.constant('  cypher.null')).is(neq('  cypher.null')).as('  GENERATED1').select('p').values('age').where(gte('  GENERATED1'))).select('p').project('p.name', 'p.age').by(__.choose(neq('  cypher.null'), __.choose(__.values('name'), __.values('name'), __.constant('  cypher.null')))).by(__.choose(neq('  cypher.null'), __.choose(__.values('age'), __.values('age'), __.constant('  cypher.null')))).order().by(__.select('p.age'), asc)",
          "p1", 25);

      int i = 0;
      int lastAge = 0;
      for (; result.hasNext(); ++i) {
        final Result row = result.next();
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
  public void testCypherSyntaxError() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testcypher");
    try {

      graph.getDatabase().getSchema().createVertexType("Person");

      try {
        graph.getDatabase().query("gremlin",
            "g.V().as('p').hasLabel22222('Person').where(__.choose(__.constant(p1), __.constant(p1), __.constant('  cypher.null')).is(neq('  cypher.null')).as('  GENERATED1').select('p').values('age').where(gte('  GENERATED1'))).select('p').project('p.name', 'p.age').by(__.choose(neq('  cypher.null'), __.choose(__.values('name'), __.values('name'), __.constant('  cypher.null')))).by(__.choose(neq('  cypher.null'), __.choose(__.values('age'), __.values('age'), __.constant('  cypher.null')))).order().by(__.select('p.age'), asc)",
            "p1", 25);
        Assertions.fail();
      } catch (final CommandParsingException e) {
        // EXPECTED
      }

    } finally {
      graph.drop();
    }
  }

  @Test
  public void testGremlinParse() {
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
  public void testGremlinLists() {
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
  public void testUseIndex() {
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
  public void infinityValue() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testInfinite");
    try {
      final Vertex alice = graph.addVertex("person");
      alice.property("hair", Double.POSITIVE_INFINITY);

      final Vertex bob = graph.addVertex("person");
      bob.property("hair", 500);

      final ArcadeGremlin gremlinReadOnly = graph.gremlin("g.V().has('hair', 500.00)");
      final ResultSet result = gremlinReadOnly.execute();

      Assertions.assertTrue(result.hasNext());

    } finally {
      graph.drop();
    }
  }

  // ISSUE: https://github.com/ArcadeData/arcadedb/issues/690
  @Test
  public void testVertexConstraints() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testConstraints");
    try {
      final VertexType type = graph.getDatabase().getSchema().getOrCreateVertexType("ChipID");
      type.getOrCreateProperty("name", Type.STRING).setMandatory(true).setNotNull(true).setReadonly(true);
      type.getOrCreateProperty("uid", Type.STRING).setMandatory(true).setNotNull(true).setReadonly(true);

      final ArcadeGremlin gremlinReadOnly = graph.gremlin("g.addV('ChipID').property('name', 'a').property('uid', 'b')");
      final ResultSet result = gremlinReadOnly.execute();

      Assertions.assertTrue(result.hasNext());

    } finally {
      graph.drop();
    }
  }

  // ISSUE: https://github.com/ArcadeData/arcadedb/issues/290
  @Test
  public void sort() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testOrder");
    try {
      graph.getDatabase().getSchema().getOrCreateVertexType("Person");
      graph.getDatabase().getSchema().getOrCreateEdgeType("FriendOf");

      final Vertex alice = graph.addVertex(T.label, "Person", "name", "Alice");
      final Vertex bob = graph.addVertex(T.label, "Person", "name", "Bob");
      final Vertex steve = graph.addVertex(T.label, "Person", "name", "Steve");

      alice.addEdge("FriendOf", bob);
      alice.addEdge("FriendOf", steve);
      steve.addEdge("FriendOf", bob);

      final ArcadeGremlin gremlinReadOnly = graph.gremlin("g.V().order().by('name', asc)");
      final ResultSet result = gremlinReadOnly.execute();

      Assertions.assertTrue(result.hasNext());

    } finally {
      graph.drop();
    }
  }

  // ISSUE: https://github.com/ArcadeData/arcadedb/issues/911
  @Test
  public void testLongOverflow() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testLongOverflow");
    try {
      Result value = graph.gremlin("g.inject(Long.MAX_VALUE, 0).sum()").execute().nextIfAvailable();
      Assertions.assertEquals(Long.MAX_VALUE, (long) value.getProperty("result"));

      value = graph.gremlin("g.inject(Long.MAX_VALUE, 1).sum()").execute().nextIfAvailable();
      Assertions.assertEquals(Long.MAX_VALUE + 1, (long) value.getProperty("result"));

      value = graph.gremlin("g.inject(BigInteger.valueOf(Long.MAX_VALUE), 1).sum()").execute().nextIfAvailable();
      Assertions.assertEquals(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.valueOf(1L)), (BigInteger) value.getProperty("result"));
    } finally {
      graph.drop();
    }
  }

  // ISSUE: https://github.com/ArcadeData/arcadedb/issues/912
  @Test
  public void testNumberConversion() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testNumberConversion");
    try {
      Result value = graph.gremlin("g.inject(1).size()").execute().nextIfAvailable();
      Assertions.assertEquals(1, (int) value.getProperty("result"));
    } finally {
      graph.drop();
    }
  }

  @Test
  public void testGroupBy() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testGroupBy");
    try {
      graph.getDatabase().getSchema().getOrCreateVertexType("Person");
      graph.getDatabase().getSchema().getOrCreateEdgeType("FriendOf");

      final Vertex alice = graph.addVertex(T.label, "Person", "name", "Alice");
      final Vertex bob = graph.addVertex(T.label, "Person", "name", "Bob");
      final Vertex steve = graph.addVertex(T.label, "Person", "name", "Steve");

      alice.addEdge("FriendOf", bob);
      alice.addEdge("FriendOf", steve);
      steve.addEdge("FriendOf", bob);

      ResultSet resultSet = graph.gremlin("g.V().hasLabel('Person').group().by('name')").execute();
      Result result = resultSet.nextIfAvailable();
      Assertions.assertNotNull(result.getProperty("Alice"));
      Assertions.assertNotNull(result.getProperty("Bob"));
      Assertions.assertNotNull(result.getProperty("Steve"));
    } finally {
      graph.drop();
    }
  }

  @Test
  public void testTraversalBinding() {
    GlobalConfiguration.GREMLIN_TRAVERSAL_BINDINGS.setValue(
        Map.of(
            "friends",
            (ArcadeTraversalBinder.TraversalSupplier) g -> g.traversal(SocialTraversalSource.class)));

    final ArcadeGraph graph = ArcadeGraph.open("./target/testTraversalBindings");
    try {
      graph.getDatabase().getSchema().getOrCreateVertexType("Person");
      graph.getDatabase().getSchema().getOrCreateEdgeType("FriendOf");

      final Vertex alice = graph.addVertex(T.label, "Person", "name", "Alice");
      final Vertex bob = graph.addVertex(T.label, "Person", "name", "Bob");
      alice.addEdge("FriendOf", bob);

      ResultSet resultSet = graph.gremlin("friends.V().named('Alice').friend('Bob')").execute();
      Result result = resultSet.nextIfAvailable();
      Assertions.assertEquals(result.getProperty("name"), "Bob");

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
