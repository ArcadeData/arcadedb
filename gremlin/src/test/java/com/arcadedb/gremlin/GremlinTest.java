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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.cypher.ArcadeCypher;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.math.*;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests execution of gremlin queries as text.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GremlinTest {
  @Test
  void gremlin() {
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
        assertThat(row.<String>getProperty("p.name")).isEqualTo("Jay");
        assertThat(row.getProperty("p.age") instanceof Number).isTrue();
        assertThat((int) row.getProperty("p.age") > lastAge).isTrue();

        lastAge = row.getProperty("p.age");
      }

      assertThat(i).isEqualTo(25);

    } finally {
      graph.drop();
    }
  }

  @Test
  void gremlinTargetingBuckets() {
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
      assertThat(total).isEqualTo(10);

      result = graph.gremlin("g.E().hasLabel('bucket:LinkedTo_1')").execute();
      total = 0;
      for (; result.hasNext(); ++total)
        result.next();
      assertThat(total).isEqualTo(9);

    } finally {
      graph.drop();
    }
  }

  @Test
  void gremlinCountNotDefinedTypes() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testgremlin");
    try {
      assertThat((Long) graph.gremlin("g.V().hasLabel ( 'foo-label' ).count ()").execute().nextIfAvailable()
          .getProperty("result")).isEqualTo(0);

      assertThat((Long) graph.gremlin("g.E().hasLabel ( 'foo-label' ).count ()").execute().nextIfAvailable()
          .getProperty("result")).isEqualTo(0);

    } finally {
      graph.drop();
    }
  }

  @Test
  void gremlinEmbeddedDocument() {
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
        assertThat(address.getString("street")).isEqualTo("Via Roma, 10");
        assertThat(address.getString("city")).isEqualTo("Rome");
        assertThat(address.getString("country")).isEqualTo("Italy");
      }
      assertThat(total).isEqualTo(10);

    } finally {
      graph.drop();
    }
  }

  /**
   * Issue https://github.com/ArcadeData/arcadedb/issues/500
   */
  @Test
  void gremlinIssue500() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testgremlin");
    try {

      graph.addVertex("vl1").property("vp1", 1);
      graph.addVertex("vl2").property("vp1", 1);
      graph.addVertex("vl3").property("vp1", 1);

      ResultSet result = graph.gremlin("g.V().has('vl1','vp1',lt(2)).count()").execute();
      assertThat(result.hasNext()).isTrue();
      Result row = result.next();
      assertThat((Long) row.getProperty("result")).isEqualTo(1L);

      result = graph.gremlin("g.V().has('vl1','vp1',lt(2)).hasLabel('vl1','vl2','vl3').count()").execute();
      assertThat(result.hasNext()).isTrue();
      row = result.next();
      assertThat((Long) row.getProperty("result")).isEqualTo(1L);

    } finally {
      graph.drop();
    }
  }

  @Test
  void gremlinLoadByRID() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testgremlin");
    try {

      ArcadeVertex v1 = graph.addVertex("vl1");
      ArcadeVertex v2 = graph.addVertex("vl2");

      ResultSet result = graph.gremlin("g.V('" + v1.id() + "').addE('FriendOf').to( V('" + v2.id() + "') )").execute();
      assertThat(result.hasNext()).isTrue();
      Result row = result.next();

      assertThat(row.isEdge()).isTrue();

      final Edge edge = row.getEdge().get();

      assertThat(edge.getOut().getIdentity().toString()).isEqualTo(v1.id());
      assertThat(edge.getIn().getIdentity().toString()).isEqualTo(v2.id());

    } finally {
      graph.drop();
    }
  }

  @Test
  void gremlinFromDatabase() {
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
        assertThat(row.isElement()).isFalse();
        assertThat(row.<String>getProperty("p.name")).isEqualTo("Jay");
        assertThat(row.getProperty("p.age") instanceof Number).isTrue();
        assertThat((int) row.getProperty("p.age") > lastAge).isTrue();

        lastAge = row.getProperty("p.age");
      }

      assertThat(i).isEqualTo(25);

    } finally {
      if (database.isTransactionActive())
        database.rollback();
      database.drop();
    }
  }

  @Test
  void cypherSyntaxError() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testgremlin");
    try {

      graph.getDatabase().getSchema().createVertexType("Person");

      assertThatThrownBy(() -> graph.getDatabase().query("gremlin",
          "g.V().as('p').hasLabel22222('Person').where(__.choose(__.constant(p1), __.constant(p1), __.constant('  cypher.null')).is(neq('  cypher.null')).as('  GENERATED1').select('p').values('age').where(gte('  GENERATED1'))).select('p').project('p.name', 'p.age').by(__.choose(neq('  cypher.null'), __.choose(__.values('name'), __.values('name'), __.constant('  cypher.null')))).by(__.choose(neq('  cypher.null'), __.choose(__.values('age'), __.values('age'), __.constant('  cypher.null')))).order().by(__.select('p.age'), asc)",
          "p1", 25)).isInstanceOf(CommandParsingException.class);

    } finally {
      graph.drop();
    }
  }

  @Test
  void gremlinParse() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testgremlin");
    try {

      final ArcadeGremlin gremlinReadOnly = graph.gremlin(
          "g.V().as('p').hasLabel('Person').where(__.choose(__.constant(25), __.constant(25), __.constant('  cypher.null')).is(neq('  cypher.null')).as('  GENERATED1').select('p').values('age').where(gte('  GENERATED1'))).select('p').project('p.name', 'p.age').by(__.choose(neq('  cypher.null'), __.choose(__.values('name'), __.values('name'), __.constant('  cypher.null')))).by(__.choose(neq('  cypher.null'), __.choose(__.values('age'), __.values('age'), __.constant('  cypher.null')))).order().by(__.select('p.age'), asc)");

      assertThat(gremlinReadOnly.parse().isIdempotent()).isTrue();
      assertThat(gremlinReadOnly.parse().isDDL()).isFalse();

      final ArcadeGremlin gremlinWrite = graph.gremlin("g.V().addV('Person')");

      assertThat(gremlinWrite.parse().isIdempotent()).isFalse();
      assertThat(gremlinWrite.parse().isDDL()).isFalse();

    } finally {
      graph.drop();
    }
  }

  @Test
  void gremlinLists() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testgremlin");
    try {
      final ResultSet result = graph.gremlin("g.addV('Person').property( 'list', ['a', 'b'] )").execute();

      assertThat(result.hasNext()).isTrue();
      final Result v = result.next();
      assertThat(v.isVertex()).isTrue();
      final List list = (List) v.getVertex().get().get("list");
      assertThat(list.size()).isEqualTo(2);
      assertThat(list.contains("a")).isTrue();
      assertThat(list.contains("b")).isTrue();

    } finally {
      graph.drop();
    }
  }

  @Test
  void useIndex() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testgremlin");
    try {
      graph.getDatabase().getSchema().getOrCreateVertexType("Person").getOrCreateProperty("id", Type.STRING)
          .getOrCreateIndex(Schema.INDEX_TYPE.LSM_TREE, true);

      final String uuid = UUID.randomUUID().toString();
      final Vertex v = graph.addVertex("Person");
      v.property("id", uuid);

      final ArcadeGremlin gremlinReadOnly = graph.gremlin("g.V().as('p').hasLabel('Person').has( 'id', eq('" + uuid + "'))");
      final ResultSet result = gremlinReadOnly.execute();

      assertThat(result.hasNext()).isTrue();
    } finally {
      graph.drop();
    }
  }

  @Test
  void labelExists() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testgremlin");
    try {
      graph.traversal().V().hasLabel("Car").forEachRemaining(System.out::println);
    } finally {
      graph.drop();
    }
  }

  // ISSUE: https://github.com/ArcadeData/arcadedb/issues/289
  @Disabled
  @Test
  void infinityValue() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testgremlin");
    try {
      final Vertex alice = graph.addVertex("person");
      alice.property("hair", Double.POSITIVE_INFINITY);

      final Vertex bob = graph.addVertex("person");
      bob.property("hair", 500);

      final ArcadeGremlin gremlinReadOnly = graph.gremlin("g.V().has('hair', 500.00)");
      final ResultSet result = gremlinReadOnly.execute();

      assertThat(result.hasNext()).isTrue();

    } finally {
      graph.drop();
    }
  }

  // ISSUE: https://github.com/ArcadeData/arcadedb/issues/690
  @Test
  void vertexConstraints() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testgremlin");
    try {
      final VertexType type = graph.getDatabase().getSchema().getOrCreateVertexType("ChipID");
      type.getOrCreateProperty("name", Type.STRING).setMandatory(true).setNotNull(true).setReadonly(true);
      type.getOrCreateProperty("uid", Type.STRING).setMandatory(true).setNotNull(true).setReadonly(true);

      final ArcadeGremlin gremlinReadOnly = graph.gremlin("g.addV('ChipID').property('name', 'a').property('uid', 'b')");
      final ResultSet result = gremlinReadOnly.execute();

      assertThat(result.hasNext()).isTrue();

    } finally {
      graph.drop();
    }
  }

  // ISSUE: https://github.com/ArcadeData/arcadedb/issues/290
  @Test
  void sort() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testgremlin");
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

      assertThat(result.hasNext()).isTrue();

    } finally {
      graph.drop();
    }
  }

  // ISSUE: https://github.com/ArcadeData/arcadedb/issues/911
  // This test works only with Groovy engine, from v25.12.1 the default engine is Java
  @Test
  void longOverflow() {
    GlobalConfiguration.GREMLIN_ENGINE.setValue("groovy");
    final ArcadeGraph graph = ArcadeGraph.open("./target/testgremlin");
    try {
      Result value = graph.gremlin("g.inject(Long.MAX_VALUE, 0).sum()").execute().nextIfAvailable();
      assertThat((long) value.getProperty("result")).isEqualTo(Long.MAX_VALUE);

      value = graph.gremlin("g.inject(Long.MAX_VALUE, 1).sum()").execute().nextIfAvailable();
      assertThat((long) value.getProperty("result")).isEqualTo(Long.MAX_VALUE + 1);

      value = graph.gremlin("g.inject(BigInteger.valueOf(Long.MAX_VALUE), 1).sum()").execute().nextIfAvailable();
      assertThat((BigInteger) value.getProperty("result")).isEqualTo(
          BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.valueOf(1L)));
    } finally {
      graph.drop();
      GlobalConfiguration.GREMLIN_ENGINE.reset();
    }
  }

  // ISSUE: https://github.com/ArcadeData/arcadedb/issues/912
  // This test works only with Groovy engine, from v25.12.1 the default engine is Java
  @Test
  void numberConversion() {
    GlobalConfiguration.GREMLIN_ENGINE.setValue("groovy");
    final ArcadeGraph graph = ArcadeGraph.open("./target/testgremlin");
    try {
      Result value = graph.gremlin("g.inject(1).size()").execute().nextIfAvailable();
      assertThat((int) value.getProperty("result")).isEqualTo(1);
    } finally {
      graph.drop();
      GlobalConfiguration.GREMLIN_ENGINE.reset();
    }
  }

  @Test
  void groupBy() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testgremlin");
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
      assertThat(result.<Object>getProperty("Alice")).isNotNull();
      assertThat(result.<Object>getProperty("Bob")).isNotNull();
      assertThat(result.<Object>getProperty("Steve")).isNotNull();
    } finally {
      graph.drop();
    }
  }

  // Issue https://github.com/ArcadeData/arcadedb/issues/1301
  @Test
  void merge() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testgremlin");
    try {
      graph.database.command("sqlscript",//
          "CREATE VERTEX TYPE TestMerge;" + //
              "CREATE PROPERTY TestMerge.id INTEGER;" +//
              "CREATE INDEX ON TestMerge (id) UNIQUE;");

      graph.cypher("CREATE (v:TestMerge{id: 0})").execute();
      graph.cypher("UNWIND range(0, 10) AS id MERGE (v:TestMerge{id: id}) RETURN v").execute();

    } finally {
      graph.drop();
    }
  }

  // https://github.com/ArcadeData/arcadedb/issues/1674
  @Test
  void booleanProperties() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testgremlin");
    try {
      graph.database.command("sqlscript",//
          "CREATE VERTEX TYPE A;" + //
              "CREATE PROPERTY A.b BOOLEAN;");

      graph.gremlin("g.addV('A').property('b', true)").execute().nextIfAvailable();
      graph.gremlin("g.addV('A').property('b', true)").execute().nextIfAvailable();
      graph.gremlin("g.addV('A').property('b', false)").execute().nextIfAvailable();
      graph.gremlin("g.addV('A')").execute().nextIfAvailable();
      assertThat(graph.gremlin("g.V().hasLabel('A')").execute().toVertices().size()).isEqualTo(4);
      assertThat(graph.gremlin("g.V().hasLabel('A').has('b',true)").execute().toVertices().size()).isEqualTo(2);
      assertThat((Long) graph.gremlin("g.V().hasLabel('A').has('b',true).count()").execute().nextIfAvailable()
          .getProperty("result")).isEqualTo(2);

    } finally {
      graph.drop();
    }
  }

  @BeforeEach
  @AfterEach
  void clean() {
    FileUtils.deleteRecursively(new File("./target/testgremlin"));
  }
}
