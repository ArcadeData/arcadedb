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
package com.arcadedb.server.gremlin;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.gremlin.io.ArcadeIoRegistry;
import com.arcadedb.server.BaseGraphServerTest;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV2;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV3;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13043: a Gremlin Server traversal whose result carries a vertex with an EMBEDDED document property lost the whole
 * result. The property value handed to TinkerPop is the engine's own embedded document, which the GraphBinary registry had no
 * serializer for ("Serializer for type com.arcadedb.database.ImmutableEmbeddedDocument not found") and which GraphSON walked as a
 * bean, down to the engine's {@code database} field. The embedded document must reach the client as its own properties and the
 * {@code @type} / {@code @cat} metadata, the way the HTTP endpoint answers it, over every serializer the server speaks.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue13043GremlinServerEmbeddedDocumentTest extends AbstractGremlinServerIT {

  enum Format {
    GRAPH_BINARY,
    GRAPH_SON_V3,
    GRAPH_SON_V2
  }

  @ParameterizedTest
  @EnumSource(Format.class)
  void valuesOfAnEmbeddedPropertyIsTheDocumentProperties(final Format format) {
    seed();

    final Cluster cluster = createCluster(format);
    try {
      final Object value = traversal(cluster).V().has("T", "name", "v1").values("emb").next();

      assertThat(value).isInstanceOf(Map.class);
      final Map<?, ?> emb = (Map<?, ?>) value;
      assertThat(emb.get("city")).isEqualTo("Rome");
      assertThat(emb.get("zip")).isEqualTo(100);
      assertThat(emb.get("@type")).isEqualTo("D");
      assertThat(emb.get("@cat")).isEqualTo("d");
      // THE ENGINE'S INTERNAL OBJECT GRAPH NEVER REACHES THE WIRE
      assertThat(emb.containsKey("database")).isFalse();
    } finally {
      cluster.close();
    }
  }

  @ParameterizedTest
  @EnumSource(Format.class)
  void aVertexCarryingAnEmbeddedPropertyIsReturned(final Format format) {
    seed();

    final Cluster cluster = createCluster(format);
    try {
      final List<Vertex> vertices = traversal(cluster).V().hasLabel("T").toList();
      assertThat(vertices).hasSize(3);

      final Vertex v1 = vertices.stream().filter(v -> "v1".equals(v.value("name"))).findFirst().orElseThrow();
      final Object emb = v1.value("emb");
      assertThat(emb).isInstanceOf(Map.class);
      assertThat(((Map<?, ?>) emb).get("city")).isEqualTo("Rome");
      assertThat(((Map<?, ?>) emb).get("@type")).isEqualTo("D");
    } finally {
      cluster.close();
    }
  }

  @ParameterizedTest
  @EnumSource(Format.class)
  void valueMapAndElementMapCarryTheEmbeddedDocument(final Format format) {
    seed();

    final Cluster cluster = createCluster(format);
    try {
      final GraphTraversalSource g = traversal(cluster);

      final Map<Object, Object> valueMap = g.V().has("T", "name", "v1").valueMap("emb").next();
      final Object first = ((List<?>) valueMap.get("emb")).get(0);
      assertThat(first).isInstanceOf(Map.class);
      assertThat(((Map<?, ?>) first).get("city")).isEqualTo("Rome");

      final Map<Object, Object> elementMap = g.V().has("T", "name", "v1").elementMap("emb").next();
      assertThat(elementMap.get("emb")).isInstanceOf(Map.class);
      assertThat(((Map<?, ?>) elementMap.get("emb")).get("city")).isEqualTo("Rome");
    } finally {
      cluster.close();
    }
  }

  @ParameterizedTest
  @EnumSource(Format.class)
  void embeddedDocumentsNestedInAListAMapAndAnEmbeddedDocument(final Format format) {
    seed();

    final Cluster cluster = createCluster(format);
    try {
      final GraphTraversalSource g = traversal(cluster);

      final Object list = g.V().has("T", "name", "v1").values("embList").next();
      assertThat(list).isInstanceOf(List.class);
      assertThat((List<?>) list).hasSize(2);
      assertThat(((Map<?, ?>) ((List<?>) list).get(0)).get("city")).isEqualTo("Milan");
      assertThat(((Map<?, ?>) ((List<?>) list).get(1)).get("city")).isEqualTo("Naples");

      final Object map = g.V().has("T", "name", "v1").values("embMap").next();
      assertThat(map).isInstanceOf(Map.class);
      final Object home = ((Map<?, ?>) map).get("home");
      assertThat(home).isInstanceOf(Map.class);
      assertThat(((Map<?, ?>) home).get("city")).isEqualTo("Turin");
      assertThat(((Map<?, ?>) home).get("@type")).isEqualTo("D");

      final Object outer = g.V().has("T", "name", "v1").values("emb").next();
      final Object inner = ((Map<?, ?>) outer).get("inner");
      assertThat(inner).isInstanceOf(Map.class);
      assertThat(((Map<?, ?>) inner).get("city")).isEqualTo("Paris");
    } finally {
      cluster.close();
    }
  }

  @ParameterizedTest
  @EnumSource(Format.class)
  void anEmbeddedDocumentWithoutPropertiesOrWithANullPropertyIsReturned(final Format format) {
    seed();

    final Cluster cluster = createCluster(format);
    try {
      final Object value = traversal(cluster).V().has("T", "name", "v2").values("emb").next();

      assertThat(value).isInstanceOf(Map.class);
      final Map<?, ?> emb = (Map<?, ?>) value;
      assertThat(emb.get("@type")).isEqualTo("D");
      assertThat(emb.get("@cat")).isEqualTo("d");
      assertThat(emb.get("city")).isNull();
      assertThat(emb.get("note")).isNull();
    } finally {
      cluster.close();
    }
  }

  @ParameterizedTest
  @EnumSource(Format.class)
  void aVertexWithoutTheEmbeddedPropertyStillWorks(final Format format) {
    seed();

    final Cluster cluster = createCluster(format);
    try {
      assertThat(traversal(cluster).V().has("T", "name", "v3").values("name").toList()).containsExactly("v3");
      assertThat(traversal(cluster).V().has("T", "name", "v3").values("emb").toList()).isEmpty();
    } finally {
      cluster.close();
    }
  }

  private void seed() {
    final Database db = getServerDatabase(0, getDatabaseName());
    db.command("sql", "CREATE VERTEX TYPE T");
    db.command("sql", "CREATE DOCUMENT TYPE D");
    db.command("sql", "CREATE PROPERTY T.name STRING");
    db.command("sql", "CREATE PROPERTY D.city STRING");
    db.command("sql", "CREATE PROPERTY D.zip INTEGER");
    db.command("sql", "CREATE PROPERTY D.note STRING");
    db.command("sql", "CREATE PROPERTY D.inner EMBEDDED");
    db.command("sql", "CREATE PROPERTY T.emb EMBEDDED");
    db.command("sql", "CREATE PROPERTY T.embList LIST");
    db.command("sql", "CREATE PROPERTY T.embMap MAP");

    db.transaction(() -> {
      final MutableVertex v1 = db.newVertex("T").set("name", "v1");
      v1.newEmbeddedDocument("D", "emb").set("city", "Rome").set("zip", 100).newEmbeddedDocument("D", "inner").set("city", "Paris");
      v1.newEmbeddedDocument("D", "embList").set("city", "Milan");
      v1.newEmbeddedDocument("D", "embList").set("city", "Naples");
      v1.newEmbeddedDocument("D", "embMap", "home").set("city", "Turin");
      v1.save();

      // NO PROPERTY VALUE AT ALL, AND A NULL ONE
      final MutableVertex v2 = db.newVertex("T").set("name", "v2");
      v2.newEmbeddedDocument("D", "emb").set("note", null);
      v2.save();

      db.newVertex("T").set("name", "v3").save();
    });
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  private Cluster createCluster(final Format format) {
    final MessageSerializer<?> serializer = switch (format) {
      case GRAPH_BINARY ->
          new GraphBinaryMessageSerializerV1(new TypeSerializerRegistry.Builder().addRegistry(new ArcadeIoRegistry()));
      case GRAPH_SON_V3 -> new GraphSONMessageSerializerV3();
      case GRAPH_SON_V2 -> new GraphSONMessageSerializerV2();
    };

    return Cluster.build().enableSsl(false).addContactPoint("localhost").port(getGremlinPort())
        .credentials("root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).serializer(serializer).create();
  }

  private GraphTraversalSource traversal(final Cluster cluster) {
    return AnonymousTraversalSource.traversal().withRemote(DriverRemoteConnection.using(cluster, getDatabaseName()));
  }
}
