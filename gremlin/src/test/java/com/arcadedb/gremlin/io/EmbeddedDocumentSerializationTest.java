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
package com.arcadedb.gremlin.io;

import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.database.MutableEmbeddedDocument;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.gremlin.ArcadeGraph;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryIo;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.CustomTypeSerializer;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONIo;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TypeInfo;
import org.apache.tinkerpop.gremlin.util.ser.NettyBufferFactory;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #13043: an embedded document is the other ArcadeDB-specific type, next to the RID, that a property value carries to the
 * Gremlin Server serializers. The registry has to give it a GraphBinary and a GraphSON form made of the document's own
 * properties and the {@code @type} / {@code @cat} metadata, and never the engine's object graph (the {@code database} field).
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class EmbeddedDocumentSerializationTest {
  private ArcadeGraph graph;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-embedded-document-serialization");
    graph.getDatabase().getSchema().createVertexType("Customer");
    graph.getDatabase().getSchema().getType("Customer").createProperty("history", Type.LIST);
    graph.getDatabase().getSchema().getType("Customer").createProperty("byName", Type.MAP);
    graph.getDatabase().getSchema().buildDocumentType().withName("Address").withTotalBuckets(0).create();

    graph.getDatabase().transaction(() -> {
      final MutableVertex v = graph.getDatabase().newVertex("Customer").set("name", "c1");
      final MutableEmbeddedDocument address = v.newEmbeddedDocument("Address", "residence");
      address.set("city", "Rome").set("zip", 100).set("note", null);
      address.newEmbeddedDocument("Address", "previous").set("city", "Paris");
      v.newEmbeddedDocument("Address", "history").set("city", "Milan");
      v.newEmbeddedDocument("Address", "history").set("city", "Naples");
      v.newEmbeddedDocument("Address", "byName", "home").set("city", "Turin");
      v.save();
    });
  }

  @AfterEach
  void teardown() {
    if (graph != null)
      graph.drop();
  }

  @Test
  void graphBinaryWritesAnImmutableEmbeddedDocumentAsItsProperties() throws IOException {
    final Object value = graphBinaryRoundTrip(storedValue("residence"));

    assertThat(value).isInstanceOf(Map.class);
    final Map<?, ?> map = (Map<?, ?>) value;
    assertThat(map.get("city")).isEqualTo("Rome");
    assertThat(map.get("zip")).isEqualTo(100);
    assertThat(map.get("@type")).isEqualTo("Address");
    assertThat(map.get("@cat")).isEqualTo("d");
    assertThat(map.containsKey("database")).isFalse();
    // THE EMBEDDED DOCUMENT IN THE EMBEDDED DOCUMENT
    assertThat(((Map<?, ?>) map.get("previous")).get("city")).isEqualTo("Paris");
  }

  @Test
  void graphBinaryWritesAMutableEmbeddedDocumentAsItsProperties() throws IOException {
    final MutableVertex v = graph.getDatabase().newVertex("Customer");
    final MutableEmbeddedDocument address = v.newEmbeddedDocument("Address", "residence");
    address.set("city", "Rome");

    final Object value = graphBinaryRoundTrip(address);

    assertThat(value).isInstanceOf(Map.class);
    assertThat(((Map<?, ?>) value).get("city")).isEqualTo("Rome");
    assertThat(((Map<?, ?>) value).get("@type")).isEqualTo("Address");
    assertThat(((Map<?, ?>) value).containsKey("database")).isFalse();
  }

  @Test
  void graphBinaryWritesTheEmbeddedDocumentsOfAListAndAMapOneByOne() throws IOException {
    final Object list = graphBinaryRoundTrip(storedValue("history"));
    assertThat(list).isInstanceOf(List.class);
    assertThat((List<?>) list).hasSize(2);
    assertThat(((Map<?, ?>) ((List<?>) list).get(0)).get("city")).isEqualTo("Milan");
    assertThat(((Map<?, ?>) ((List<?>) list).get(1)).get("city")).isEqualTo("Naples");

    final Object map = graphBinaryRoundTrip(storedValue("byName"));
    assertThat(map).isInstanceOf(Map.class);
    assertThat(((Map<?, ?>) ((Map<?, ?>) map).get("home")).get("city")).isEqualTo("Turin");

    // A NULL BETWEEN TWO DOCUMENTS IN THE SAME LIST
    final List<Object> withNull = new ArrayList<>();
    withNull.add(storedValue("residence"));
    withNull.add(null);
    withNull.add(storedValue("residence"));
    final Object roundTrip = graphBinaryRoundTrip(withNull);
    assertThat((List<?>) roundTrip).hasSize(3);
    assertThat(((List<?>) roundTrip).get(1)).isNull();
    assertThat(((Map<?, ?>) ((List<?>) roundTrip).get(2)).get("city")).isEqualTo("Rome");
  }

  @Test
  @SuppressWarnings("unchecked")
  void graphBinaryValueFormHonoursTheNullableFlag() throws IOException {
    final TypeSerializerRegistry registry = new TypeSerializerRegistry.Builder().addRegistry(new ArcadeIoRegistry()).create();
    final GraphBinaryWriter writer = new GraphBinaryWriter(registry);
    final GraphBinaryReader reader = new GraphBinaryReader(registry);
    final EmbeddedDocument document = storedValue("residence");

    for (final boolean nullable : new boolean[] { true, false }) {
      final Buffer buffer = newBuffer();
      writer.writeValue(document, buffer, nullable);
      // THE READER ANSWERS THE MAP THE DOCUMENT WAS WRITTEN AS, NOT A DOCUMENT
      final Object value = reader.readValue(buffer, (Class<Object>) (Class<?>) document.getClass(), nullable);
      assertThat(value).isInstanceOf(Map.class);
      assertThat(((Map<?, ?>) value).get("city")).isEqualTo("Rome");
    }

    final Buffer buffer = newBuffer();
    writer.writeValue(null, buffer, true);
    assertThat(reader.readValue(buffer, (Class<Object>) (Class<?>) document.getClass(), true)).isNull();

    assertThatThrownBy(() -> new EmbeddedDocumentSerializer().writeValue(null, newBuffer(), writer,
        false)).isInstanceOf(IOException.class);
  }

  @Test
  void graphSonV3TypedWritesAnEmbeddedDocumentAsAGMap() throws IOException {
    final ObjectMapper mapper = GraphSONMapper.build().version(GraphSONVersion.V3_0).addRegistry(new ArcadeIoRegistry()).create()
        .createMapper();

    final String json = mapper.writeValueAsString(storedValue("residence"));

    assertThat(json).contains("\"@type\":\"g:Map\"").contains("Rome").contains("Address");
    assertNoEngineInternals(json);

    final Object read = mapper.readValue(json, Object.class);
    assertThat(read).isInstanceOf(Map.class);
    assertThat(((Map<?, ?>) read).get("city")).isEqualTo("Rome");
    assertThat(((Map<?, ?>) read).get("@type")).isEqualTo("Address");
    assertThat(((Map<?, ?>) ((Map<?, ?>) read).get("previous")).get("city")).isEqualTo("Paris");
  }

  @Test
  void graphSonV3TypedWritesTheEmbeddedDocumentsOfAListAndAMap() throws IOException {
    final ObjectMapper mapper = GraphSONMapper.build().version(GraphSONVersion.V3_0).addRegistry(new ArcadeIoRegistry()).create()
        .createMapper();

    final Object list = mapper.readValue(mapper.writeValueAsString(storedValue("history")), Object.class);
    assertThat((List<?>) list).hasSize(2);
    assertThat(((Map<?, ?>) ((List<?>) list).get(1)).get("city")).isEqualTo("Naples");

    final String mapJson = mapper.writeValueAsString(storedValue("byName"));
    assertNoEngineInternals(mapJson);
    final Object map = mapper.readValue(mapJson, Object.class);
    assertThat(((Map<?, ?>) ((Map<?, ?>) map).get("home")).get("city")).isEqualTo("Turin");
  }

  @Test
  void graphSonV3UntypedWritesAnEmbeddedDocumentAsAPlainObject() throws IOException {
    final ObjectMapper mapper = GraphSONMapper.build().version(GraphSONVersion.V3_0)
        .typeInfo(TypeInfo.NO_TYPES).addRegistry(new ArcadeIoRegistry()).create()
        .createMapper();

    final String json = mapper.writeValueAsString(storedValue("residence"));

    assertThat(json).contains("\"city\":\"Rome\"").contains("\"@type\":\"Address\"");
    assertNoEngineInternals(json);
  }

  @Test
  void graphSonV2WritesAnEmbeddedDocumentAsAPlainObject() throws IOException {
    final ObjectMapper mapper = GraphSONMapper.build().version(GraphSONVersion.V2_0).addRegistry(new ArcadeIoRegistry()).create()
        .createMapper();

    final String json = mapper.writeValueAsString(storedValue("residence"));

    assertThat(json).contains("\"city\":\"Rome\"").contains("Address");
    assertNoEngineInternals(json);
  }

  @Test
  void graphSonV1WritesAnEmbeddedDocumentAsAPlainObject() throws IOException {
    final ObjectMapper mapper = GraphSONMapper.build().version(GraphSONVersion.V1_0).addRegistry(new ArcadeIoRegistry()).create()
        .createMapper();

    final String json = mapper.writeValueAsString(storedValue("residence"));

    assertThat(json).contains("\"city\":\"Rome\"").contains("\"@type\":\"Address\"");
    assertNoEngineInternals(json);
  }

  @Test
  void aLocalTraversalStillSeesAnEmbeddedDocument() {
    // THE CONVERSION IS IN THE SERIALIZERS: THE VALUE THE EMBEDDED PATH HANDS BACK STAYS THE ENGINE'S DOCUMENT
    final Object value = graph.traversal().V().hasLabel("Customer").values("residence").next();
    assertThat(value).isInstanceOf(EmbeddedDocument.class);
    assertThat(((EmbeddedDocument) value).getString("city")).isEqualTo("Rome");
  }

  @Test
  void theRegistryHasAGraphBinaryAndAGraphSonSerializerForTheEmbeddedDocument() {
    final ArcadeIoRegistry registry = new ArcadeIoRegistry();

    assertThat(registry.find(GraphBinaryIo.class,
        CustomTypeSerializer.class).stream()
        .filter(p -> p.getValue0() == EmbeddedDocument.class).count()).isEqualTo(1);
    assertThat(registry.find(GraphSONIo.class,
        SimpleModule.class).stream()
        .filter(p -> p.getValue0() == EmbeddedDocument.class).count()).isEqualTo(1);
  }

  private static void assertNoEngineInternals(final String json) {
    assertThat(json).doesNotContain("database").doesNotContain("indexCompactions").doesNotContain("LocalDatabase");
  }

  /** The value of the property as the traversal gets it: the immutable document (or collection of them) read from the record. */
  @SuppressWarnings("unchecked")
  private <T> T storedValue(final String property) {
    try (final ResultSet resultSet = graph.getDatabase().query("sql", "select from Customer")) {
      final Result result = resultSet.next();
      return (T) result.getProperty(property);
    }
  }

  /**
   * A buffer with no netty type in the test: the gremlin test classes run against the shaded jar, which relocates netty. Wrapping a
   * NIO buffer and rewinding both indexes gives a buffer that is written, then read, from its start.
   */
  private static Buffer newBuffer() {
    final Buffer buffer = new NettyBufferFactory().wrap(ByteBuffer.allocate(64 * 1024));
    buffer.writerIndex(0);
    buffer.readerIndex(0);
    return buffer;
  }

  private Object graphBinaryRoundTrip(final Object value) throws IOException {
    final TypeSerializerRegistry registry = new TypeSerializerRegistry.Builder().addRegistry(new ArcadeIoRegistry()).create();
    final GraphBinaryWriter writer = new GraphBinaryWriter(registry);
    final GraphBinaryReader reader = new GraphBinaryReader(registry);

    final Buffer buffer = newBuffer();
    writer.write(value, buffer);
    return reader.read(buffer);
  }
}
