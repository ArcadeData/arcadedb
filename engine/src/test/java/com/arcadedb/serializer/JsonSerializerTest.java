package com.arcadedb.serializer;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.jayway.jsonpath.JsonPath;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JsonSerializerTest extends TestHelper {

  private JsonSerializer jsonSerializer;

  @BeforeEach
  void setUp() {
    jsonSerializer = JsonSerializer.createJsonSerializer();
  }

  @Test
  void testSerializeDocument() {
    database.transaction(() -> {
      DocumentType type = database.getSchema().createDocumentType("TestType");
      type.createProperty("key1", Type.STRING);
      type.createProperty("key2", Type.INTEGER);

      MutableDocument document = database.newDocument("TestType")
          .set("key1", "value1")
          .set("key2", 123)
          .save();

      String json = jsonSerializer.serializeDocument(document).toString();

      assertThat(JsonPath.<String>read(json, "$.@type")).isEqualTo("TestType");
      assertThat(JsonPath.<String>read(json, "$.key1")).isEqualTo("value1");
      assertThat(JsonPath.<Integer>read(json, "$.key2")).isEqualTo(123);
    });
  }

  @Test
  void testSerializeVertex() {
    database.transaction(() -> {
      DocumentType type = database.getSchema().createVertexType("TestVertexType");
      type.createProperty("key1", Type.STRING);
      type.createProperty("key2", Type.INTEGER);

      MutableVertex vertex = database.newVertex("TestVertexType")
          .set("key1", "value1")
          .set("key2", 123)
          .save();

      String json = jsonSerializer.serializeDocument(vertex).toString();

      assertThat(JsonPath.<String>read(json, "$.@type")).isEqualTo("TestVertexType");
      assertThat(JsonPath.<String>read(json, "$.key1")).isEqualTo("value1");
      assertThat(JsonPath.<Integer>read(json, "$.key2")).isEqualTo(123);
    });
  }

  @Test
  void testSerializeEdge() {
    database.transaction(() -> {
      DocumentType vertexType = database.getSchema().createVertexType("TestVertexType");
      DocumentType edgeType = database.getSchema().createEdgeType("TestEdgeType");

      MutableVertex vertex1 = database.newVertex("TestVertexType").save();
      MutableVertex vertex2 = database.newVertex("TestVertexType").save();

      MutableEdge edge = vertex1.newEdge("TestEdgeType", vertex2, true).save();

      String json = jsonSerializer.serializeDocument(edge).toString();

      assertThat(JsonPath.<String>read(json, "$.@type")).isEqualTo("TestEdgeType");
      assertThat(JsonPath.<String>read(json, "$.@in")).isEqualTo(vertex2.getIdentity().toString());
      assertThat(JsonPath.<String>read(json, "$.@out")).isEqualTo(vertex1.getIdentity().toString());
    });
  }
}
