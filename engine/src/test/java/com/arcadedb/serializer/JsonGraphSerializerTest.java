package com.arcadedb.serializer;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.DocumentType;
import com.jayway.jsonpath.JsonPath;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class JsonGraphSerializerTest extends TestHelper {

  private JsonGraphSerializer jsonGraphSerializer;
  private MutableEdge         edge;
  private MutableVertex       vertex1;
  private MutableVertex       vertex2;

  @BeforeEach
  void setUp() {
    jsonGraphSerializer = JsonGraphSerializer.createJsonGraphSerializer();

    database.transaction(() -> {
      DocumentType vertexType = database.getSchema().createVertexType("TestVertexType");
      DocumentType edgeType = database.getSchema().createEdgeType("TestEdgeType");

      vertex1 = database.newVertex("TestVertexType").save();
      vertex2 = database.newVertex("TestVertexType").save();

      edge = vertex1.newEdge("TestEdgeType", vertex2).save();
    });
  }

  @Test
  void testSerializeEdge() {

    String json = jsonGraphSerializer.serializeGraphElement(edge).toString();

    assertThat(JsonPath.<String>read(json, "$.p.@rid")).isNotEmpty();
    assertThat(JsonPath.<String>read(json, "$.p.@type")).isEqualTo("TestEdgeType");
    assertThat(JsonPath.<String>read(json, "$.p.@cat")).isEqualTo("e");
    assertThat(JsonPath.<String>read(json, "$.p.@in")).isNotEmpty();
    assertThat(JsonPath.<String>read(json, "$.p.@out")).isNotEmpty();
    assertThat(JsonPath.<String>read(json, "$.r")).isNotEmpty();
    assertThat(JsonPath.<String>read(json, "$.t")).isEqualTo("TestEdgeType");
    assertThat(JsonPath.<String>read(json, "$.i")).isNotEmpty();
    assertThat(JsonPath.<String>read(json, "$.o")).isNotEmpty();

  }

  @Test
  void testSerializeEdgeWithoutMetadata() {
    jsonGraphSerializer.setIncludeMetadata(false);

    String json = jsonGraphSerializer.serializeGraphElement(edge).toString();

    //nometadata
    assertThat(JsonPath.<Map<?, ?>>read(json, "$.p")).isEmpty();

    assertThat(JsonPath.<String>read(json, "$.r")).isNotEmpty();
    assertThat(JsonPath.<String>read(json, "$.t")).isEqualTo("TestEdgeType");
    assertThat(JsonPath.<String>read(json, "$.i")).isNotEmpty();
    assertThat(JsonPath.<String>read(json, "$.o")).isNotEmpty();

  }

}
