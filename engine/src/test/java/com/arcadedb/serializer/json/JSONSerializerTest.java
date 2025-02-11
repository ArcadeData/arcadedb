package com.arcadedb.serializer.json;

import com.arcadedb.TestHelper;
import com.arcadedb.database.JSONSerializer;
import com.arcadedb.database.MutableEmbeddedDocument;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class JSONSerializerTest extends TestHelper {

  private JSONSerializer jsonSerializer;
  private DocumentType   testType;
  private DocumentType   anotherType;

  @BeforeEach
  void setUp() {
    database.transaction(() -> {
      testType = database.getSchema().createDocumentType("TestType");
      testType.createProperty("key1", Type.STRING);
      testType.createProperty("key2", Type.INTEGER);

      anotherType = database.getSchema().createVertexType("AnotherType");
      anotherType.createProperty("embedded", Type.EMBEDDED, testType.getName());
      anotherType.createProperty("list", Type.LIST, Type.STRING.name());
      anotherType.createProperty("map", Type.MAP, Type.STRING.name());

    });
    jsonSerializer = new JSONSerializer(database);
  }

  @Test
  void testComplexDocumentJsonSerialization() {
    database.transaction(() -> {

      MutableVertex document = database.newVertex("AnotherType")
          .set("list", List.of("value1", "value2"))
          .set("map", Map.of("key1", "value1", "key2", "value2"));

      MutableEmbeddedDocument emb = document.newEmbeddedDocument("TestType", "embedded");
      emb.set("key1", "foo");
      emb.set("key2", 11);
      document.save();

      //it uses JsonSerializer underneath
      JSONObject json = document.toJSON(true);

      // check AnotherType
      assertThat(json.getString("@type")).isEqualTo("AnotherType");
      assertThat(json.getString("@rid")).isNotNull();
      assertThat(json.getJSONArray("list")).containsExactly("value1", "value2");
      assertThat(json.getJSONObject("map")).satisfies(map -> {
        assertThat(map.getString("key1")).isEqualTo("value1");
        assertThat(map.getString("key2")).isEqualTo("value2");
      });

      // check embedded TestType
      assertThat(json.getJSONObject("embedded")).satisfies(map -> {
        assertThat(map.getString("@type")).isEqualTo("TestType");
        assertThat(map.getString("key1")).isEqualTo("foo");
        assertThat(map.getInt("key2")).isEqualTo(11);
      });

    });

  }

  @Test
  void testMap2json() {
    Map<String, Object> map = new HashMap<>();
    map.put("key1", "value1");
    map.put("key2", 123);

    JSONObject json = jsonSerializer.map2json(map, testType);

    assertThat(json.getString("key1")).isEqualTo("value1");
    assertThat(json.getInt("key2")).isEqualTo(123);
  }

  @Test
  void testJson2map() {
    JSONObject json = new JSONObject();
    json.put("key1", "value1");
    json.put("key2", 123);

    Map<String, Object> map = jsonSerializer.json2map(json);

    assertThat(map).containsEntry("key1", "value1");
    assertThat(map).containsEntry("key2", 123);
  }

  @Test
  void testConvertToJSONType() {
    Map<String, Object> map = new HashMap<>();
    map.put("key1", "value1");
    map.put("key2", 123);

    Object jsonType = jsonSerializer.map2json(map, testType);

    assertThat(jsonType).isInstanceOf(JSONObject.class);
    JSONObject json = (JSONObject) jsonType;
    assertThat(json.getString("key1")).isEqualTo("value1");
    assertThat(json.getInt("key2")).isEqualTo(123);
  }

  @Test
  void testConvertFromJSONType() {
    JSONObject json = new JSONObject();
    json.put("key1", "value1");
    json.put("key2", 123);

    Object mapType = jsonSerializer.json2map(json);

    assertThat(mapType).isInstanceOf(Map.class);
    Map<String, Object> map = (Map<String, Object>) mapType;
    assertThat(map).containsEntry("key1", "value1");
    assertThat(map).containsEntry("key2", 123);
  }
}
