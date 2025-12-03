package com.arcadedb.serializer.json;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class to verify the fix for JSON empty array serialization issue.
 * Tests the specific case where Object[] arrays (including empty ones)
 * should be properly serialized as JSON arrays instead of string representations.
 */
class JSONObjectArrayFixTest {

  @Test
  void emptyObjectArraySerialization() {
    JSONObject json = new JSONObject();
    Object[] emptyArray = new Object[0];

    // This should not produce a string representation like "[Ljava.lang.Object;@b78a709"
    json.put("emptyArray", emptyArray);

    String jsonString = json.toString();

    // Should contain proper JSON array syntax
    assertThat(jsonString)
        .as("Empty Object[] array should serialize as [] and not as a Java object string")
        .contains("\"emptyArray\":[]")
        .doesNotContain("[Ljava.lang.Object;");
  }

  @Test
  void nonEmptyObjectArraySerialization() {
    JSONObject json = new JSONObject();
    Object[] array = new Object[] { "test", 123, true };

    json.put("testArray", array);

    String jsonString = json.toString();

    // Should contain proper JSON array with values
    assertThat(jsonString)
        .as("Object[] should serialize as a JSON array and not a Java object string")
        .contains("\"testArray\":[")
        .contains("\"test\"", "123", "true")
        .doesNotContain("[Ljava.lang.Object;");
  }

  @Test
  void mixedObjectArraySerialization() {
    JSONObject json = new JSONObject();
    Object[] mixedArray = new Object[] { null, "string", 42, new Object[0] };

    json.put("mixedArray", mixedArray);

    String jsonString = json.toString();

    assertThat(jsonString)
        .as("Mixed Object[] should serialize as a JSON array and not a Java object string")
        .contains("\"mixedArray\":[")
        .contains("null")
        .doesNotContain("[Ljava.lang.Object;");
  }

  @Test
  void objectArrayDeserialization() {
    // Test round-trip: serialize -> parse -> verify
    JSONObject original = new JSONObject();
    Object[] testArray = new Object[] { "hello", 999, false };
    original.put("data", testArray);

    String jsonString = original.toString();

    // Parse it back
    JSONObject parsed = new JSONObject(jsonString);
    JSONArray parsedArray = parsed.getJSONArray("data");

    assertThat(parsedArray.toList()).containsExactly("hello", 999, false);
  }
}
