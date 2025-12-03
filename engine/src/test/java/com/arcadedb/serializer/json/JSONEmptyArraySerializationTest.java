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
package com.arcadedb.serializer.json;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.TreeSet;
import java.util.Vector;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test JSON serialization bug with empty arrays from issue #2497.
 * <p>
 * This test reproduces the NoSuchElementException that occurs when serializing
 * documents containing empty arrays via JSON.
 *
 *
 */
class JSONEmptyArraySerializationTest extends TestHelper {

  @Test
  void jsonObjectWithEmptyArrayShouldNotThrow() {
    // Test direct JSONObject creation with empty array
    JSONObject json = new JSONObject();

    // These should all work without throwing exceptions
    json.put("emptyList", new ArrayList<>());
    json.put("emptyArray", new Object[0]);
    json.put("emptyStringArray", new String[0]);
    json.put("emptySet", new HashSet<>());

    // Verify the JSON object was created successfully
    assertThat(json.has("emptyList")).isTrue();
    assertThat(json.has("emptyArray")).isTrue();
    assertThat(json.has("emptyStringArray")).isTrue();
    assertThat(json.has("emptySet")).isTrue();

    // Verify arrays are empty
    assertThat(json.getJSONArray("emptyList").length()).isEqualTo(0);
    assertThat(json.getJSONArray("emptyArray").length()).isEqualTo(0);
    assertThat(json.getJSONArray("emptyStringArray").length()).isEqualTo(0);
    assertThat(json.getJSONArray("emptySet").length()).isEqualTo(0);

    // Verify serialization works
    String serialized = json.toString();
    assertThat(serialized).isNotNull();
    assertThat(serialized).contains("\"emptyList\":[]");
    assertThat(serialized).contains("\"emptyArray\":[]");
    assertThat(serialized).contains("\"emptyStringArray\":[]");
    assertThat(serialized).contains("\"emptySet\":[]");
  }

  @Test
  void jsonObjectFromMapWithEmptyCollections() {
    // Test creating JSONObject from Map containing empty collections
    Map<String, Object> data = new HashMap<>();
    data.put("attributes", new ArrayList<>());
    data.put("tags", new HashSet<>());
    data.put("values", new Object[0]);
    data.put("items", Collections.emptyList());

    // This should not throw an exception
    JSONObject json = new JSONObject(data);

    // Verify the object was created correctly
    assertThat(json.has("attributes")).isTrue();
    assertThat(json.has("tags")).isTrue();
    assertThat(json.has("values")).isTrue();
    assertThat(json.has("items")).isTrue();

    // Verify arrays are empty
    assertThat(json.getJSONArray("attributes").length()).isEqualTo(0);
    assertThat(json.getJSONArray("tags").length()).isEqualTo(0);
    assertThat(json.getJSONArray("values").length()).isEqualTo(0);
    assertThat(json.getJSONArray("items").length()).isEqualTo(0);

    // Verify serialization works
    String serialized = json.toString();
    assertThat(serialized).isNotNull();
    assertThat(serialized).contains("[]");
  }

  @Test
  void jsonArrayWithEmptyNestedArrays() {
    // Test JSONArray containing empty arrays
    JSONArray outerArray = new JSONArray();
    outerArray.put(new ArrayList<>());
    outerArray.put(new Object[0]);
    outerArray.put(Collections.emptySet());

    // Verify the array was created correctly
    assertThat(outerArray.length()).isEqualTo(3);

    // Verify nested arrays are empty
    assertThat(outerArray.getJSONArray(0).length()).isEqualTo(0);
    assertThat(outerArray.getJSONArray(1).length()).isEqualTo(0);
    assertThat(outerArray.getJSONArray(2).length()).isEqualTo(0);

    // Verify serialization works
    String serialized = outerArray.toString();
    assertThat(serialized).isNotNull();
    assertThat(serialized).isEqualTo("[[],[],[]]");
  }

  @Test
  void mixedContentWithEmptyArrays() {
    // Test object with mix of empty and non-empty collections
    JSONObject json = new JSONObject();
    json.put("nonEmptyArray", Arrays.asList(1, 2, 3));
    json.put("emptyArray", new ArrayList<>());
    json.put("anotherNonEmpty", Arrays.asList("a", "b"));
    json.put("anotherEmpty", Collections.emptySet());

    // Verify the object was created correctly
    assertThat(json.getJSONArray("nonEmptyArray").length()).isEqualTo(3);
    assertThat(json.getJSONArray("emptyArray").length()).isEqualTo(0);
    assertThat(json.getJSONArray("anotherNonEmpty").length()).isEqualTo(2);
    assertThat(json.getJSONArray("anotherEmpty").length()).isEqualTo(0);

    // Verify serialization works
    String serialized = json.toString();
    assertThat(serialized).isNotNull();
    assertThat(serialized).contains("\"emptyArray\":[]");
    assertThat(serialized).contains("\"anotherEmpty\":[]");
  }

  @Test
  void emptyIterableTypes() {
    // Test various types of empty iterables
    JSONObject json = new JSONObject();

    // Different types of empty collections
    json.put("arrayList", new ArrayList<>());
    json.put("linkedList", new LinkedList<>());
    json.put("hashSet", new HashSet<>());
    json.put("treeSet", new TreeSet<>());
    json.put("linkedHashSet", new LinkedHashSet<>());
    json.put("vector", new Vector<>());
    json.put("stack", new Stack<>());

    // All should create empty arrays
    for (String key : List.of("arrayList", "linkedList", "hashSet", "treeSet",
        "linkedHashSet", "vector", "stack")) {
      assertThat(json.has(key)).isTrue();
      assertThat(json.getJSONArray(key).length()).isEqualTo(0);
    }

    // Verify serialization works for all
    String serialized = json.toString();
    assertThat(serialized).isNotNull();
    assertThat(serialized).contains("[]");
  }

  @Test
  void emptyCollectionsSerialization() {
    // Test that empty collections serialize correctly to JSON strings
    JSONObject json = new JSONObject();
    json.put("empty", Collections.emptyList());

    String jsonString = json.toString();
    assertThat(jsonString).contains("\"empty\":[]");

    // Test round-trip
    JSONObject parsed = new JSONObject(jsonString);
    assertThat(parsed.has("empty")).isTrue();
    assertThat(parsed.getJSONArray("empty").length()).isEqualTo(0);
  }

  @Test
  void emptyArrayToMap() {
    // Test conversion to Map with empty arrays
    JSONObject json = new JSONObject();
    json.put("empty", new ArrayList<>());
    json.put("data", "value");

    Map<String, Object> map = json.toMap();
    assertThat(map.get("empty")).isInstanceOf(List.class);
    assertThat(((List<?>) map.get("empty"))).isEmpty();
    assertThat(map.get("data")).isEqualTo("value");
  }

  @Test
  void emptyArrayValidation() {
    // Test validation method with empty arrays
    JSONObject json = new JSONObject();
    json.put("empty", new ArrayList<>());
    json.put("value", 42);

    // Should not throw during validation
    json.validate();

    assertThat(json.getJSONArray("empty").length()).isEqualTo(0);
    assertThat(json.getInt("value")).isEqualTo(42);
  }
}
