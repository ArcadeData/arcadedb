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
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test JSON parser and it support for types.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class JSONTest extends TestHelper {
  @Test
  public void testDates() {
    JSONObject json = new JSONObject()
        .put("date", new Date())
        .put("dateTime", LocalDateTime.now())
        .put("localDate", LocalDate.now());
    final String serialized = json.toString();
    JSONObject deserialized = new JSONObject(serialized);
    assertThat(deserialized).isEqualTo(json);
  }

  @Test
  public void testLists() {
    JSONObject json = new JSONObject().put("list", List.of(1, 2, 3));
    final String serialized = json.toString();
    JSONObject deserialized = new JSONObject(serialized);
    assertThat(deserialized).isEqualTo(json);
  }

  @Test
  public void testListsOfLists() {
    final List<List<Integer>> list = List.of(List.of(1, 2, 3), List.of(7, 8, 9));
    JSONObject json = new JSONObject().put("list", list);
    final String serialized = json.toString();
    JSONObject deserialized = new JSONObject(serialized);
    assertThat(deserialized).isEqualTo(json);
  }

  @Test
  public void testDatesWithFormat() {
    JSONObject json = new JSONObject()
        .setDateFormat(database.getSchema().getDateFormat())
        .setDateTimeFormat(database.getSchema().getDateTimeFormat())
        .put("date", new Date())
        .put("dateTime", LocalDateTime.now())
        .put("localDate", LocalDate.now());

    final String serialized = json.toString();
    JSONObject deserialized = new JSONObject(serialized);

    assertThat(deserialized).isEqualTo(json);
  }

  @Test
  public void testEmbeddedMaps() {
    final Map<String, Object> map = new HashMap<>();
    map.put("first", 1);
    map.put("2nd", 2);
    JSONObject json = new JSONObject().put("map", map);

    final String serialized = json.toString();
    JSONObject deserialized = new JSONObject(serialized);

    assertThat(deserialized).isEqualTo(json);
  }

  @Test
  public void testMalformedTrailingCommas() {
    JSONObject json = new JSONObject("{'array':[1,2,3,]}");
    assertThat(json.getJSONArray("array").length()).isEqualTo(4);

    json = new JSONObject("{'array':[{'a':3},]}");
    assertThat(json.getJSONArray("array").length()).isEqualTo(2);
// NOT SUPPORTED BY GSON LIBRARY
//    json = new JSONObject("{'map':{'a':3,}");
//    Assertions.assertThat(json.getJSONArray("map").length()).isEqualTo(2);
  }

  @Test
  public void testNaN() {
    final JSONObject json = new JSONObject()
        .put("a", 10)
        .put("nan", Double.NaN)
        .put("arrayNan", new JSONArray().put(0).put(Double.NaN).put(5));

    json.validate();

    assertThat(json.getInt("nan")).isEqualTo(0);
    assertThat(json.getJSONArray("arrayNan").get(0)).isEqualTo(0);
    assertThat(json.getJSONArray("arrayNan").get(1)).isEqualTo(0);
    assertThat(json.getJSONArray("arrayNan").get(2)).isEqualTo(5);
  }

  @Test
  void testMixedTypes() {
    JSONObject json = new JSONObject()
        .put("int", 10)
        .put("float", 10.5f)
        .put("double", 10.5d)
        .put("long", 10L)
        .put("string", "hello")
        .put("boolean", true)
        .put("null", (String) null)
        .put("array", List.of(1, 2, 3))
        .put("stringArray", new String[] { "one", "two", "three" })
        .put("map", Map.of("a", 1, "b", 2, "c", 3));
    json.validate();

    assertThat(json.getInt("int")).isEqualTo(10);
    assertThat(json.getFloat("float")).isEqualTo(10.5f);
    assertThat(json.getDouble("double")).isEqualTo(10.5d);
    assertThat(json.getLong("long")).isEqualTo(10L);
    assertThat(json.getString("string")).isEqualTo("hello");
    assertThat(json.getBoolean("boolean")).isTrue();
    assertThat(json.isNull("null")).isTrue();
    assertThat(json.getJSONArray("array").length()).isEqualTo(3);
    assertThat(json.getJSONArray("stringArray").length()).isEqualTo(3);
    assertThat(json.getJSONObject("map").length()).isEqualTo(3);

    Map<String, Object> map = json.toMap();
    assertThat(map.get("int")).isEqualTo(10);
    assertThat(map.get("float")).isEqualTo(10.5f);
    assertThat(map.get("double")).isEqualTo(10.5d);
    assertThat(map.get("long")).isEqualTo(10L);
    assertThat(map.get("string")).isEqualTo("hello");
    assertThat(map.get("boolean")).isEqualTo(true);
    assertThat(map.get("null")).isNull();
    assertThat(map.get("array")).isEqualTo(List.of(1, 2, 3));
    assertThat(map.get("stringArray")).isEqualTo(List.of("one", "two", "three"));
    assertThat(map.get("map")).isEqualTo(Map.of("a", 1, "b", 2, "c", 3));
  }

  // MICRO BENCHMARK
  public static void main(String[] args) {
    final JSONObject json = new JSONObject()
        .put("float", 3.14F)
        .put("double", 3.14D)
        .put("int", 3)
        .put("long", 33426776323232L);

    var beginTime = System.currentTimeMillis();
    for (int i = 0; i < 100_000_000; i++) {
      final float value = (float) json.get("float");
      Assertions.assertThat(value).isEqualTo(3.14F);
    }
    System.out.println("JSON float: " + (System.currentTimeMillis() - beginTime) + "ms");

    beginTime = System.currentTimeMillis();
    for (int i = 0; i < 100_000_000; i++) {
      final double value = (double) json.get("double");
      Assertions.assertThat(value).isEqualTo(3.14D);
    }
    System.out.println("JSON double: " + (System.currentTimeMillis() - beginTime) + "ms");

    beginTime = System.currentTimeMillis();
    for (int i = 0; i < 100_000_000; i++) {
      final long value = (long) json.get("long");
      Assertions.assertThat(value).isEqualTo(33426776323232L);
    }
    System.out.println("JSON long: " + (System.currentTimeMillis() - beginTime) + "ms");
  }
}
