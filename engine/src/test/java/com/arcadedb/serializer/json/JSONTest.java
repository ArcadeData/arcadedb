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

import java.time.*;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Test JSON parser and it support for types.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class JSONTest extends TestHelper {
  @Test
  void dates() {
    JSONObject json = new JSONObject()
        .put("date", new Date())
        .put("dateTime", LocalDateTime.now())
        .put("localDate", LocalDate.now());
    final String serialized = json.toString();
    JSONObject deserialized = new JSONObject(serialized);
    assertThat(deserialized).isEqualTo(json);
  }

  @Test
  void lists() {
    JSONObject json = new JSONObject().put("list", List.of(1, 2, 3));
    final String serialized = json.toString();
    JSONObject deserialized = new JSONObject(serialized);
    assertThat(deserialized).isEqualTo(json);
  }

  @Test
  void listsOfLists() {
    final List<List<Integer>> list = List.of(List.of(1, 2, 3), List.of(7, 8, 9));
    JSONObject json = new JSONObject().put("list", list);
    final String serialized = json.toString();
    JSONObject deserialized = new JSONObject(serialized);
    assertThat(deserialized).isEqualTo(json);
  }

  @Test
  void datesWithFormat() {
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
  void embeddedMaps() {
    final Map<String, Object> map = new HashMap<>(Map.of(
        "first", 1,
        "2nd", 2));
    JSONObject json = new JSONObject().put("map", map);

    final String serialized = json.toString();
    JSONObject deserialized = new JSONObject(serialized);

    assertThat(deserialized).isEqualTo(json);
  }

  @Test
  void malformedTrailingCommas() {
    JSONObject json = new JSONObject("{'array':[1,2,3,]}");
    assertThat(json.getJSONArray("array").length()).isEqualTo(4);

    json = new JSONObject("{'array':[{'a':3},]}");
    assertThat(json.getJSONArray("array").length()).isEqualTo(2);
// NOT SUPPORTED BY GSON LIBRARY
//    json = new JSONObject("{'map':{'a':3,}");
//    Assertions.assertThat(json.getJSONArray("map").length()).isEqualTo(2);
  }

  @Test
  void naN() {
    final JSONObject json = new JSONObject()
        .put("a", 10)
        .put("nan", Double.NaN)
        .put("arrayNan", new JSONArray().put(0).put(Double.NaN).put(5));

    assertThat(json.isNull("nan")).isTrue();
    assertThat(json.getJSONArray("arrayNan").get(0)).isEqualTo(0);
    assertThat(json.getJSONArray("arrayNan").get(1)).isEqualTo(0);
    assertThat(json.getJSONArray("arrayNan").get(2)).isEqualTo(5);

    final JSONObject json2 = new JSONObject(Map.of(
        "a", 10,
        "nan", Double.NaN,
        "arrayNan", List.of(0, Double.NaN, 5))
    );

    assertThat(json2.isNull("nan")).isTrue();
    assertThat(json2.getJSONArray("arrayNan").get(0)).isEqualTo(0);
    assertThat(json2.getJSONArray("arrayNan").get(1)).isEqualTo(0);
    assertThat(json2.getJSONArray("arrayNan").get(2)).isEqualTo(5);
  }

  @Test
  void mixedTypes() {
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

  @Test
  void expressions() {
    final Map<String, Object> map = new HashMap<>(Map.of(
        "first", 1,
        "second", new JSONArray().put(3).put(5)));
    JSONObject json = new JSONObject().put("map", map);

    final String serialized = json.toString();
    JSONObject deserialized = new JSONObject(serialized);

    assertThat(deserialized).isEqualTo(json);
    assertThat(deserialized.getExpression("map.second[1]")).isEqualTo(5);
  }

  /**
   * Test for GitHub issue #1602: Special characters should be preserved in JSON output.
   * <p>
   * Verifies that special characters like &, <, >, ", ' are NOT escaped to unicode escapes
   * like \u0026, \u003c, etc. when serializing to JSON.
   */
  @Test
  void specialCharacters() {
    JSONObject json = new JSONObject()
        .put("ampersand", "LdhgfdY&hgff2&a")
        .put("lessThan", "a < b")
        .put("greaterThan", "b > a")
        .put("quote", "He said \"hello\"")
        .put("singleQuote", "It's a test")
        .put("allTogether", "& < > \" '")
        .put("multipleAmpersands", "a && b &&& c");

    final String serialized = json.toString();

    // Verify that special characters are NOT unicode-escaped
    assertThat(serialized)
        .as("Ampersand should not be escaped to \\u0026")
        .contains("&")
        .doesNotContain("\\u0026");
    assertThat(serialized)
        .as("Less-than should not be escaped to \\u003c")
        .contains("<")
        .doesNotContain("\\u003c");
    assertThat(serialized)
        .as("Greater-than should not be escaped to \\u003e")
        .contains(">")
        .doesNotContain("\\u003e");

    // Verify round-trip: deserialize and check values
    JSONObject deserialized = new JSONObject(serialized);
    assertThat(deserialized.getString("ampersand")).isEqualTo("LdhgfdY&hgff2&a");
    assertThat(deserialized.getString("lessThan")).isEqualTo("a < b");
    assertThat(deserialized.getString("greaterThan")).isEqualTo("b > a");
    assertThat(deserialized.getString("quote")).isEqualTo("He said \"hello\"");
    assertThat(deserialized.getString("singleQuote")).isEqualTo("It's a test");
    assertThat(deserialized.getString("allTogether")).isEqualTo("& < > \" '");
    assertThat(deserialized.getString("multipleAmpersands")).isEqualTo("a && b &&& c");
  }

  @Test
  void temporalTypesInArrays() {
    final LocalDateTime ldt = LocalDateTime.of(2024, 6, 15, 10, 30, 0);
    final LocalDate ld = LocalDate.of(2024, 6, 15);
    final Date d = new Date(1_000_000L);

    // LocalDateTime inside a List → put(String, Object) → JSONArray constructor
    JSONObject json = new JSONObject().put("dates", List.of(ldt));
    assertThat(json.getJSONArray("dates").length()).isEqualTo(1);
    assertThat(json.getJSONArray("dates").get(0)).isInstanceOf(Number.class);

    // LocalDate inside a List
    json = new JSONObject().put("dates", List.of(ld));
    assertThat(json.getJSONArray("dates").length()).isEqualTo(1);
    assertThat(json.getJSONArray("dates").get(0)).isInstanceOf(Number.class);

    // java.util.Date inside a List - value is deterministic (epoch millis)
    json = new JSONObject().put("dates", List.of(d));
    assertThat(json.getJSONArray("dates").length()).isEqualTo(1);
    assertThat(((Number) json.getJSONArray("dates").get(0)).longValue()).isEqualTo(1_000_000L);

    // JSONArray.put(Object) with LocalDateTime
    final JSONArray arr = new JSONArray();
    arr.put((Object) ldt);
    assertThat(arr.length()).isEqualTo(1);
    assertThat(arr.get(0)).isInstanceOf(Number.class);

    // JSONArray(Collection) constructor with temporal elements
    final JSONArray arrFromColl = new JSONArray(List.of(ldt, ld, d));
    assertThat(arrFromColl.length()).isEqualTo(3);
    for (int i = 0; i < 3; i++)
      assertThat(arrFromColl.get(i)).isInstanceOf(Number.class);

    // JSONArray(Object[]) constructor with temporal elements
    final JSONArray arrFromArr = new JSONArray(new Object[] { ldt, ld, d });
    assertThat(arrFromArr.length()).isEqualTo(3);
    for (int i = 0; i < 3; i++)
      assertThat(arrFromArr.get(i)).isInstanceOf(Number.class);

    // Mixed temporal types round-trip through JSON string
    json = new JSONObject().put("mixed", List.of(ldt, ld));
    final JSONObject deserialized = new JSONObject(json.toString());
    assertThat(deserialized.getJSONArray("mixed").length()).isEqualTo(2);
    for (int i = 0; i < 2; i++)
      assertThat(deserialized.getJSONArray("mixed").get(i)).isInstanceOf(Number.class);
  }

  @Test
  void unsupportedTemporalInArrayFallsBackToString() {
    // LocalTime is a TemporalAccessor that DateUtils.dateTimeToTimestamp does not support and
    // returns null for; the array serializer must fall back to toString() rather than NPE.
    final LocalTime time = LocalTime.of(13, 45, 30);
    final JSONArray arr = new JSONArray(List.of(time));
    assertThat(arr.length()).isEqualTo(1);
    assertThat(arr.get(0)).isEqualTo(time.toString());
  }

  @Test
  void durationInArrayPreservesPrecisionAndSign() {
    // Leading-zero nanoseconds must not be lost (5ns must not collapse to 0.5s).
    final JSONArray smallNanos = new JSONArray(List.of(Duration.ofSeconds(5, 5)));
    assertThat(smallNanos.getDouble(0)).isCloseTo(5.000000005, within(1e-12));

    // Negative durations stored as (negative seconds, positive nanos) must serialize correctly,
    // not as the naive "-6.999999995" that "%d.%d" formatting would produce.
    final Duration negative = Duration.ofSeconds(-5).minusNanos(5); // -5.000000005s
    final JSONArray neg = new JSONArray(List.of(negative));
    assertThat(neg.getDouble(0)).isCloseTo(-5.000000005, within(1e-12));
    assertThat(neg.getDouble(0)).isLessThan(0.0);
  }

  @Test
  void enumAndClassInArrays() {
    // Enum serializes to its name(); the array path must mirror put(String, Object).
    final JSONArray enums = new JSONArray(List.of(DayOfWeek.MONDAY, Month.JUNE));
    assertThat(enums.getString(0)).isEqualTo("MONDAY");
    assertThat(enums.getString(1)).isEqualTo("JUNE");

    // Class serializes to its fully-qualified name (matching put(String, Object)), not toString().
    final JSONArray classes = new JSONArray(List.of(String.class));
    assertThat(classes.getString(0)).isEqualTo("java.lang.String");
  }

  @Test
  void localDateConsistentBetweenPutAndArrayPaths() {
    // put(String, Object) and the array path (objectToElement) must produce the same epoch-millis
    // for a LocalDate, both using the DST-correct atStartOfDay(ZoneId) form.
    final LocalDate ld = LocalDate.of(2024, 6, 15);
    final long viaPut = new JSONObject().put("d", ld).getLong("d");
    final long viaArray = ((Number) new JSONArray(List.of(ld)).get(0)).longValue();
    assertThat(viaPut).isEqualTo(viaArray);
  }

  @Test
  void durationConsistentBetweenPutAndArrayPaths() {
    // put(String, Object) and the array path (objectToElement) must produce the same value.
    final Duration duration = Duration.ofSeconds(5, 5); // 5.000000005s
    final double viaPut = new JSONObject().put("d", duration).getDouble("d");
    final double viaArray = new JSONArray(List.of(duration)).getDouble(0);
    assertThat(viaPut).isEqualTo(viaArray);
    assertThat(viaPut).isCloseTo(5.000000005, within(1e-12));
  }

  @Test
  void nestedExpression() {
    final String schema = """
          {
          "presentation": {
          },
          "login": {
            "default": {
              "url": "https://url1.com"
            },
            "mobile": {
              "url": "https://url2.com"
            },
            "mobile-native": {
              "url": "https://url3.com"
            }
          }
        }
        """;

    JSONObject deserialized = new JSONObject(schema);
    assertThat(deserialized.getExpression("login.default.url")).isEqualTo("https://url1.com");
  }

  /**
   * Issue #4709: JSONArray(String) must preserve the original parse error as the cause and include the offending input in the message,
   * matching the behavior of JSONObject(String).
   */
  @Test
  void invalidJSONArrayKeepsParseCause() {
    final String invalid = "[1, 2,";
    assertThatThrownBy(() -> new JSONArray(invalid))
        .isInstanceOf(JSONException.class)
        .hasMessageContaining(invalid)
        .hasCauseInstanceOf(Exception.class);
  }

//
//  // MICRO BENCHMARK
//  public static void main(String[] args) {
//    final JSONObject json = new JSONObject()
//        .put("float", 3.14F)
//        .put("double", 3.14D)
//        .put("int", 3)
//        .put("long", 33426776323232L);
//
//    var beginTime = System.currentTimeMillis();
//    for (int i = 0; i < 100_000_000; i++) {
//      final float value = (float) json.get("float");
//      Assertions.assertThat(value).isEqualTo(3.14F);
//    }
//    System.out.println("JSON float: " + (System.currentTimeMillis() - beginTime) + "ms");
//
//    beginTime = System.currentTimeMillis();
//    for (int i = 0; i < 100_000_000; i++) {
//      final double value = (double) json.get("double");
//      Assertions.assertThat(value).isEqualTo(3.14D);
//    }
//    System.out.println("JSON double: " + (System.currentTimeMillis() - beginTime) + "ms");
//
//    beginTime = System.currentTimeMillis();
//    for (int i = 0; i < 100_000_000; i++) {
//      final long value = (long) json.get("long");
//      Assertions.assertThat(value).isEqualTo(33426776323232L);
//    }
//    System.out.println("JSON long: " + (System.currentTimeMillis() - beginTime) + "ms");
//  }
}
