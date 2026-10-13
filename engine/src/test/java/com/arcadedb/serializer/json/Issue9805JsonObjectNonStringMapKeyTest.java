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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.JsonSerializer;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9805: a JSON object's member name is DERIVED from a map key, it is not the key. {@code JsonSerializer.serializeMap}
 * held that rule, the shared {@link JSONObject} constructor did not: it cast every key to {@code String}, so a stored MAP with an
 * Integer key made {@code Result.toJSON()}, {@code convert.toJson()}, {@code asJSON()} and the Postgres wire throw
 * {@code ClassCastException}, and a {@code null} key an {@code IllegalArgumentException} that names neither the property nor the
 * key. The claim is pinned, not the case: a map whose keys are an Integer, a Long, a {@code null}, a Date and an arbitrary object
 * is rendered by every entry point to the same member names.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9805JsonObjectNonStringMapKeyTest extends TestHelper {

  /** An arbitrary key: only its text can name a JSON member. */
  private record Handle(String id) {
    @Override
    public String toString() {
      return "handle-" + id;
    }
  }

  private static final Date DATE_KEY = new Date(1_700_000_000_000L);

  private static Map<Object, Object> mixedKeys() {
    final Map<Object, Object> map = new LinkedHashMap<>();
    map.put(1, "int");
    map.put(2L, "long");
    map.put(null, "null");
    map.put(DATE_KEY, "date");
    map.put(new Handle("a"), "object");
    return map;
  }

  private static Set<String> mixedKeyNames() {
    return new TreeSet<>(List.of("1", "2", "null", DATE_KEY.toString(), "handle-a"));
  }

  /** {@code asJSON()} answers a JSON object and {@code convert.toJson()} its text: both must name the same members. */
  private static JSONObject asJson(final Object value) {
    return value instanceof JSONObject json ? json : new JSONObject(value.toString());
  }

  private static Set<String> names(final JSONObject json) {
    return new TreeSet<>(json.keySet());
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  void constructorNamesEveryKey() {
    // a raw Map is how every caller reaches the constructor: the Map<String, ?> of its signature is a promise none can keep
    final JSONObject json = new JSONObject((Map) mixedKeys());
    assertThat(names(json)).isEqualTo(mixedKeyNames());
    assertThat(json.getString("1")).isEqualTo("int");
    assertThat(json.getString("null")).isEqualTo("null");
    assertThat(json.getString("handle-a")).isEqualTo("object");
  }

  @Test
  void putOfANestedMapNamesEveryKey() {
    final JSONObject json = new JSONObject().put("m", mixedKeys());
    assertThat(names(json.getJSONObject("m"))).isEqualTo(mixedKeyNames());
  }

  @Test
  void arrayElementThatIsAMapNamesEveryKey() {
    final JSONArray array = new JSONArray();
    array.put((Object) mixedKeys());
    assertThat(names(array.getJSONObject(0))).isEqualTo(mixedKeyNames());

    final JSONArray fromCollection = new JSONArray(List.of(mixedKeys()));
    assertThat(names(fromCollection.getJSONObject(0))).isEqualTo(mixedKeyNames());

    final List<JSONObject> objects = array.toListOfObjects();
    assertThat(names(objects.getFirst())).isEqualTo(mixedKeyNames());
  }

  @Test
  void keysWithTheSameTextNameOneMemberAndTheLastEntryWinsOnEverySerializer() {
    final Map<Object, Object> map = new LinkedHashMap<>();
    map.put(null, "first");
    map.put("null", "second");
    map.put(1, "int");
    map.put("1", "string");

    final JSONObject constructed = new JSONObject(map);
    assertThat(names(constructed)).containsExactly("1", "null");
    assertThat(constructed.getString("null")).isEqualTo("second");
    assertThat(constructed.getString("1")).isEqualTo("string");

    final ResultInternal row = new ResultInternal(database);
    row.setProperty("m", map);
    final JSONObject viaResult = row.toJSON().getJSONObject("m");
    assertThat(viaResult.getString("null")).isEqualTo("second");
    assertThat(viaResult.getString("1")).isEqualTo("string");

    final JSONObject viaSerializer = JsonSerializer.createJsonSerializer().serializeResult(database, row).getJSONObject("m");
    assertThat(viaSerializer.getString("null")).isEqualTo("second");
    assertThat(viaSerializer.getString("1")).isEqualTo("string");
  }

  @Test
  void anEnumKeyIsNamedByItsText() {
    // the shape Gremlin's elementMap() produces: its keys are the T enum (T.id, T.label)
    final Map<Object, Object> map = new LinkedHashMap<>();
    map.put(TimeUnit.SECONDS, 1);
    map.put("name", "Jay");
    assertThat(names(new JSONObject(map))).containsExactly("SECONDS", "name");
    assertThat(names(new JSONObject().put("m", map).getJSONObject("m"))).containsExactly("SECONDS", "name");
  }

  @Test
  void aMapOfMapsNamesEveryKeyAtEveryLevel() {
    final Map<Object, Object> outer = new HashMap<>();
    outer.put(7, mixedKeys());
    final JSONObject json = new JSONObject().put("m", outer);
    assertThat(names(json.getJSONObject("m").getJSONObject("7"))).isEqualTo(mixedKeyNames());
  }

  @Test
  void nonStringKeysOfAStoredMapRenderThroughEveryEntryPoint() {
    store();
    for (int round = 0; round < 2; round++) {
      // round 1 runs after a close and reopen: the keys come back as Integer/Long, not as the Strings they were written as
      if (round == 1)
        reopenDatabase();

      try (final ResultSet rs = database.query("sql", "SELECT name, m FROM D WHERE name = 'intKeys'")) {
        final Result row = rs.next();
        assertThat(row.<Map<?, ?>>getProperty("m").keySet()).as("keys come back as numbers").allMatch(k -> k instanceof Number);

        final Set<String> expected = Set.of("1", "2");
        assertThat(names(row.toJSON().getJSONObject("m"))).as("Result.toJSON()").isEqualTo(expected);
        assertThat(names(JsonSerializer.createJsonSerializer().serializeResult(database, row).getJSONObject("m"))).as(
            "JsonSerializer.serializeResult").isEqualTo(expected);
      }

      try (final ResultSet rs = database.query("sql", "SELECT FROM D WHERE name = 'intKeys'")) {
        assertThat(names(rs.next().toJSON().getJSONObject("m"))).as("element Result.toJSON()").isEqualTo(Set.of("1", "2"));
      }

      try (final ResultSet rs = database.query("sql", "SELECT m.asJSON() AS j, convert.toJson(m) AS c FROM D WHERE name = 'intKeys'")) {
        final Result row = rs.next();
        assertThat(names(asJson(row.getProperty("j")))).as("asJSON()").isEqualTo(Set.of("1", "2"));
        assertThat(names(asJson(row.getProperty("c")))).as("convert.toJson()").isEqualTo(Set.of("1", "2"));
      }
    }
  }

  @Test
  void aNullKeyOfAStoredMapIsNamedNullThroughEveryEntryPoint() {
    store();
    try (final ResultSet rs = database.query("sql", "SELECT name, m FROM D WHERE name = 'nullKey'")) {
      final Result row = rs.next();
      assertThat(names(row.toJSON().getJSONObject("m"))).as("Result.toJSON()").isEqualTo(Set.of("null"));
      assertThat(names(JsonSerializer.createJsonSerializer().serializeResult(database, row).getJSONObject("m"))).as(
          "JsonSerializer.serializeResult").isEqualTo(Set.of("null"));
    }

    try (final ResultSet rs = database.query("sql", "SELECT m.asJSON() AS j, convert.toJson(m) AS c FROM D WHERE name = 'nullKey'")) {
      final Result row = rs.next();
      assertThat(names(asJson(row.getProperty("j")))).as("asJSON()").isEqualTo(Set.of("null"));
      assertThat(names(asJson(row.getProperty("c")))).as("convert.toJson()").isEqualTo(Set.of("null"));
    }
  }

  private void store() {
    final DocumentType type = database.getSchema().createDocumentType("D");
    type.createProperty("name", Type.STRING);
    type.createProperty("m", Type.MAP);

    final Map<Object, Object> intKeys = new LinkedHashMap<>();
    intKeys.put(1, "one");
    intKeys.put(2L, "two");
    final Map<Object, Object> nullKey = new HashMap<>();
    nullKey.put(null, "nothing");

    database.transaction(() -> {
      database.newDocument("D").set("name", "intKeys").set("m", intKeys).save();
      database.newDocument("D").set("name", "nullKey").set("m", nullKey).save();
    });
  }
}
