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
package com.arcadedb.serializer;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #9011: a datetime inside a LIST arrived as epoch milliseconds and inside a MAP without fractional
 * seconds, so two instants a microsecond apart became the same value.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9011TemporalInCollectionTest extends TestHelper {
  private static final LocalDateTime A = LocalDateTime.of(2026, 10, 3, 12, 34, 56, 123_456_000);
  private static final LocalDateTime B = LocalDateTime.of(2026, 10, 3, 12, 34, 56, 123_457_000);

  @Test
  void temporalsInListAndMapKeepTheirPrecision() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("T9011").createProperty("one", Type.DATETIME_MICROS);
      database.getSchema().getType("T9011").createProperty("many", Type.LIST);
      final Map<String, Object> byKey = new LinkedHashMap<>();
      byKey.put("k", A);
      database.newDocument("T9011").set("one", A).set("many", List.of(A, B)).set("byKey", byKey).save();
    });

    final JsonSerializer serializer = new JsonSerializer(database);
    try (final ResultSet rs = database.query("sql", "SELECT one, many, byKey FROM T9011")) {
      final JSONObject json = serializer.serializeResult(database, rs.next());
      assertThat(json.getString("one")).isEqualTo("2026-10-03 12:34:56.123456");
      final JSONArray many = json.getJSONArray("many");
      assertThat(many.getString(0)).isEqualTo("2026-10-03 12:34:56.123456");
      assertThat(many.getString(1)).isEqualTo("2026-10-03 12:34:56.123457");
      assertThat(json.getJSONObject("byKey").getString("k")).isEqualTo("2026-10-03 12:34:56.123456");
    }

    try (final ResultSet rs = database.query("sql", "SELECT FROM T9011")) {
      final JSONObject json = serializer.serializeResult(database, rs.next());
      assertThat(json.getJSONArray("many").getString(1)).isEqualTo("2026-10-03 12:34:56.123457");
      assertThat(json.getJSONObject("byKey").getString("k")).isEqualTo("2026-10-03 12:34:56.123456");
    }
  }

  @Test
  void wholeSecondTemporalInListIsAStringToo() {
    final LocalDateTime whole = LocalDateTime.of(2026, 10, 3, 12, 34, 56);
    database.getSchema().createDocumentType("T9011b");
    database.transaction(() -> database.newDocument("T9011b").set("many", List.of(whole)).save());
    final JsonSerializer serializer = new JsonSerializer(database);
    try (final ResultSet rs = database.query("sql", "SELECT many FROM T9011b")) {
      assertThat(serializer.serializeResult(database, rs.next()).getJSONArray("many").getString(0)).isEqualTo("2026-10-03 12:34:56");
    }
  }

  @Test
  void temporalsInNestedContainersAndOtherTemporalClassesKeepTheirPrecision() {
    database.getSchema().createDocumentType("T9011c");
    database.transaction(() -> {
      final Map<String, Object> byKey = new LinkedHashMap<>();
      byKey.put("l", List.of(A, B));
      database.newDocument("T9011c").set("nested", List.of(List.of(A), List.of(B))).set("map", byKey)
          .set("instants", List.of(A.toInstant(ZoneOffset.UTC))).save();
    });
    final JsonSerializer serializer = new JsonSerializer(database);
    try (final ResultSet rs = database.query("sql", "SELECT nested, map, instants FROM T9011c")) {
      final JSONObject json = serializer.serializeResult(database, rs.next());
      assertThat(json.getJSONArray("nested").getJSONArray(1).getString(0)).isEqualTo("2026-10-03 12:34:56.123457");
      assertThat(json.getJSONObject("map").getJSONArray("l").getString(0)).isEqualTo("2026-10-03 12:34:56.123456");
      assertThat(json.getJSONArray("instants").getString(0)).isEqualTo("2026-10-03 12:34:56.123456");
    }
  }
}
