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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression guard for #9114 (#9043, #9044): the JSON classes wrote a non-finite number as {@code 0} (JSONArray.put(Number)), as the
 * text {@code NaN} (JSONArray.put(Object), the Collection constructor), or as {@code null} (JSONObject.put(String, Number)), and a
 * {@code byte[]} inside a list as its identity hash ({@code "[B@33a10788"}) while a top-level one was an array of integers.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9114JsonArrayNonFiniteAndByteArrayTest extends TestHelper {

  @Test
  void nonFiniteIsNullInEveryPosition() {
    final List<Object> list = new ArrayList<>(List.of(1.0, Double.NaN, Double.POSITIVE_INFINITY, 4.0));
    final String expected = "[1.0,null,null,4.0]";

    assertThat(new JSONArray().put((Number) Double.NaN).toString()).isEqualTo("[null]");
    assertThat(new JSONArray().put((Object) Double.NaN).toString()).isEqualTo("[null]");
    assertThat(new JSONArray().put((Object) Float.NEGATIVE_INFINITY).toString()).isEqualTo("[null]");
    assertThat(new JSONArray(list).toString()).isEqualTo(expected);
    assertThat(new JSONArray(list.toArray()).toString()).isEqualTo(expected);
    assertThat(new JSONObject().put("v", (Object) list).getJSONArray("v").toString()).isEqualTo(expected);
    assertThat(new JSONObject(Map.of("v", list)).getJSONArray("v").toString()).isEqualTo(expected);
    assertThat(new JSONObject().put("v", new float[] { 1F, Float.NaN, Float.POSITIVE_INFINITY }).getJSONArray("v").toString())
        .isEqualTo("[1.0,null,null]");
    assertThat(new JSONObject().put("v", Double.NaN).isNull("v")).isTrue();
  }

  @Test
  void finiteNumbersKeepTheirValue() {
    assertThat(new JSONArray(List.of(1, 2L, 3.5d, new BigDecimal("1e400"))).toString()).isEqualTo("[1,2,3.5,1E+400]");
  }

  @Test
  void primitiveArrayInsideAListIsAnArray() {
    final byte[] bytes = { 1, 2 };
    assertThat(new JSONArray(List.of(bytes)).toString()).isEqualTo("[[1,2]]");
    assertThat(new JSONArray().put((Object) bytes).toString()).isEqualTo("[[1,2]]");
    assertThat(new JSONArray().put((Object) new int[] { 1, 2 }).toString()).isEqualTo("[[1,2]]");
    assertThat(new JSONObject().put("v", List.of(bytes)).getJSONArray("v").toString()).isEqualTo("[[1,2]]");
    assertThat(new JSONObject().put("v", Map.of("k", bytes)).getJSONObject("v").toString()).isEqualTo("{\"k\":[1,2]}");
    assertThat(new JSONObject().put("v", bytes).getJSONArray("v").toString()).isEqualTo("[1,2]");
  }

  @Test
  void storedRecordWithByteArrayInList() {
    database.command("sql", "CREATE DOCUMENT TYPE Blob");
    database.transaction(() -> {
      final Map<String, Object> map = new HashMap<>();
      map.put("k", new byte[] { 1, 2 });
      database.newDocument("Blob").set("in_list", new ArrayList<>(List.of(new byte[] { 1, 2 }))).set("in_map", map).save();
    });
    try (final ResultSet rs = database.query("sql", "SELECT FROM Blob")) {
      final Result row = rs.next();
      final JSONObject json = row.toJSON();
      assertThat(json.getJSONArray("in_list").toString()).doesNotContain("[B@");
      assertThat(json.getJSONArray("in_list").getJSONArray(0).toString()).isEqualTo("[1,2]");
    }
  }
}
