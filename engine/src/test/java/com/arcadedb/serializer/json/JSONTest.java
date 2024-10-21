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

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test JSON parser and it support for types.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class JSONTest extends TestHelper {
  @Test
  public void testDates() {
    final Date date = new Date();
    JSONObject json = new JSONObject().put("date", date);
    final String serialized = json.toString();
    JSONObject deserialized = new JSONObject(serialized);
    assertThat(deserialized).isEqualTo(json);
  }

  @Test
  public void testLists() {
    JSONObject json = new JSONObject().put("list", Collections.unmodifiableList(List.of(1, 2, 3)));
    final String serialized = json.toString();
    JSONObject deserialized = new JSONObject(serialized);
    assertThat(deserialized).isEqualTo(json);
  }

  @Test
  public void testListsOfLists() {
    final List<List<Integer>> list = List.of(Collections.unmodifiableList(List.of(1, 2, 3)),
        Collections.unmodifiableList(List.of(7, 8, 9)));
    JSONObject json = new JSONObject().put("list", list);
    final String serialized = json.toString();
    JSONObject deserialized = new JSONObject(serialized);
    assertThat(deserialized).isEqualTo(json);
  }

  @Test
  public void testDatesWithFormat() {
    final Date date = new Date();
    JSONObject json = new JSONObject().setDateFormat(database.getSchema().getDateTimeFormat()).put("date", date);

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
    final JSONObject json = new JSONObject().put("a", 10);
    json.put("nan", Double.NaN);
    json.put("arrayNan", new JSONArray().put(0).put(Double.NaN).put(5));
    json.validate();

    assertThat(json.getInt("nan")).isEqualTo(0);
    assertThat(json.getJSONArray("arrayNan").get(0)).isEqualTo(0);
    assertThat(json.getJSONArray("arrayNan").get(1)).isEqualTo(0);
    assertThat(json.getJSONArray("arrayNan").get(2)).isEqualTo(5);
  }
}
