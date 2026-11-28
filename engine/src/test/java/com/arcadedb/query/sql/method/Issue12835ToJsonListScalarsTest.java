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
package com.arcadedb.query.sql.method;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12835: {@code toJSON()} / {@code asJSON()} on a list answered {@code null} for every number or boolean element
 * ({@code [1,2].toJSON()} was {@code [null,null]}).
 */
class Issue12835ToJsonListScalarsTest extends TestHelper {
  @Test
  void aListOfNumbersKeepsItsNumbers() {
    for (final String method : new String[] { "toJSON", "asJSON" }) {
      final JSONArray json = array("SELECT [1, 2.5, 3000000000].%s() AS v".formatted(method));
      assertThat(json.length()).isEqualTo(3);
      assertThat(json.getInt(0)).isEqualTo(1);
      assertThat(json.getDouble(1)).isEqualTo(2.5);
      assertThat(json.getLong(2)).isEqualTo(3000000000L);
    }
  }

  @Test
  void aMixedListKeepsRecordsNumbersAndBooleans() {
    final JSONArray json = array("SELECT [{\"a\": 1}, 5, true].toJSON() AS v");
    assertThat(json.length()).isEqualTo(3);
    assertThat(json.getJSONObject(0).getInt("a")).isEqualTo(1);
    assertThat(json.getInt(1)).isEqualTo(5);
    assertThat(json.get(2)).isEqualTo(true);
  }

  @Test
  void aStoredListOfLongsKeepsEveryValue() {
    database.command("sql", "CREATE DOCUMENT TYPE D");
    database.command("sql", "CREATE PROPERTY D.ids LIST OF LONG");
    database.transaction(() -> database.command("sql", "INSERT INTO D SET ids = [1234567890123456, 7]"));

    final JSONArray json = array("SELECT ids.toJSON() AS v FROM D");
    assertThat(json.getLong(0)).isEqualTo(1234567890123456L);
    assertThat(json.getLong(1)).isEqualTo(7L);
  }

  /** A nested list recurses, so its numbers were lost at every level ([[null,null],[null]]). */
  @Test
  void aNestedListKeepsItsNumbers() {
    final JSONArray nested = array("SELECT [[1, 2], [3]].toJSON() AS v");
    assertThat(nested.toString()).isEqualTo("[[1,2],[3]]");
  }

  /** JSON text inside a list is still parsed, as before. */
  @Test
  void aJsonTextElementIsStillParsed() {
    final JSONArray text = array("SELECT ['{\"a\": 1}'].toJSON() AS v");
    assertThat(text.getJSONObject(0).getInt("a")).isEqualTo(1);
  }

  /**
   * Every other element with no conversion of its own fell to null too: a link and a date are written as the same value is
   * written inside a map ({@code {"d": d}.toJSON()}), instead of being dropped.
   */
  @Test
  void aListOfLinksAndDatesKeepsEveryValue() {
    database.command("sql", "CREATE DOCUMENT TYPE L");
    database.command("sql", "CREATE PROPERTY L.d DATE");
    database.transaction(() -> database.command("sql", "INSERT INTO L SET d = date('2026-01-02', 'yyyy-MM-dd')"));

    final Document doc = database.iterateType("L", false).next().asDocument();

    try (final ResultSet rs = database.query("sql", "SELECT [@rid, d].toJSON() AS v, {\"d\": d}.toJSON() AS m FROM L")) {
      final Result row = rs.next();
      final JSONArray json = row.getProperty("v");
      final JSONObject map = row.getProperty("m");

      assertThat(json.length()).isEqualTo(2);
      assertThat(json.getString(0)).isEqualTo(doc.getIdentity().toString());
      assertThat(json.get(1)).isNotNull().isEqualTo(map.get("d"));
    }
  }

  private JSONArray array(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return (JSONArray) rs.next().getProperty("v");
    }
  }
}
