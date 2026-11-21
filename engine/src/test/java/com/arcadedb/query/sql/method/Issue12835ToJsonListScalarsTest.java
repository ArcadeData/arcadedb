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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
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

  /** The branches that already worked are unchanged: nested lists, maps and JSON text. */
  @Test
  void theOtherElementKindsAreUnchanged() {
    final JSONArray nested = array("SELECT [[1, 2], [3]].toJSON() AS v");
    assertThat(nested.toString()).isEqualTo("[[1,2],[3]]");

    final JSONArray text = array("SELECT ['{\"a\": 1}'].toJSON() AS v");
    assertThat(text.getJSONObject(0).getInt("a")).isEqualTo(1);
  }

  private JSONArray array(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return (JSONArray) rs.next().getProperty("v");
    }
  }
}
