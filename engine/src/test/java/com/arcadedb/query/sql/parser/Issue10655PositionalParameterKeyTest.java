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
package com.arcadedb.query.sql.parser;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10655 (item 1): {@link PositionalParameter} looks its value up under the key {@code String.valueOf(paramNumber)};
 * the key for small numbers now comes from a table instead of being built on every evaluation. Lookup semantics must not
 * change, for numbers inside the table, at its edges and above it.
 */
class Issue10655PositionalParameterKeyTest extends TestHelper {

  private static final int[] NUMBERS = { 0, 1, 9, 10, 31, 62, 63, 64, 65, 99, 100, 1_000, 123_456, Integer.MAX_VALUE, -1, -64 };

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE V");
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.newDocument("V").set("a", (long) i).save();
    });
  }

  @Test
  void getValueAndBindResolveTheKeyOfEveryNumber() {
    for (final int number : NUMBERS) {
      final PositionalParameter parameter = new PositionalParameter();
      parameter.paramNumber = number;

      final Map<String, Object> params = new HashMap<>();
      params.put(String.valueOf(number), "v" + number);
      // a neighbour under another key must never be picked
      params.put(String.valueOf(number + 1), "other");

      assertThat(parameter.getValue(params)).as("getValue %d", number).isEqualTo("v" + number);
      // bind returns the parsed tree of the value under this number's key, never the neighbour's
      assertThat(String.valueOf(parameter.bindFromInputParams(params))).as("bind %d", number)
          .isEqualTo(String.valueOf(parameter.toParsedTree("v" + number)))
          .doesNotContain("other");
      assertThat(parameter.getValue(new HashMap<>())).as("absent %d", number).isNull();
      assertThat(parameter.getValue(null)).isNull();
      assertThat(parameter.bindFromInputParams(null)).isSameAs(parameter);
    }
  }

  @Test
  void theKeyFollowsAChangeOfTheNumber() {
    final PositionalParameter parameter = new PositionalParameter();
    final Map<String, Object> params = new HashMap<>();
    params.put("3", "three");
    params.put("70", "seventy");

    parameter.paramNumber = 3;
    assertThat(parameter.getValue(params)).isEqualTo("three");
    parameter.paramNumber = 70;
    assertThat(parameter.getValue(params)).isEqualTo("seventy");
    parameter.paramNumber = 3;
    assertThat(parameter.getValue(params)).isEqualTo("three");
  }

  @Test
  void aStatementWithMoreParametersThanTheTableStillBindsThemAll() {
    final int count = 100;
    final StringBuilder sql = new StringBuilder("SELECT FROM V WHERE a IN [");
    final Object[] args = new Object[count];
    for (int i = 0; i < count; i++) {
      sql.append(i == 0 ? "?" : ", ?");
      args[i] = 1_000L + i; // matches nothing...
    }
    sql.append("]");
    args[count - 1] = 7L; // ...except the last one, parameter number 99, which lies above the table
    try (final ResultSet rs = database.query("sql", sql.toString(), args)) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.<Long>getProperty("a")).isEqualTo(7L);
      assertThat(rs.hasNext()).isFalse();
    }

    // a filtered scan evaluates the same parameter for every row
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM V WHERE a >= ? AND a < ?", 3L, 8L)) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(5L);
    }
  }
}
