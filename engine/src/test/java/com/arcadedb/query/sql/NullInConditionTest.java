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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for SQL three-valued logic of {@code IN} / {@code NOT IN} when a NULL is
 * involved (issue #4591).
 * <p>
 * Standard SQL semantics (matching Neo4j/PostgreSQL/the SQL standard):
 * <ul>
 *   <li>{@code x IN (..., NULL)} with no match -&gt; UNKNOWN -&gt; row excluded in WHERE</li>
 *   <li>{@code x NOT IN (..., NULL)} with no match -&gt; UNKNOWN -&gt; row excluded in WHERE
 *       (this is the inverted case reported in the issue)</li>
 *   <li>{@code NULL IN (...)} / {@code NULL NOT IN (...)} -&gt; UNKNOWN -&gt; row excluded</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class NullInConditionTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Num4591");
      for (int i = 1; i <= 5; i++)
        database.command("SQL", "INSERT INTO Num4591 SET v = " + i);
    });
  }

  private List<Integer> values(final String sql, final Map<String, Object> params) {
    try (final ResultSet rs = params == null ? database.query("SQL", sql) : database.query("SQL", sql, params)) {
      return rs.stream().map(r -> (Integer) r.<Number>getProperty("v").intValue()).sorted().toList();
    }
  }

  @Test
  void notInWithNullLiteralExcludesEveryRow() {
    // 5 NOT IN (1,2,NULL) -> TRUE AND TRUE AND UNKNOWN -> UNKNOWN -> excluded
    // 1 NOT IN (1,2,NULL) -> FALSE -> excluded
    // No row must pass the filter.
    final List<Integer> result = values("SELECT FROM Num4591 WHERE v NOT IN (1, 2, null)", null);
    assertThat(result).isEmpty();
  }

  @Test
  void inWithNullLiteralKeepsOnlyMatches() {
    // 1 IN (1,2,NULL) -> TRUE ; 5 IN (1,2,NULL) -> UNKNOWN -> excluded
    final List<Integer> result = values("SELECT FROM Num4591 WHERE v IN (1, 2, null)", null);
    assertThat(result).containsExactly(1, 2);
  }

  @Test
  void notInWithoutNullIsUnaffected() {
    final List<Integer> result = values("SELECT FROM Num4591 WHERE v NOT IN (1, 2)", null);
    assertThat(result).containsExactly(3, 4, 5);
  }

  @Test
  void inWithoutNullIsUnaffected() {
    final List<Integer> result = values("SELECT FROM Num4591 WHERE v IN (1, 2)", null);
    assertThat(result).containsExactly(1, 2);
  }

  @Test
  void notInWithNullParameterListExcludesEveryRow() {
    final Map<String, Object> params = new HashMap<>();
    final List<Object> list = new ArrayList<>(List.of(
        1,
        2));
    list.add(null);
    params.put("list", list);
    final List<Integer> result = values("SELECT FROM Num4591 WHERE v NOT IN :list", params);
    assertThat(result).isEmpty();
  }

  @Test
  void inWithNullParameterListKeepsOnlyMatches() {
    final Map<String, Object> params = new HashMap<>();
    final List<Object> list = new ArrayList<>(List.of(
        1,
        2));
    list.add(null);
    params.put("list", list);
    final List<Integer> result = values("SELECT FROM Num4591 WHERE v IN :list", params);
    assertThat(result).containsExactly(1, 2);
  }

  @Test
  void notInNestedInsideNotIsNullSafe() {
    // Exercises NotBlock unboxing path: NOT (v IN (1,2,null)).
    // v=1 -> IN is TRUE  -> NOT TRUE -> excluded
    // v=5 -> IN is UNKNOWN -> NOT UNKNOWN -> UNKNOWN -> excluded
    final List<Integer> result = values("SELECT FROM Num4591 WHERE NOT (v IN (1, 2, null))", null);
    assertThat(result).isEmpty();
  }
}
