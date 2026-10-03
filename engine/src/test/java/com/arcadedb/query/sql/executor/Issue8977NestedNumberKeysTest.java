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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression guard for #8977: SQL DISTINCT and GROUP BY kept [1], [1L], [1.0] and {a: 1}, {a: 1L} apart while count(DISTINCT)
 * and openCypher merged them, giving three different answers for the number of distinct values.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8977NestedNumberKeysTest extends TestHelper {

  @BeforeEach
  void load() {
    database.command("sql", "CREATE VERTEX TYPE T");
    final List<Object> values = List.of(List.of(1), List.of(1L), List.of(1.0d), List.of(new BigDecimal("1.00")), List.of(1, 2),
        List.of(1L, 2L), Map.of("a", 1), Map.of("a", 1L), 1, 1L, 1.0d);
    database.transaction(() -> {
      for (final Object v : values)
        database.newVertex("T").set("x", v).save();
    });
  }

  private long rows(final String language, final String query) {
    long n = 0;
    try (final ResultSet rs = database.query(language, query)) {
      while (rs.hasNext()) {
        rs.next();
        n++;
      }
    }
    return n;
  }

  private long scalar(final String language, final String query) {
    try (final ResultSet rs = database.query(language, query)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  @Test
  void selectDistinctMergesNestedNumbers() {
    assertThat(rows("sql", "SELECT DISTINCT x FROM T")).isEqualTo(4);
  }

  @Test
  void groupByMergesNestedNumbers() {
    assertThat(rows("sql", "SELECT x, count(*) AS n FROM T GROUP BY x")).isEqualTo(4);
  }

  @Test
  void countDistinctMergesNestedNumbers() {
    assertThat(scalar("sql", "SELECT count(DISTINCT x) AS c FROM T")).isEqualTo(4);
  }

  @Test
  void allFormsAgreeWithOpenCypher() {
    assertThat(scalar("opencypher", "MATCH (n:T) RETURN count(DISTINCT n.x) AS c")).isEqualTo(4);
    assertThat(scalar("sql", "SELECT count(*) AS c FROM (SELECT DISTINCT x FROM T)")).isEqualTo(4);
  }

  @Test
  void setsKeyByContentNotIterationOrder() {
    assertThat(Type.normalizeForKey(new LinkedHashSet<>(List.of(1, 2, 3)))).isEqualTo(Type.normalizeForKey(new LinkedHashSet<>(List.of(3L, 2L, 1L))));
  }

  @Test
  void normalizeForKeyShapes() {
    // list vs set stay different, sets ignore order, nested containers and null items are handled
    assertThat(Type.normalizeForKey(List.of(1, 2))).isEqualTo(Type.normalizeForKey(new Object[] { 1L, 2.0d }));
    assertThat(Type.normalizeForKey(List.of(1, 2))).isNotEqualTo(Type.normalizeForKey(Set.of(1, 2)));
    assertThat(Type.normalizeForKey(Map.of("a", List.of(1, Map.of("b", 2))))).isEqualTo(Type.normalizeForKey(Map.of("a", List.of(1L, Map.of("b", 2.0d)))));
    assertThat(Type.normalizeForKey(Arrays.asList(1, null))).isEqualTo(Type.normalizeForKey(Arrays.asList(1L, null)));
    assertThat(Type.normalizeForKey(null)).isNull();
    // a collection that holds nothing to canonicalize is returned as it is
    final List<String> plain = List.of("a", "b");
    assertThat(Type.normalizeForKey(plain)).isSameAs(plain);
  }

  @Test
  void primitiveArraysKeyByContentWithoutBoxing() {
    assertThat(Type.normalizeForKey(new float[] { 1f, 2f })).isEqualTo(Type.normalizeForKey(new float[] { 1f, 2f }));
    assertThat(Type.normalizeForKey(new float[] { 1f, 2f })).isNotEqualTo(Type.normalizeForKey(new float[] { 1f, 3f }));
    assertThat(Type.normalizeForKey(new byte[] { 1 })).isNotEqualTo(Type.normalizeForKey(new int[] { 1 }));
    assertThat(Type.normalizeForKey(new byte[] { 1, 2 }).hashCode()).isEqualTo(Type.normalizeForKey(new byte[] { 1, 2 }).hashCode());
  }
}
