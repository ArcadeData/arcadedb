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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #4284: query parameters that are JSON numeric arrays arrive in the engine
 * as Java primitive arrays ({@code long[]}, {@code int[]}, {@code double[]}) rather than {@link List}.
 * Cypher list operations (IN, indexing, slicing, head/tail/last/reverse/size, concatenation, equality)
 * must treat these arrays as lists instead of failing.
 */
class CypherArrayParameterTest {
  private static final String DB_PATH = "./target/testarrayparams";
  private Database database;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
    database.getSchema().getOrCreateVertexType("Item");
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:Item {val: 10})");
      database.command("opencypher", "CREATE (n:Item {val: 20})");
      database.command("opencypher", "CREATE (n:Item {val: 30})");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  /**
   * Exact reproduction of issue #4284: collect the integer-encoded RIDs from id(), pass them back as a
   * long[] parameter, and filter with {@code ID(n) IN $ids}.
   */
  @Test
  void idInLongArrayParameter() {
    final List<Long> idList = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:Item) RETURN ID(n) AS id")) {
      while (rs.hasNext())
        idList.add(((Number) rs.next().getProperty("id")).longValue());
    }
    assertThat(idList).hasSize(3);

    final long[] ids = new long[idList.size()];
    for (int i = 0; i < ids.length; i++)
      ids[i] = idList.get(i);

    int count = 0;
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (n:Item) WHERE ID(n) IN $ids RETURN n.val AS val, ID(n) AS id", Map.of("ids", ids))) {
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
    }
    assertThat(count).isEqualTo(3);
  }

  @Test
  void inOperatorWithLongArrayParameter() {
    assertThat(matchValuesIn(new long[] { 10, 30 })).containsExactlyInAnyOrder(10L, 30L);
  }

  @Test
  void inOperatorWithIntArrayParameter() {
    assertThat(matchValuesIn(new int[] { 10, 30 })).containsExactlyInAnyOrder(10L, 30L);
  }

  @Test
  void inOperatorWithDoubleArrayParameter() {
    assertThat(matchValuesIn(new double[] { 10.0, 30.0 })).containsExactlyInAnyOrder(10L, 30L);
  }

  @Test
  void inOperatorWithObjectArrayParameter() {
    assertThat(matchValuesIn(new Object[] { 10, 30 })).containsExactlyInAnyOrder(10L, 30L);
  }

  @Test
  void listFunctionsWithLongArrayParameter() {
    final Map<String, Object> params = Map.of("list", new long[] { 1, 2, 3 });
    try (final ResultSet rs = database.query("opencypher",
        "RETURN head($list) AS h, last($list) AS l, size($list) AS s, tail($list) AS t, reverse($list) AS r", params)) {
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("h")).longValue()).isEqualTo(1L);
      assertThat(((Number) row.getProperty("l")).longValue()).isEqualTo(3L);
      assertThat(((Number) row.getProperty("s")).longValue()).isEqualTo(3L);
      assertThat(asLongList(row.getProperty("t"))).containsExactly(2L, 3L);
      assertThat(asLongList(row.getProperty("r"))).containsExactly(3L, 2L, 1L);
    }
  }

  @Test
  void indexingWithLongArrayParameter() {
    final Map<String, Object> params = Map.of("list", new long[] { 1, 2, 3 });
    try (final ResultSet rs = database.query("opencypher", "RETURN $list[0] AS first, $list[-1] AS lastEl", params)) {
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("first")).longValue()).isEqualTo(1L);
      assertThat(((Number) row.getProperty("lastEl")).longValue()).isEqualTo(3L);
    }
  }

  @Test
  void slicingWithLongArrayParameter() {
    final Map<String, Object> params = Map.of("list", new long[] { 1, 2, 3, 4 });
    try (final ResultSet rs = database.query("opencypher", "RETURN $list[1..3] AS slice", params)) {
      assertThat(asLongList(rs.next().getProperty("slice"))).containsExactly(2L, 3L);
    }
  }

  @Test
  void concatenationWithLongArrayParameter() {
    final Map<String, Object> params = Map.of("list", new long[] { 1, 2, 3 });
    try (final ResultSet rs = database.query("opencypher", "RETURN $list + 4 AS c", params)) {
      assertThat(asLongList(rs.next().getProperty("c"))).containsExactly(1L, 2L, 3L, 4L);
    }
  }

  @Test
  void equalityWithLongArrayParameter() {
    final Map<String, Object> params = Map.of("list", new long[] { 1, 2, 3 });
    try (final ResultSet rs = database.query("opencypher", "RETURN $list = [1, 2, 3] AS eq, $list = [1, 2] AS neq", params)) {
      final Result row = rs.next();
      assertThat((Boolean) row.getProperty("eq")).isTrue();
      assertThat((Boolean) row.getProperty("neq")).isFalse();
    }
  }

  private List<Long> matchValuesIn(final Object arrayParam) {
    final List<Long> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (n:Item) WHERE n.val IN $vals RETURN n.val AS val", Map.of("vals", arrayParam))) {
      while (rs.hasNext())
        values.add(((Number) rs.next().getProperty("val")).longValue());
    }
    return values;
  }

  private static List<Long> asLongList(final Object value) {
    final List<?> source = (List<?>) value;
    final List<Long> result = new ArrayList<>(source.size());
    for (final Object o : source)
      result.add(((Number) o).longValue());
    return result;
  }
}
