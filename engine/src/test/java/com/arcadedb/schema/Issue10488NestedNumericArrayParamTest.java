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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #10488: the HTTP command handler parses the request with {@code toMap(true)}, which turns a
 * homogeneous numeric array found anywhere in the tree into a primitive {@code long[]}/{@code double[]}. The top level of a
 * parameter is turned back into a list before it is bound, but a list nested in a list or in a map was stored as the
 * primitive array it was parsed to, so {@code =}, {@code .type()} and {@code GROUP BY} saw a different value than for the
 * same list written as a literal or from Java.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10488NestedNumericArrayParamTest extends TestHelper {

  private static final String REQUEST = "{\"params\":{\"m\":{\"a\":[1,2]},\"ll\":[[1,2],[3]],\"md\":{\"a\":[1.5,2.5]}}}";

  @Test
  void nestedListParameterIsStoredAsAList() {
    createType();
    insertRows();

    try (final ResultSet rs = database.query("sql", "SELECT FROM D ORDER BY k")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final String k = row.getProperty("k");
        assertThat(((Map<?, ?>) row.getProperty("m")).get("a")).as("m.a of " + k).isInstanceOf(List.class);
        assertThat(((List<?>) row.getProperty("ll")).get(0)).as("ll[0] of " + k).isInstanceOf(List.class);
        assertThat(((Map<?, ?>) row.getProperty("md")).get("a")).as("md.a of " + k).isInstanceOf(List.class);
      }
    }
  }

  @Test
  void nestedListParameterAnswersLikeTheLiteralAndTheJavaValue() {
    createType();
    insertRows();

    assertThat(keys("SELECT k FROM D WHERE m.a = [1, 2] ORDER BY k")).containsExactly("java", "literal", "param");
    assertThat(keys("SELECT k FROM D WHERE ll[0] = [1, 2] ORDER BY k")).containsExactly("java", "literal", "param");
    assertThat(keys("SELECT k FROM D WHERE m.a CONTAINS 2 ORDER BY k")).containsExactly("java", "literal", "param");
    assertThat(keys("SELECT k FROM D WHERE m.a.size() = 2 ORDER BY k")).containsExactly("java", "literal", "param");
    assertThat(keys("SELECT k FROM D WHERE m.a[1] = 2 ORDER BY k")).containsExactly("java", "literal", "param");
    assertThat(keys("SELECT k FROM D WHERE ll[0].size() = 2 ORDER BY k")).containsExactly("java", "literal", "param");
    assertThat(keys("SELECT k FROM D WHERE m.a.type() = 'LIST' ORDER BY k")).containsExactly("java", "literal", "param");
    assertThat(keys("SELECT k FROM D WHERE md.a = [1.5, 2.5] ORDER BY k")).containsExactly("java", "literal", "param");
  }

  @Test
  void identicalMapsFromParameterLiteralAndJavaFallInOneGroup() {
    createType();
    insertRows();

    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM D GROUP BY m")) {
      final List<Long> counts = new ArrayList<>();
      while (rs.hasNext())
        counts.add(rs.next().<Long>getProperty("c"));
      assertThat(counts).containsExactly(3L);
    }
  }

  @Test
  void nestedListIsStoredAsAListAfterAReopen() {
    createType();
    insertRows();
    reopenDatabase();

    assertThat(keys("SELECT k FROM D WHERE m.a.type() = 'LIST' ORDER BY k")).containsExactly("java", "literal", "param");
    assertThat(keys("SELECT k FROM D WHERE m.a = [1, 2] ORDER BY k")).containsExactly("java", "literal", "param");
  }

  @Test
  void aTopLevelPrimitiveArrayOfAListPropertyStillBecomesAList() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.l LIST");

    final Map<String, Object> params = new HashMap<>();
    params.put("l", new long[] { 1L, 2L });
    database.transaction(() -> database.command("sql", "INSERT INTO T SET l = :l", params).close());

    try (final ResultSet rs = database.query("sql", "SELECT FROM T WHERE l.type() = 'LIST'")) {
      assertThat(rs.stream().count()).isEqualTo(1L);
    }
  }

  @Test
  void undeclaredPropertyStoresTheNestedListAsAList() {
    database.command("sql", "CREATE DOCUMENT TYPE Schemaless");
    final Map<String, Object> params = new HashMap<>();
    params.put("m", Map.of("a", new long[] { 1L, 2L }));
    params.put("l", List.of(new double[] { 1.5, 2.5 }));
    database.transaction(() -> database.command("sql", "INSERT INTO Schemaless SET m = :m, l = :l", params).close());

    try (final ResultSet rs = database.query("sql", "SELECT FROM Schemaless WHERE m.a = [1, 2] AND l[0] = [1.5, 2.5]")) {
      assertThat(rs.stream().count()).isEqualTo(1L);
    }
  }

  @Test
  void unwrapKeepsTheContainerWhenThereIsNothingToReplace() {
    final Map<String, Object> plain = Map.of("a", List.of(1, 2), "b", Map.of("c", "x"));
    assertThat(Type.unwrapNestedPrimitiveArrays(plain)).isSameAs(plain);

    final List<Object> list = List.of(List.of(1), 2, "x");
    assertThat(Type.unwrapNestedPrimitiveArrays(list)).isSameAs(list);

    final long[] topLevel = { 1L, 2L };
    assertThat(Type.unwrapNestedPrimitiveArrays(topLevel)).isSameAs(topLevel);
    assertThat(Type.unwrapNestedPrimitiveArrays(null)).isNull();
    assertThat(Type.unwrapNestedPrimitiveArrays("text")).isEqualTo("text");
  }

  @Test
  void unwrapReplacesAtEveryDepthAndNarrowsLikeTheJsonParser() {
    final Map<String, Object> nested = new HashMap<>();
    nested.put("deep", List.of(Map.of("a", new long[] { 1L, 5_000_000_000L }), new double[] { 0.5 }));

    @SuppressWarnings("unchecked")
    final Map<String, Object> unwrapped = (Map<String, Object>) Type.unwrapNestedPrimitiveArrays(nested);

    assertThat(unwrapped).isNotSameAs(nested);
    assertThat(unwrapped.get("deep")).isEqualTo(List.of(Map.of("a", List.of(1, 5_000_000_000L)), List.of(0.5)));
    // the source is not touched
    assertThat(((List<?>) nested.get("deep")).get(1)).isInstanceOf(double[].class);
  }

  @Test
  void unwrapKeepsASetASet() {
    final Object unwrapped = Type.unwrapNestedPrimitiveArrays(new LinkedHashSet<Object>(List.of(new long[] { 1L })));
    assertThat(unwrapped).isInstanceOf(Set.class);
  }

  private void createType() {
    database.command("sql", "CREATE DOCUMENT TYPE D");
    database.command("sql", "CREATE PROPERTY D.m MAP");
    database.command("sql", "CREATE PROPERTY D.ll LIST");
    database.command("sql", "CREATE PROPERTY D.md MAP");
  }

  /** The three ways of writing the same values: the parameters as the HTTP handler parses them, a literal and the Java API. */
  @SuppressWarnings("unchecked")
  private void insertRows() {
    // PostCommandHandler parses the request body with toMap(true)
    final Map<String, Object> request = new JSONObject(REQUEST).toMap(true);
    final Map<String, Object> params = (Map<String, Object>) request.get("params");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO D SET k = 'param', m = :m, ll = :ll, md = :md", params).close();
      database.command("sql", "INSERT INTO D SET k = 'literal', m = {\"a\": [1, 2]}, ll = [[1, 2], [3]], md = {\"a\": [1.5, 2.5]}").close();
      database.newDocument("D").set("k", "java").set("m", Map.of("a", List.of(1, 2)))
          .set("ll", List.of(List.of(1, 2), List.of(3))).set("md", Map.of("a", List.of(1.5, 2.5))).save();
    });
  }

  private List<String> keys(final String sql) {
    final List<String> keys = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        keys.add(rs.next().getProperty("k"));
    }
    return keys;
  }
}
