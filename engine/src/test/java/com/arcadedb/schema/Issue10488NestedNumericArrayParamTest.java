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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

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

  /**
   * The unwrapper replaces {@code long[]} and {@code double[]} and nothing else, because those are the only arrays the request
   * parser leaves in a value. This ties the two: a primitive array of any other kind showing up in a parsed request has to be
   * added to {@code Type.unwrapNestedPrimitiveArrays} too.
   */
  @Test
  void theRequestParserLeavesNoPrimitiveArrayTheUnwrapperDoesNotReplace() {
    final Map<String, Object> parsed = new JSONObject(
        "{\"a\":[1,2],\"b\":[1.5],\"c\":[1,2.5],\"d\":[[1],[2.5],[\"x\"]],\"e\":[\"x\"],\"f\":[true],\"g\":[1e3],"
            + "\"h\":[12345678901234567890],\"i\":{\"j\":[[1,2],[3]],\"k\":[0.5,0.25]},\"l\":[],\"m\":[null,1],\"n\":[[]]}")
        .toMap(true);

    assertThat(Type.unwrapNestedPrimitiveArrays(parsed)).isNotNull();
    assertThat(primitiveArraysIn(Type.unwrapNestedPrimitiveArrays(parsed))).as("arrays left after the unwrap").isEmpty();
    assertThat(primitiveArraysIn(parsed)).as("arrays the parser leaves").allMatch(
        array -> array == long[].class || array == double[].class);
  }

  private static List<Class<?>> primitiveArraysIn(final Object value) {
    final List<Class<?>> found = new ArrayList<>();
    if (value != null && value.getClass().isArray())
      found.add(value.getClass());
    else if (value instanceof Map<?, ?> map)
      for (final Object item : map.values())
        found.addAll(primitiveArraysIn(item));
    else if (value instanceof List<?> list)
      for (final Object item : list)
        found.addAll(primitiveArraysIn(item));
    return found;
  }

  /** A list holding an integer beyond the int range: the elements are an Integer and a Long, as the literal's are. */
  @Test
  void aNestedListWithALongBeyondTheIntRangeEqualsTheLiteralAndGroupsWithIt() {
    database.command("sql", "CREATE DOCUMENT TYPE Big");
    final Map<String, Object> request = new JSONObject("{\"params\":{\"m\":{\"a\":[5000000000,1]}}}").toMap(true);
    @SuppressWarnings("unchecked")
    final Map<String, Object> params = (Map<String, Object>) request.get("params");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Big SET k = 'param', m = :m", params).close();
      database.command("sql", "INSERT INTO Big SET k = 'literal', m = {\"a\": [5000000000, 1]}").close();
    });

    assertThat(keys("SELECT k FROM Big WHERE m.a = [5000000000, 1] ORDER BY k")).containsExactly("literal", "param");
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Big GROUP BY m")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(2L);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /** Every way a map or a list reaches a record funnels through the one place that unwraps: the setters, the map and JSON loads, MERGE and CONTENT. */
  @Test
  void everyPathThatAssignsAPropertyStoresTheNestedListAsAList() {
    database.command("sql", "CREATE DOCUMENT TYPE Paths");
    final Map<String, Object> nested = new HashMap<>();
    nested.put("a", new long[] { 1L, 2L });

    final Map<String, Object> contentRow = new HashMap<>();
    contentRow.put("k", "content");
    contentRow.put("m", nested);
    final Map<String, Object> mergeParams = new HashMap<>();
    mergeParams.put("m", nested);

    database.transaction(() -> {
      database.newDocument("Paths").set("k", "set").set("m", nested).save();
      database.newDocument("Paths").set(Map.of("k", "setMap", "m", nested)).save();
      database.newDocument("Paths").fromMap(contentRow).set("k", "fromMap").save();
      database.newDocument("Paths").set("k", "merge").save();
      database.command("sql", "INSERT INTO Paths CONTENT :row", Map.of("row", contentRow)).close();
      database.command("sql", "UPDATE Paths MERGE {\"m\": {\"a\": [1, 2]}} WHERE k = 'merge'").close();
    });
    database.transaction(() -> {
      database.command("sql", "UPDATE Paths SET m = :m WHERE k = 'merge'", mergeParams).close();
      database.command("sql", "UPDATE Paths CONTENT :row WHERE k = 'setMap'", Map.of("row", Map.of("k", "setMap", "m", nested))).close();
    });

    assertThat(keys("SELECT k FROM Paths WHERE m.a.type() = 'LIST' ORDER BY k")).containsExactly("content", "fromMap", "merge", "set",
        "setMap");
    assertThat(keys("SELECT k FROM Paths WHERE m.a = [1, 2] ORDER BY k")).containsExactly("content", "fromMap", "merge", "set", "setMap");
  }

  /**
   * What a container becomes when it holds a replacement is documented here: a sorted or linked input is copied to the plain
   * family of its kind, and one that holds no array is returned as it came, whatever its type.
   */
  @Test
  void aContainerKeepsItsTypeUnlessItHoldsAReplacement() {
    final TreeMap<String, Object> sortedPlain = new TreeMap<>(Map.of("a", 1));
    final LinkedList<Object> linkedPlain = new LinkedList<>(List.of(1, 2));
    assertThat(Type.unwrapNestedPrimitiveArrays(sortedPlain)).isSameAs(sortedPlain);
    assertThat(Type.unwrapNestedPrimitiveArrays(linkedPlain)).isSameAs(linkedPlain);

    final TreeMap<String, Object> sortedWithArray = new TreeMap<>();
    sortedWithArray.put("a", new long[] { 1L });
    final Object unwrappedMap = Type.unwrapNestedPrimitiveArrays(sortedWithArray);
    assertThat(unwrappedMap).isInstanceOf(LinkedHashMap.class).isEqualTo(Map.of("a", List.of(1)));

    final LinkedList<Object> linkedWithArray = new LinkedList<>();
    linkedWithArray.add(new double[] { 0.5 });
    assertThat(Type.unwrapNestedPrimitiveArrays(linkedWithArray)).isInstanceOf(ArrayList.class).isEqualTo(List.of(List.of(0.5)));

    final TreeSet<Object> sortedSet = new TreeSet<>(List.of("b", "a"));
    assertThat(Type.unwrapNestedPrimitiveArrays(sortedSet)).isSameAs(sortedSet);
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
