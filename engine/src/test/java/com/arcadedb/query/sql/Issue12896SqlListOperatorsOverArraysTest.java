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
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12896: a list nested in a map parameter over HTTP ({@code INSERT ... CONTENT :map}, openCypher {@code SET n = $map},
 * {@code UNWIND $rows}) is stored as a primitive array, ARRAY_OF_LONGS / ARRAY_OF_DOUBLES. The SQL list operators read it as a
 * scalar: {@code SET l += 3} stored the string {@code "[J@...3"}, {@code SET l -= 1} wiped the property, {@code REMOVE l[0]} and
 * {@code REMOVE l = 2} failed, and {@code difference()} / {@code symmetricDifference()} took the whole array as one element.
 * Each must answer for the array what it answers for the same list.
 */
class Issue12896SqlListOperatorsOverArraysTest extends TestHelper {
  private static final String TYPE = "Doc12896";

  static Stream<Arguments> updates() {
    final List<Arguments> cases = new ArrayList<>();
    final Object[][] longCases = { //
        { "SET l += 3", List.of(1.0, 2.0, 3.0) }, //
        { "SET l = l + [3]", List.of(1.0, 2.0, 3.0) }, //
        { "SET l = l + 3", List.of(1.0, 2.0, 3.0) }, //
        { "SET l += [3, 4]", List.of(1.0, 2.0, 3.0, 4.0) }, //
        { "SET l -= 1", List.of(2.0) }, //
        { "SET l = l - 1", List.of(2.0) }, //
        { "REMOVE l[0]", List.of(2.0) }, //
        { "REMOVE l = 2", List.of(1.0) } };
    for (final Object[] c : longCases) {
      cases.add(Arguments.of(c[0], new long[] { 1L, 2L }, c[1]));
      cases.add(Arguments.of(c[0], new ArrayList<>(List.of(1L, 2L)), c[1]));
    }
    final Object[][] doubleCases = { //
        { "SET l += 3.5", List.of(1.5, 2.5, 3.5) }, //
        { "SET l = l + [3.5]", List.of(1.5, 2.5, 3.5) }, //
        { "SET l -= 1.5", List.of(2.5) }, //
        { "REMOVE l[1]", List.of(1.5) }, //
        { "REMOVE l = 2.5", List.of(1.5) } };
    for (final Object[] c : doubleCases) {
      cases.add(Arguments.of(c[0], new double[] { 1.5, 2.5 }, c[1]));
      cases.add(Arguments.of(c[0], new ArrayList<>(List.of(1.5, 2.5)), c[1]));
    }
    return cases.stream();
  }

  @ParameterizedTest(name = "{0} on {1}")
  @MethodSource("updates")
  void updateTreatsAnArrayAsTheListItIs(final String operation, final Object stored, final List<Double> expected) {
    database.getSchema().createDocumentType(TYPE);
    final RID rid = store("l", stored);

    database.transaction(() -> database.command("sql", "UPDATE " + rid + " " + operation).close());

    assertThat(numbers(reload(rid).get("l"))).as("%s on %s", operation, stored.getClass().getSimpleName()).containsExactlyElementsOf(expected);
  }

  @Test
  void removeReachesAnArrayNestedInAMap() {
    database.getSchema().createDocumentType(TYPE);
    final Map<String, Object> byIndex = new HashMap<>();
    byIndex.put("l", new long[] { 1L, 2L });
    final RID first = store("m", byIndex);
    final Map<String, Object> byValue = new HashMap<>();
    byValue.put("l", new long[] { 1L, 2L });
    final RID second = store("m", byValue);

    database.transaction(() -> {
      database.command("sql", "UPDATE " + first + " REMOVE m.l[0]").close();
      database.command("sql", "UPDATE " + second + " REMOVE m.l = 2").close();
    });

    assertThat(numbers(((Map<?, ?>) reload(first).get("m")).get("l"))).containsExactly(2.0);
    assertThat(numbers(((Map<?, ?>) reload(second).get("m")).get("l"))).containsExactly(1.0);
  }

  static Stream<Arguments> functions() {
    final List<Arguments> cases = new ArrayList<>();
    final Object[][] functionCases = { //
        { "difference(l, [2])", List.of(1.0) }, //
        { "difference([2, 3], l)", List.of(3.0) }, //
        { "symmetricDifference(l, [2, 3])", List.of(1.0, 3.0) }, //
        { "symmetricDifference([2, 3], l)", List.of(1.0, 3.0) } };
    for (final Object[] c : functionCases) {
      cases.add(Arguments.of(c[0], new long[] { 1L, 2L }, c[1]));
      cases.add(Arguments.of(c[0], new ArrayList<>(List.of(1L, 2L)), c[1]));
    }
    return cases.stream();
  }

  @ParameterizedTest(name = "{0} on {1}")
  @MethodSource("functions")
  void setFunctionsReadAnArrayAsItsItems(final String expression, final Object stored, final List<Double> expected) {
    database.getSchema().createDocumentType(TYPE);
    final RID rid = store("l", stored);

    try (final ResultSet rs = database.query("sql", "SELECT " + expression + " AS r FROM " + rid)) {
      assertThat(numbers(rs.next().getProperty("r"))).as("%s on %s", expression, stored.getClass().getSimpleName())
          .containsExactlyInAnyOrderElementsOf(expected);
    }
  }

  @Test
  void symmetricDifferenceAggregatesTheItemsOfArrays() {
    database.getSchema().createDocumentType(TYPE);
    store("l", new long[] { 1L, 2L });
    store("l", new long[] { 2L, 3L });

    try (final ResultSet rs = database.query("sql", "SELECT symmetricDifference(l) AS r FROM " + TYPE)) {
      assertThat(numbers(rs.next().getProperty("r"))).containsExactlyInAnyOrder(1.0, 3.0);
    }
  }

  @Test
  void stringPropertyStoresTheTextOfAnArrayAsOfTheSameList() {
    final DocumentType type = database.getSchema().createDocumentType(TYPE);
    type.createProperty("s", Type.STRING);

    assertThat(reload(store("s", new long[] { 1L, 2L })).get("s")).isEqualTo("[1, 2]");
    assertThat(reload(store("s", new double[] { 1.5, 2.5 })).get("s")).isEqualTo("[1.5, 2.5]");
    assertThat(reload(store("s", new ArrayList<>(List.of(1L, 2L)))).get("s")).isEqualTo("[1, 2]");
    // a byte[] is a BINARY value, not a sequence: its text would be its identity, so it is still refused
    assertThatThrownBy(() -> store("s", new byte[] { 1, 2 })).isInstanceOf(IllegalArgumentException.class);
  }

  private RID store(final String property, final Object value) {
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument(TYPE).set(property, value).save().getIdentity());
    return rid[0];
  }

  private Document reload(final RID rid) {
    return database.lookupByRID(rid, true).asDocument();
  }

  private static List<Double> numbers(final Object value) {
    assertThat(value).isInstanceOf(Collection.class);
    final List<Double> numbers = new ArrayList<>();
    for (final Object item : (Collection<?>) value)
      numbers.add(((Number) item).doubleValue());
    return numbers;
  }
}
