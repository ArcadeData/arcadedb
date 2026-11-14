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

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11582: a numeric list nested in an HTTP parameter arrives as a {@code long[]} / {@code double[]}, and
 * {@code UNWIND $rows AS row CREATE (:T {l: row.l})} stores it as ARRAY_OF_LONGS / ARRAY_OF_DOUBLES. The list functions
 * that tested their argument with {@code instanceof List} / {@code instanceof Collection} refused such a value
 * ({@code isEmpty}, {@code toIntegerList}, {@code convert.toSet}, {@code map.fromLists}, ...) or read it as a scalar
 * ({@code convert.toJson} answered the Java identity string, {@code convert.toList} and {@code coll.flatten} wrapped it as
 * one element). Each function must answer for the array exactly what it answers for the equivalent list.
 */
class Issue11582ListFunctionsOverArraysTest extends TestHelper {
  private static final String[] EXPRESSIONS = { //
      "isEmpty($l)", //
      "toIntegerList($l)", //
      "toFloatList($l)", //
      "toStringList($l)", //
      "toBooleanList($l)", //
      "toString($l)", //
      "coll.flatten([$l, [7]])", //
      "apoc.coll.flatten([$l])", //
      "apoc.convert.toJson($l)", //
      "apoc.convert.toJson([$l])", //
      "apoc.convert.toList($l)", //
      "apoc.convert.toSet($l)", //
      "apoc.map.fromLists(['a', 'b', 'c'], $l)", //
      "apoc.map.fromLists($l, ['a', 'b', 'c'])", //
      "apoc.map.fromPairs([$l])", //
      "apoc.map.clean({a: 1, b: 5}, ['x'], $l)", //
      "apoc.map.removeKeys({a: 1, `3`: 5}, $l)", //
      "apoc.map.submap({a: 1, `3`: 5}, $l)", //
      "apoc.map.values({a: 1, `3`: 5}, $l)", //
      "apoc.text.join($l, '-')", //
      "apoc.util.validate(true, 'values %s %s %s', $l)" };

  static Stream<Arguments> cases() {
    final List<Arguments> cases = new ArrayList<>();
    for (final String expression : EXPRESSIONS) {
      cases.add(Arguments.of(expression, new long[] { 3L, 1L, 2L }, List.of(3L, 1L, 2L)));
      cases.add(Arguments.of(expression, new double[] { 3.0, 1.0, 2.0 }, List.of(3.0, 1.0, 2.0)));
      cases.add(Arguments.of(expression, new int[] { 3, 1, 2 }, List.of(3, 1, 2)));
    }
    return cases.stream();
  }

  @ParameterizedTest(name = "{0} over {1}")
  @MethodSource("cases")
  void listFunctionAnswersTheSameForAnArrayAsForTheEquivalentList(final String expression, final Object array, final List<?> list) {
    assertThat(evaluate(expression, array)).as("%s with $l = %s", expression, array.getClass().getSimpleName())
        .isEqualTo(evaluate(expression, list));
  }

  /** The value of the expression, or the class and message of the innermost exception when it fails. */
  private Object evaluate(final String expression, final Object value) {
    try (final ResultSet result = database.query("opencypher", "RETURN " + expression + " AS v", Map.of("l", value))) {
      return result.next().getProperty("v");
    } catch (final RuntimeException e) {
      Throwable cause = e;
      while (cause.getCause() != null && cause.getCause() != cause)
        cause = cause.getCause();
      // toString() names the Java class of what it refuses, which is not what this test is about
      final String message = expression.startsWith("toString(") ? "" : cause.getMessage();
      return "threw " + cause.getClass().getSimpleName() + ": " + message;
    }
  }
}
