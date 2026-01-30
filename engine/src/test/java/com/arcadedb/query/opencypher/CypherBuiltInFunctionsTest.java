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
import com.arcadedb.function.Function;
import com.arcadedb.function.FunctionRegistry;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.procedure.Procedure;
import com.arcadedb.function.procedure.ProcedureRegistry;
import com.arcadedb.query.opencypher.functions.CypherFunctionRegistry;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.opencypher.procedures.CypherProcedureRegistry;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Tests for Cypher built-in functions and procedures.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
class CypherBuiltInFunctionsTest extends TestHelper {

  // ===================== REGISTRY TESTS =====================

  @Test
  void apocPrefixCompatibilityForFunctions() {
    // Test that functions can be accessed with "apoc." prefix
    assertThat(CypherFunctionRegistry.hasFunction("apoc.text.indexOf")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("apoc.map.merge")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("apoc.math.sigmoid")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("apoc.convert.toJson")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("apoc.date.currentTimestamp")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("apoc.util.md5")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("apoc.agg.median")).isTrue();

    // Verify same function is returned with or without prefix
    assertThat(CypherFunctionRegistry.get("apoc.text.indexOf")).isSameAs(CypherFunctionRegistry.get("text.indexOf"));
    assertThat(CypherFunctionRegistry.get("apoc.map.merge")).isSameAs(CypherFunctionRegistry.get("map.merge"));

    // Test case insensitivity
    assertThat(CypherFunctionRegistry.get("APOC.TEXT.INDEXOF")).isSameAs(CypherFunctionRegistry.get("TEXT.INDEXOF"));
  }

  @Test
  void apocPrefixCompatibilityForProcedures() {
    // Test that procedures can be accessed with "apoc." prefix
    assertThat(CypherProcedureRegistry.hasProcedure("apoc.merge.relationship")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("apoc.merge.node")).isTrue();

    // Verify same procedure is returned with or without prefix
    assertThat(CypherProcedureRegistry.get("apoc.merge.relationship")).isSameAs(CypherProcedureRegistry.get("merge.relationship"));
    assertThat(CypherProcedureRegistry.get("apoc.merge.node")).isSameAs(CypherProcedureRegistry.get("merge.node"));

    // Test case insensitivity
    assertThat(CypherProcedureRegistry.get("APOC.MERGE.RELATIONSHIP")).isSameAs(CypherProcedureRegistry.get("MERGE.RELATIONSHIP"));
  }

  @Test
  void apocPrefixFunctionExecution() {
    // Test that functions accessed via apoc prefix work correctly
    final StatelessFunction fn = CypherFunctionRegistry.get("apoc.text.indexOf");
    assertThat(fn).isNotNull();
    assertThat(fn.execute(new Object[]{"hello", "h"}, null)).isEqualTo(0L);

    final StatelessFunction md5 = CypherFunctionRegistry.get("apoc.util.md5");
    assertThat(md5).isNotNull();
    assertThat(md5.execute(new Object[]{"hello"}, null)).isEqualTo("5d41402abc4b2a76b9719d911017c592");
  }

  @Test
  void functionRegistryHasTextFunctions() {
    assertThat(CypherFunctionRegistry.hasFunction("text.indexOf")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("text.join")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("text.split")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("text.capitalize")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("text.camelCase")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("text.levenshteinDistance")).isTrue();
  }

  // ===================== UNIFIED REGISTRY TESTS =====================

  @Test
  void unifiedFunctionRegistryHasCypherFunctions() {
    // Verify Cypher functions are registered in unified FunctionRegistry
    assertThat(FunctionRegistry.hasFunction("text.indexOf")).isTrue();
    assertThat(FunctionRegistry.hasFunction("map.merge")).isTrue();
    assertThat(FunctionRegistry.hasFunction("math.sigmoid")).isTrue();
    assertThat(FunctionRegistry.hasFunction("convert.toJson")).isTrue();
    assertThat(FunctionRegistry.hasFunction("date.currentTimestamp")).isTrue();
    assertThat(FunctionRegistry.hasFunction("util.md5")).isTrue();
    assertThat(FunctionRegistry.hasFunction("agg.median")).isTrue();

    // Verify APOC prefix works with unified registry
    assertThat(FunctionRegistry.hasFunction("apoc.text.indexOf")).isTrue();
    assertThat(FunctionRegistry.hasFunction("APOC.MAP.MERGE")).isTrue();
  }

  @Test
  void unifiedFunctionRegistryReturnsStatelessFunction() {
    // Verify functions from unified registry are StatelessFunction
    Function fn = FunctionRegistry.get("text.indexOf");
    assertThat(fn).isNotNull();
    assertThat(fn).isInstanceOf(StatelessFunction.class);

    // Verify stateless function can be executed
    StatelessFunction sf = FunctionRegistry.getStateless("text.indexOf");
    assertThat(sf).isNotNull();
    assertThat(sf.execute(new Object[]{"hello", "h"}, null)).isEqualTo(0L);
  }

  @Test
  void unifiedProcedureRegistryHasCypherProcedures() {
    // Verify Cypher procedures are registered in unified ProcedureRegistry
    assertThat(ProcedureRegistry.hasProcedure("merge.relationship")).isTrue();
    assertThat(ProcedureRegistry.hasProcedure("merge.node")).isTrue();
    assertThat(ProcedureRegistry.hasProcedure("algo.dijkstra")).isTrue();

    // Verify APOC prefix works with unified registry
    assertThat(ProcedureRegistry.hasProcedure("apoc.merge.relationship")).isTrue();
    assertThat(ProcedureRegistry.hasProcedure("APOC.MERGE.NODE")).isTrue();
  }

  @Test
  void unifiedProcedureRegistryReturnsProcedure() {
    // Verify procedures from unified registry implement Procedure interface
    Procedure proc = ProcedureRegistry.get("merge.relationship");
    assertThat(proc).isNotNull();
    assertThat(proc.getName()).isEqualTo("merge.relationship");
    assertThat(proc.isWriteProcedure()).isTrue();
    assertThat(proc.getYieldFields()).isEqualTo(List.of("rel"));
  }

  @Test
  void cypherFunctionInstancesAreSameInBothRegistries() {
    // Verify same instance is returned from both registries
    StatelessFunction cypherFn = CypherFunctionRegistry.get("text.indexOf");
    Function unifiedFn = FunctionRegistry.get("text.indexOf");
    assertThat(unifiedFn).isSameAs(cypherFn);

    CypherProcedure cypherProc = CypherProcedureRegistry.get("merge.relationship");
    Procedure unifiedProc = ProcedureRegistry.get("merge.relationship");
    assertThat(unifiedProc).isSameAs(cypherProc);
  }

  @Test
  void functionRegistryHasMapFunctions() {
    assertThat(CypherFunctionRegistry.hasFunction("map.merge")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("map.fromLists")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("map.setKey")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("map.flatten")).isTrue();
  }

  @Test
  void functionRegistryHasMathFunctions() {
    assertThat(CypherFunctionRegistry.hasFunction("math.sigmoid")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("math.tanh")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("math.maxLong")).isTrue();
  }

  @Test
  void functionRegistryHasConvertFunctions() {
    assertThat(CypherFunctionRegistry.hasFunction("convert.toJson")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("convert.fromJsonMap")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("convert.toList")).isTrue();
  }

  @Test
  void procedureRegistryHasMergeProcedures() {
    assertThat(CypherProcedureRegistry.hasProcedure("merge.relationship")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("merge.node")).isTrue();
  }

  // ===================== TEXT FUNCTION TESTS =====================

  @Test
  void textIndexOf() {
    final StatelessFunction fn = CypherFunctionRegistry.get("text.indexOf");
    assertThat(fn.execute(new Object[]{"hello", "h"}, null)).isEqualTo(0L);
    assertThat(fn.execute(new Object[]{"hello", "l"}, null)).isEqualTo(2L);
    assertThat(fn.execute(new Object[]{"hello", "x"}, null)).isEqualTo(-1L);
    assertThat(fn.execute(new Object[]{"hello", "l", 3}, null)).isEqualTo(3L);
  }

  @Test
  void textJoin() {
    final StatelessFunction fn = CypherFunctionRegistry.get("text.join");
    assertThat(fn.execute(new Object[]{List.of("a", "b", "c"), ","}, null)).isEqualTo("a,b,c");
    assertThat(fn.execute(new Object[]{List.of("a", "b", "c"), ""}, null)).isEqualTo("abc");
  }

  @Test
  void textSplit() {
    final StatelessFunction fn = CypherFunctionRegistry.get("text.split");
    assertThat(fn.execute(new Object[]{"a,b,c", ","}, null)).isEqualTo(List.of("a", "b", "c"));
  }

  @Test
  void textReplace() {
    final StatelessFunction fn = CypherFunctionRegistry.get("text.replace");
    assertThat(fn.execute(new Object[]{"hello", "e", "X"}, null)).isEqualTo("hXllo");
    assertThat(fn.execute(new Object[]{"hello world", " ", ""}, null)).isEqualTo("helloworld");
  }

  @Test
  void textRegexReplace() {
    final StatelessFunction fn = CypherFunctionRegistry.get("text.regexReplace");
    assertThat(fn.execute(new Object[]{"hello", "[aeiou]", "X"}, null)).isEqualTo("hXllX");
  }

  @Test
  void textCapitalize() {
    final StatelessFunction fn = CypherFunctionRegistry.get("text.capitalize");
    assertThat(fn.execute(new Object[]{"hello"}, null)).isEqualTo("Hello");
    assertThat(fn.execute(new Object[]{"hello world"}, null)).isEqualTo("Hello world");
  }

  @Test
  void textCapitalizeAll() {
    final StatelessFunction fn = CypherFunctionRegistry.get("text.capitalizeAll");
    assertThat(fn.execute(new Object[]{"hello world"}, null)).isEqualTo("Hello World");
  }

  @Test
  void textCamelCase() {
    final StatelessFunction fn = CypherFunctionRegistry.get("text.camelCase");
    assertThat(fn.execute(new Object[]{"hello world"}, null)).isEqualTo("helloWorld");
    assertThat(fn.execute(new Object[]{"hello_world"}, null)).isEqualTo("helloWorld");
  }

  @Test
  void textSnakeCase() {
    final StatelessFunction fn = CypherFunctionRegistry.get("text.snakeCase");
    assertThat(fn.execute(new Object[]{"helloWorld"}, null)).isEqualTo("hello_world");
    assertThat(fn.execute(new Object[]{"hello world"}, null)).isEqualTo("hello_world");
  }

  @Test
  void textUpperCamelCase() {
    final StatelessFunction fn = CypherFunctionRegistry.get("text.upperCamelCase");
    assertThat(fn.execute(new Object[]{"hello world"}, null)).isEqualTo("HelloWorld");
  }

  @Test
  void textLpad() {
    final StatelessFunction fn = CypherFunctionRegistry.get("text.lpad");
    assertThat(fn.execute(new Object[]{"42", 5, "0"}, null)).isEqualTo("00042");
    assertThat(fn.execute(new Object[]{"42", 2, "0"}, null)).isEqualTo("42");
  }

  @Test
  void textRpad() {
    final StatelessFunction fn = CypherFunctionRegistry.get("text.rpad");
    assertThat(fn.execute(new Object[]{"42", 5, "0"}, null)).isEqualTo("42000");
  }

  @Test
  void textSlug() {
    final StatelessFunction fn = CypherFunctionRegistry.get("text.slug");
    assertThat(fn.execute(new Object[]{"Hello World!"}, null)).isEqualTo("hello-world");
    assertThat(fn.execute(new Object[]{"Hello World!", "_"}, null)).isEqualTo("hello_world");
  }

  @Test
  void textLevenshteinDistance() {
    final StatelessFunction fn = CypherFunctionRegistry.get("text.levenshteinDistance");
    assertThat(fn.execute(new Object[]{"hello", "hello"}, null)).isEqualTo(0L);
    assertThat(fn.execute(new Object[]{"hello", "hallo"}, null)).isEqualTo(1L);
    assertThat(fn.execute(new Object[]{"kitten", "sitting"}, null)).isEqualTo(3L);
  }

  @Test
  void textLevenshteinSimilarity() {
    final StatelessFunction fn = CypherFunctionRegistry.get("text.levenshteinSimilarity");
    assertThat(fn.execute(new Object[]{"hello", "hello"}, null)).isEqualTo(1.0);
    // hello vs hallo has distance 1, max length 5, so similarity = 1 - 1/5 = 0.8
    assertThat((Double) fn.execute(new Object[]{"hello", "hallo"}, null)).isCloseTo(0.8, within(0.001));
  }

  @Test
  void textHammingDistance() {
    final StatelessFunction fn = CypherFunctionRegistry.get("text.hammingDistance");
    assertThat(fn.execute(new Object[]{"hello", "hello"}, null)).isEqualTo(0L);
    assertThat(fn.execute(new Object[]{"hello", "hallo"}, null)).isEqualTo(1L);
  }

  @Test
  void textCharAt() {
    final StatelessFunction fn = CypherFunctionRegistry.get("text.charAt");
    assertThat(fn.execute(new Object[]{"hello", 0}, null)).isEqualTo("h");
    assertThat(fn.execute(new Object[]{"hello", 4}, null)).isEqualTo("o");
    assertThat(fn.execute(new Object[]{"hello", 10}, null)).isNull();
  }

  @Test
  void textCode() {
    final StatelessFunction fn = CypherFunctionRegistry.get("text.code");
    assertThat(fn.execute(new Object[]{"A"}, null)).isEqualTo(65L);
    assertThat(fn.execute(new Object[]{"a"}, null)).isEqualTo(97L);
  }

  // ===================== MAP FUNCTION TESTS =====================

  @Test
  void mapMerge() {
    final StatelessFunction fn = CypherFunctionRegistry.get("map.merge");
    @SuppressWarnings("unchecked")
    final Map<String, Object> result = (Map<String, Object>) fn.execute(
        new Object[]{Map.of("a", 1), Map.of("b", 2)}, null);
    assertThat(result.get("a")).isEqualTo(1);
    assertThat(result.get("b")).isEqualTo(2);
  }

  @Test
  void mapMergeOverride() {
    final StatelessFunction fn = CypherFunctionRegistry.get("map.merge");
    @SuppressWarnings("unchecked")
    final Map<String, Object> result = (Map<String, Object>) fn.execute(
        new Object[]{Map.of("a", 1), Map.of("a", 2)}, null);
    assertThat(result.get("a")).isEqualTo(2);
  }

  @Test
  void mapFromLists() {
    final StatelessFunction fn = CypherFunctionRegistry.get("map.fromLists");
    @SuppressWarnings("unchecked")
    final Map<String, Object> result = (Map<String, Object>) fn.execute(
        new Object[]{List.of("a", "b"), List.of(1, 2)}, null);
    assertThat(result.get("a")).isEqualTo(1);
    assertThat(result.get("b")).isEqualTo(2);
  }

  @Test
  void mapFromPairs() {
    final StatelessFunction fn = CypherFunctionRegistry.get("map.fromPairs");
    @SuppressWarnings("unchecked")
    final Map<String, Object> result = (Map<String, Object>) fn.execute(
        new Object[]{List.of(List.of("a", 1), List.of("b", 2))}, null);
    assertThat(result.get("a")).isEqualTo(1);
    assertThat(result.get("b")).isEqualTo(2);
  }

  @Test
  void mapSetKey() {
    final StatelessFunction fn = CypherFunctionRegistry.get("map.setKey");
    @SuppressWarnings("unchecked")
    final Map<String, Object> result = (Map<String, Object>) fn.execute(
        new Object[]{Map.of("a", 1), "b", 2}, null);
    assertThat(result.get("a")).isEqualTo(1);
    assertThat(result.get("b")).isEqualTo(2);
  }

  @Test
  void mapRemoveKey() {
    final StatelessFunction fn = CypherFunctionRegistry.get("map.removeKey");
    @SuppressWarnings("unchecked")
    final Map<String, Object> result = (Map<String, Object>) fn.execute(
        new Object[]{Map.of("a", 1, "b", 2), "a"}, null);
    assertThat(result).doesNotContainKey("a");
    assertThat(result.get("b")).isEqualTo(2);
  }

  @Test
  void mapFlatten() {
    final StatelessFunction fn = CypherFunctionRegistry.get("map.flatten");
    @SuppressWarnings("unchecked")
    final Map<String, Object> result = (Map<String, Object>) fn.execute(
        new Object[]{Map.of("a", Map.of("b", 1)), "."}, null);
    assertThat(result.get("a.b")).isEqualTo(1);
  }

  @Test
  void mapSubmap() {
    final StatelessFunction fn = CypherFunctionRegistry.get("map.submap");
    @SuppressWarnings("unchecked")
    final Map<String, Object> result = (Map<String, Object>) fn.execute(
        new Object[]{Map.of("a", 1, "b", 2, "c", 3), List.of("a", "c")}, null);
    assertThat(result).hasSize(2);
    assertThat(result.get("a")).isEqualTo(1);
    assertThat(result.get("c")).isEqualTo(3);
    assertThat(result).doesNotContainKey("b");
  }

  // ===================== MATH FUNCTION TESTS =====================

  @Test
  void mathSigmoid() {
    final StatelessFunction fn = CypherFunctionRegistry.get("math.sigmoid");
    assertThat(fn.execute(new Object[]{0.0}, null)).isEqualTo(0.5);
    assertThat((Double) fn.execute(new Object[]{10.0}, null)).isGreaterThan(0.99);
    assertThat((Double) fn.execute(new Object[]{-10.0}, null)).isLessThan(0.01);
  }

  @Test
  void mathTanh() {
    final StatelessFunction fn = CypherFunctionRegistry.get("math.tanh");
    assertThat((Double) fn.execute(new Object[]{0.0}, null)).isCloseTo(0.0, within(0.001));
  }

  @Test
  void mathMaxLong() {
    final StatelessFunction fn = CypherFunctionRegistry.get("math.maxLong");
    assertThat(fn.execute(new Object[]{}, null)).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void mathMinLong() {
    final StatelessFunction fn = CypherFunctionRegistry.get("math.minLong");
    assertThat(fn.execute(new Object[]{}, null)).isEqualTo(Long.MIN_VALUE);
  }

  @Test
  void mathMaxDouble() {
    final StatelessFunction fn = CypherFunctionRegistry.get("math.maxDouble");
    assertThat(fn.execute(new Object[]{}, null)).isEqualTo(Double.MAX_VALUE);
  }

  // ===================== CONVERT FUNCTION TESTS =====================

  @Test
  void convertToJson() {
    final StatelessFunction fn = CypherFunctionRegistry.get("convert.toJson");
    assertThat(fn.execute(new Object[]{Map.of("a", 1)}, null)).isEqualTo("{\"a\":1}");
    assertThat(fn.execute(new Object[]{List.of(1, 2, 3)}, null)).isEqualTo("[1,2,3]");
  }

  @Test
  void convertFromJsonMap() {
    final StatelessFunction fn = CypherFunctionRegistry.get("convert.fromJsonMap");
    @SuppressWarnings("unchecked")
    final Map<String, Object> result = (Map<String, Object>) fn.execute(
        new Object[]{"{\"a\":1}"}, null);
    assertThat(((Number) result.get("a")).intValue()).isEqualTo(1);
  }

  @Test
  void convertFromJsonList() {
    final StatelessFunction fn = CypherFunctionRegistry.get("convert.fromJsonList");
    @SuppressWarnings("unchecked")
    final List<Object> result = (List<Object>) fn.execute(new Object[]{"[1,2,3]"}, null);
    assertThat(result).hasSize(3);
    assertThat(((Number) result.get(0)).intValue()).isEqualTo(1);
  }

  @Test
  void convertToList() {
    final StatelessFunction fn = CypherFunctionRegistry.get("convert.toList");
    @SuppressWarnings("unchecked")
    final List<Object> result = (List<Object>) fn.execute(new Object[]{42}, null);
    assertThat(result).hasSize(1);
    assertThat(result.get(0)).isEqualTo(42);
  }

  @Test
  void convertToSet() {
    final StatelessFunction fn = CypherFunctionRegistry.get("convert.toSet");
    @SuppressWarnings("unchecked")
    final List<Object> result = (List<Object>) fn.execute(
        new Object[]{List.of(1, 2, 2, 3, 3, 3)}, null);
    assertThat(result).hasSize(3);
  }

  @Test
  void convertToBoolean() {
    final StatelessFunction fn = CypherFunctionRegistry.get("convert.toBoolean");
    assertThat(fn.execute(new Object[]{"true"}, null)).isEqualTo(true);
    assertThat(fn.execute(new Object[]{"false"}, null)).isEqualTo(false);
    assertThat(fn.execute(new Object[]{1}, null)).isEqualTo(true);
    assertThat(fn.execute(new Object[]{0}, null)).isEqualTo(false);
  }

  @Test
  void convertToInteger() {
    final StatelessFunction fn = CypherFunctionRegistry.get("convert.toInteger");
    assertThat(fn.execute(new Object[]{"42"}, null)).isEqualTo(42L);
    assertThat(fn.execute(new Object[]{42.9}, null)).isEqualTo(42L);
    assertThat(fn.execute(new Object[]{true}, null)).isEqualTo(1L);
  }

  @Test
  void convertToFloat() {
    final StatelessFunction fn = CypherFunctionRegistry.get("convert.toFloat");
    assertThat(fn.execute(new Object[]{"42"}, null)).isEqualTo(42.0);
    assertThat(fn.execute(new Object[]{"42.5"}, null)).isEqualTo(42.5);
  }

  // ===================== DATE FUNCTION TESTS =====================

  @Test
  void functionRegistryHasDateFunctions() {
    assertThat(CypherFunctionRegistry.hasFunction("date.format")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("date.parse")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("date.add")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("date.convert")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("date.field")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("date.fields")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("date.currentTimestamp")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("date.toISO8601")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("date.fromISO8601")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("date.systemTimezone")).isTrue();
  }

  @Test
  void dateCurrentTimestamp() {
    final StatelessFunction fn = CypherFunctionRegistry.get("date.currentTimestamp");
    final long before = System.currentTimeMillis();
    final long result = (Long) fn.execute(new Object[]{}, null);
    final long after = System.currentTimeMillis();
    assertThat(result).isBetween(before, after);
  }

  @Test
  void dateSystemTimezone() {
    final StatelessFunction fn = CypherFunctionRegistry.get("date.systemTimezone");
    final String result = (String) fn.execute(new Object[]{}, null);
    assertThat(result).isNotNull().isNotEmpty();
  }

  @Test
  void dateFormat() {
    final StatelessFunction fn = CypherFunctionRegistry.get("date.format");
    // Test with a known timestamp (2024-01-15T10:30:00 UTC = 1705314600000)
    final String result = (String) fn.execute(new Object[]{1705314600000L, "ms", "yyyy-MM-dd"}, null);
    assertThat(result).isNotNull().matches("\\d{4}-\\d{2}-\\d{2}");
  }

  @Test
  void dateField() {
    final StatelessFunction fn = CypherFunctionRegistry.get("date.field");
    // Test extracting year from a timestamp
    final Long year = (Long) fn.execute(new Object[]{1705314600000L, "year"}, null);
    assertThat(year).isEqualTo(2024L);
  }

  @Test
  void dateConvert() {
    final StatelessFunction fn = CypherFunctionRegistry.get("date.convert");
    // Convert 1000 ms to seconds
    final Long result = (Long) fn.execute(new Object[]{1000L, "ms", "s"}, null);
    assertThat(result).isEqualTo(1L);

    // Convert 60 seconds to minutes
    final Long result2 = (Long) fn.execute(new Object[]{60L, "s", "m"}, null);
    assertThat(result2).isEqualTo(1L);
  }

  @Test
  void dateAdd() {
    final StatelessFunction fn = CypherFunctionRegistry.get("date.add");
    // Add 1 day (86400000 ms) to a timestamp
    final long timestamp = 1705314600000L;
    final Long result = (Long) fn.execute(new Object[]{timestamp, 1L, "d"}, null);
    assertThat(result).isEqualTo(timestamp + 86400000L);
  }

  // ===================== UTIL FUNCTION TESTS =====================

  @Test
  void functionRegistryHasUtilFunctions() {
    assertThat(CypherFunctionRegistry.hasFunction("util.md5")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("util.sha1")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("util.sha256")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("util.sha512")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("util.compress")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("util.decompress")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("util.sleep")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("util.validate")).isTrue();
  }

  @Test
  void utilMd5() {
    final StatelessFunction fn = CypherFunctionRegistry.get("util.md5");
    final String result = (String) fn.execute(new Object[]{"hello"}, null);
    assertThat(result).isEqualTo("5d41402abc4b2a76b9719d911017c592");
  }

  @Test
  void utilSha1() {
    final StatelessFunction fn = CypherFunctionRegistry.get("util.sha1");
    final String result = (String) fn.execute(new Object[]{"hello"}, null);
    assertThat(result).isEqualTo("aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d");
  }

  @Test
  void utilSha256() {
    final StatelessFunction fn = CypherFunctionRegistry.get("util.sha256");
    final String result = (String) fn.execute(new Object[]{"hello"}, null);
    assertThat(result).isEqualTo("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824");
  }

  @Test
  void utilSha512() {
    final StatelessFunction fn = CypherFunctionRegistry.get("util.sha512");
    final String result = (String) fn.execute(new Object[]{"hello"}, null);
    assertThat(result).isEqualTo("9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043");
  }

  @Test
  void utilCompressDecompress() {
    final StatelessFunction compress = CypherFunctionRegistry.get("util.compress");
    final StatelessFunction decompress = CypherFunctionRegistry.get("util.decompress");

    final String original = "Hello, World! This is a test string for compression.";
    final String compressed = (String) compress.execute(new Object[]{original, "gzip"}, null);
    assertThat(compressed).isNotNull();

    final String decompressed = (String) decompress.execute(new Object[]{compressed, "gzip"}, null);
    assertThat(decompressed).isEqualTo(original);
  }

  @Test
  void utilValidateSuccess() {
    final StatelessFunction fn = CypherFunctionRegistry.get("util.validate");
    final Boolean result = (Boolean) fn.execute(new Object[]{true, "Should not throw"}, null);
    assertThat(result).isTrue();
  }

  @Test
  void utilValidateFailure() {
    final StatelessFunction fn = CypherFunctionRegistry.get("util.validate");
    assertThatThrownBy(() -> fn.execute(new Object[]{false, "Validation failed!"}, null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // ===================== AGG FUNCTION TESTS =====================

  @Test
  void functionRegistryHasAggFunctions() {
    assertThat(CypherFunctionRegistry.hasFunction("agg.first")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("agg.last")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("agg.nth")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("agg.slice")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("agg.median")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("agg.percentiles")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("agg.statistics")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("agg.product")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("agg.minItems")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("agg.maxItems")).isTrue();
  }

  @Test
  void aggFirst() {
    final StatelessFunction fn = CypherFunctionRegistry.get("agg.first");
    assertThat(fn.execute(new Object[]{List.of("a", "b", "c")}, null)).isEqualTo("a");
    assertThat(fn.execute(new Object[]{Arrays.asList(null, "b", "c")}, null)).isEqualTo("b");
  }

  @Test
  void aggLast() {
    final StatelessFunction fn = CypherFunctionRegistry.get("agg.last");
    assertThat(fn.execute(new Object[]{List.of("a", "b", "c")}, null)).isEqualTo("c");
  }

  @Test
  void aggNth() {
    final StatelessFunction fn = CypherFunctionRegistry.get("agg.nth");
    assertThat(fn.execute(new Object[]{List.of("a", "b", "c"), 1}, null)).isEqualTo("b");
    assertThat(fn.execute(new Object[]{List.of("a", "b", "c"), 10}, null)).isNull();
  }

  @Test
  void aggSlice() {
    final StatelessFunction fn = CypherFunctionRegistry.get("agg.slice");
    @SuppressWarnings("unchecked")
    final List<Object> result = (List<Object>) fn.execute(new Object[]{List.of(1, 2, 3, 4, 5), 1, 4}, null);
    assertThat(result).isEqualTo(List.of(2, 3, 4));
  }

  @Test
  void aggMedian() {
    final StatelessFunction fn = CypherFunctionRegistry.get("agg.median");
    // Odd number of elements
    assertThat(fn.execute(new Object[]{List.of(1, 2, 3, 4, 5)}, null)).isEqualTo(3.0);
    // Even number of elements
    assertThat(fn.execute(new Object[]{List.of(1, 2, 3, 4)}, null)).isEqualTo(2.5);
  }

  @Test
  void aggPercentiles() {
    final StatelessFunction fn = CypherFunctionRegistry.get("agg.percentiles");
    @SuppressWarnings("unchecked")
    final List<Double> result = (List<Double>) fn.execute(
        new Object[]{List.of(1.0, 2.0, 3.0, 4.0, 5.0), List.of(0.5)}, null);
    assertThat(result).hasSize(1);
    assertThat(result.getFirst()).isCloseTo(3.0, within(0.001));
  }

  @Test
  void aggStatistics() {
    final StatelessFunction fn = CypherFunctionRegistry.get("agg.statistics");
    @SuppressWarnings("unchecked")
    final Map<String, Object> result = (Map<String, Object>) fn.execute(
        new Object[]{List.of(1.0, 2.0, 3.0, 4.0, 5.0)}, null);

    assertThat(result.get("count")).isEqualTo(5L);
    assertThat(result.get("min")).isEqualTo(1.0);
    assertThat(result.get("max")).isEqualTo(5.0);
    assertThat(result.get("sum")).isEqualTo(15.0);
    assertThat(result.get("mean")).isEqualTo(3.0);
  }

  @Test
  void aggProduct() {
    final StatelessFunction fn = CypherFunctionRegistry.get("agg.product");
    assertThat(fn.execute(new Object[]{List.of(1.0, 2.0, 3.0, 4.0, 5.0)}, null)).isEqualTo(120.0);
  }

  @Test
  void aggMinItems() {
    final StatelessFunction fn = CypherFunctionRegistry.get("agg.minItems");
    @SuppressWarnings("unchecked")
    final Map<String, Object> result = (Map<String, Object>) fn.execute(
        new Object[]{List.of(3.0, 1.0, 2.0, 1.0, 4.0), List.of("c", "a", "b", "d", "e")}, null);

    assertThat(result.get("value")).isEqualTo(1.0);
    @SuppressWarnings("unchecked")
    final List<Object> items = (List<Object>) result.get("items");
    assertThat(items).hasSize(2).contains("a", "d");
  }

  @Test
  void aggMaxItems() {
    final StatelessFunction fn = CypherFunctionRegistry.get("agg.maxItems");
    @SuppressWarnings("unchecked")
    final Map<String, Object> result = (Map<String, Object>) fn.execute(
        new Object[]{List.of(3.0, 1.0, 5.0, 2.0, 5.0), List.of("a", "b", "c", "d", "e")}, null);

    assertThat(result.get("value")).isEqualTo(5.0);
    @SuppressWarnings("unchecked")
    final List<Object> items = (List<Object>) result.get("items");
    assertThat(items).hasSize(2).contains("c", "e");
  }

  // ===================== NODE FUNCTION TESTS =====================

  @Test
  void functionRegistryHasNodeFunctions() {
    assertThat(CypherFunctionRegistry.hasFunction("node.degree")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("node.degree.in")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("node.degree.out")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("node.labels")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("node.id")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("node.relationship.exists")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("node.relationship.types")).isTrue();
  }

  @Test
  void apocPrefixCompatibilityForNodeFunctions() {
    assertThat(CypherFunctionRegistry.hasFunction("apoc.node.degree")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("apoc.node.labels")).isTrue();
    assertThat(CypherFunctionRegistry.get("apoc.node.degree")).isSameAs(CypherFunctionRegistry.get("node.degree"));
  }

  // ===================== REL FUNCTION TESTS =====================

  @Test
  void functionRegistryHasRelFunctions() {
    assertThat(CypherFunctionRegistry.hasFunction("rel.id")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("rel.type")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("rel.startNode")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("rel.endNode")).isTrue();
  }

  @Test
  void apocPrefixCompatibilityForRelFunctions() {
    assertThat(CypherFunctionRegistry.hasFunction("apoc.rel.type")).isTrue();
    assertThat(CypherFunctionRegistry.get("apoc.rel.type")).isSameAs(CypherFunctionRegistry.get("rel.type"));
  }

  // ===================== PATH FUNCTION TESTS =====================

  @Test
  void functionRegistryHasPathFunctions() {
    assertThat(CypherFunctionRegistry.hasFunction("path.create")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("path.combine")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("path.slice")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("path.elements")).isTrue();
  }

  @Test
  @SuppressWarnings("unchecked")
  void pathCreate() {
    final StatelessFunction fn = CypherFunctionRegistry.get("path.create");
    final Map<String, Object> result = (Map<String, Object>) fn.execute(
        new Object[]{"startNode", List.of("rel1", "rel2")}, null);

    assertThat(result).isNotNull();
    assertThat(result.get("_type")).isEqualTo("path");
    assertThat(result.get("length")).isEqualTo(2);
  }

  @Test
  @SuppressWarnings("unchecked")
  void pathElements() {
    final StatelessFunction createFn = CypherFunctionRegistry.get("path.create");
    final Map<String, Object> path = (Map<String, Object>) createFn.execute(
        new Object[]{"startNode", List.of("rel1")}, null);

    final StatelessFunction fn = CypherFunctionRegistry.get("path.elements");
    final List<Object> elements = (List<Object>) fn.execute(new Object[]{path}, null);

    assertThat(elements).isNotNull().isNotEmpty();
  }

  // ===================== CREATE FUNCTION TESTS =====================

  @Test
  void functionRegistryHasCreateFunctions() {
    assertThat(CypherFunctionRegistry.hasFunction("create.uuid")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("create.uuidBase64")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("create.vNode")).isTrue();
    assertThat(CypherFunctionRegistry.hasFunction("create.vRelationship")).isTrue();
  }

  @Test
  void createUuid() {
    final StatelessFunction fn = CypherFunctionRegistry.get("create.uuid");
    final String uuid1 = (String) fn.execute(new Object[]{}, null);
    final String uuid2 = (String) fn.execute(new Object[]{}, null);

    assertThat(uuid1).isNotNull();
    assertThat(uuid2).isNotNull();
    assertThat(uuid1).isNotEqualTo(uuid2);
    // UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    assertThat(uuid1).matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
  }

  @Test
  void createUuidBase64() {
    final StatelessFunction fn = CypherFunctionRegistry.get("create.uuidBase64");
    final String uuid1 = (String) fn.execute(new Object[]{}, null);
    final String uuid2 = (String) fn.execute(new Object[]{}, null);

    assertThat(uuid1).isNotNull();
    assertThat(uuid2).isNotNull();
    assertThat(uuid1).isNotEqualTo(uuid2);
    // Base64 UUID should be 22 characters (128 bits / 6 bits per char = ~22)
    assertThat(uuid1).hasSize(22);
  }

  @Test
  @SuppressWarnings("unchecked")
  void createVNode() {
    final StatelessFunction fn = CypherFunctionRegistry.get("create.vNode");
    final Map<String, Object> vNode = (Map<String, Object>) fn.execute(
        new Object[]{List.of("Person", "Employee"), Map.of("name", "John", "age", 30)}, null);

    assertThat(vNode).isNotNull();
    assertThat(vNode.get("_type")).isEqualTo("vNode");
    assertThat(vNode.get("_labels")).isEqualTo(List.of("Person", "Employee"));
    assertThat(vNode.get("name")).isEqualTo("John");
    assertThat(vNode.get("age")).isEqualTo(30);
  }

  @Test
  @SuppressWarnings("unchecked")
  void createVRelationship() {
    final StatelessFunction fn = CypherFunctionRegistry.get("create.vRelationship");
    final Map<String, Object> vRel = (Map<String, Object>) fn.execute(
        new Object[]{"node1", "KNOWS", "node2", Map.of("since", 2020)}, null);

    assertThat(vRel).isNotNull();
    assertThat(vRel.get("_type")).isEqualTo("vRelationship");
    assertThat(vRel.get("_relType")).isEqualTo("KNOWS");
    assertThat(vRel.get("since")).isEqualTo(2020);
  }

  // ===================== SPRINT 4: ALGORITHM PROCEDURE TESTS =====================

  @Test
  void procedureRegistryHasAlgorithmProcedures() {
    assertThat(CypherProcedureRegistry.hasProcedure("algo.dijkstra")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.astar")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("algo.allsimplepaths")).isTrue();
  }

  @Test
  void apocPrefixCompatibilityForAlgorithmProcedures() {
    assertThat(CypherProcedureRegistry.hasProcedure("apoc.algo.dijkstra")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("apoc.algo.astar")).isTrue();
    assertThat(CypherProcedureRegistry.get("apoc.algo.dijkstra")).isSameAs(CypherProcedureRegistry.get("algo.dijkstra"));
  }

  @Test
  void algoDijkstraProcedureMetadata() {
    final CypherProcedure proc = CypherProcedureRegistry.get("algo.dijkstra");
    assertThat(proc).isNotNull();
    assertThat(proc.getName()).isEqualTo("algo.dijkstra");
    assertThat(proc.getMinArgs()).isEqualTo(4);
    assertThat(proc.getMaxArgs()).isEqualTo(5);
    assertThat(proc.getYieldFields()).contains("path", "weight");
  }

  @Test
  void algoAStarProcedureMetadata() {
    final CypherProcedure proc = CypherProcedureRegistry.get("algo.astar");
    assertThat(proc).isNotNull();
    assertThat(proc.getName()).isEqualTo("algo.astar");
    assertThat(proc.getMinArgs()).isEqualTo(4);
    assertThat(proc.getMaxArgs()).isEqualTo(6);
    assertThat(proc.getYieldFields()).contains("path", "weight");
  }

  @Test
  void algoAllSimplePathsProcedureMetadata() {
    final CypherProcedure proc = CypherProcedureRegistry.get("algo.allsimplepaths");
    assertThat(proc).isNotNull();
    assertThat(proc.getName()).isEqualTo("algo.allsimplepaths");
    assertThat(proc.getMinArgs()).isEqualTo(4);
    assertThat(proc.getMaxArgs()).isEqualTo(4);
    assertThat(proc.getYieldFields()).contains("path");
  }

  // ===================== SPRINT 4: PATH EXPANSION PROCEDURE TESTS =====================

  @Test
  void procedureRegistryHasPathProcedures() {
    assertThat(CypherProcedureRegistry.hasProcedure("path.expand")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("path.expandconfig")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("path.subgraphnodes")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("path.subgraphall")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("path.spanningtree")).isTrue();
  }

  @Test
  void apocPrefixCompatibilityForPathProcedures() {
    assertThat(CypherProcedureRegistry.hasProcedure("apoc.path.expand")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("apoc.path.expandconfig")).isTrue();
    assertThat(CypherProcedureRegistry.get("apoc.path.expand")).isSameAs(CypherProcedureRegistry.get("path.expand"));
  }

  @Test
  void pathExpandProcedureMetadata() {
    final CypherProcedure proc = CypherProcedureRegistry.get("path.expand");
    assertThat(proc).isNotNull();
    assertThat(proc.getName()).isEqualTo("path.expand");
    assertThat(proc.getMinArgs()).isEqualTo(5);
    assertThat(proc.getMaxArgs()).isEqualTo(5);
    assertThat(proc.getYieldFields()).contains("path");
  }

  @Test
  void pathExpandConfigProcedureMetadata() {
    final CypherProcedure proc = CypherProcedureRegistry.get("path.expandconfig");
    assertThat(proc).isNotNull();
    assertThat(proc.getName()).isEqualTo("path.expandconfig");
    assertThat(proc.getMinArgs()).isEqualTo(2);
    assertThat(proc.getMaxArgs()).isEqualTo(2);
    assertThat(proc.getYieldFields()).contains("path");
  }

  @Test
  void pathSubgraphNodesProcedureMetadata() {
    final CypherProcedure proc = CypherProcedureRegistry.get("path.subgraphnodes");
    assertThat(proc).isNotNull();
    assertThat(proc.getName()).isEqualTo("path.subgraphnodes");
    assertThat(proc.getMinArgs()).isEqualTo(2);
    assertThat(proc.getMaxArgs()).isEqualTo(2);
    assertThat(proc.getYieldFields()).contains("node");
  }

  @Test
  void pathSubgraphAllProcedureMetadata() {
    final CypherProcedure proc = CypherProcedureRegistry.get("path.subgraphall");
    assertThat(proc).isNotNull();
    assertThat(proc.getName()).isEqualTo("path.subgraphall");
    assertThat(proc.getMinArgs()).isEqualTo(2);
    assertThat(proc.getMaxArgs()).isEqualTo(2);
    assertThat(proc.getYieldFields()).contains("nodes", "relationships");
  }

  @Test
  void pathSpanningTreeProcedureMetadata() {
    final CypherProcedure proc = CypherProcedureRegistry.get("path.spanningtree");
    assertThat(proc).isNotNull();
    assertThat(proc.getName()).isEqualTo("path.spanningtree");
    assertThat(proc.getMinArgs()).isEqualTo(2);
    assertThat(proc.getMaxArgs()).isEqualTo(2);
    assertThat(proc.getYieldFields()).contains("path");
  }

  // ===================== SPRINT 4: META PROCEDURE TESTS =====================

  @Test
  void procedureRegistryHasMetaProcedures() {
    assertThat(CypherProcedureRegistry.hasProcedure("meta.graph")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("meta.schema")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("meta.stats")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("meta.nodetypeproperties")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("meta.reltypeproperties")).isTrue();
  }

  @Test
  void apocPrefixCompatibilityForMetaProcedures() {
    assertThat(CypherProcedureRegistry.hasProcedure("apoc.meta.graph")).isTrue();
    assertThat(CypherProcedureRegistry.hasProcedure("apoc.meta.schema")).isTrue();
    assertThat(CypherProcedureRegistry.get("apoc.meta.graph")).isSameAs(CypherProcedureRegistry.get("meta.graph"));
  }

  @Test
  void metaGraphProcedureMetadata() {
    final CypherProcedure proc = CypherProcedureRegistry.get("meta.graph");
    assertThat(proc).isNotNull();
    assertThat(proc.getName()).isEqualTo("meta.graph");
    assertThat(proc.getMinArgs()).isEqualTo(0);
    assertThat(proc.getMaxArgs()).isEqualTo(0);
    assertThat(proc.getYieldFields()).contains("nodes", "relationships");
  }

  @Test
  void metaSchemaProcedureMetadata() {
    final CypherProcedure proc = CypherProcedureRegistry.get("meta.schema");
    assertThat(proc).isNotNull();
    assertThat(proc.getName()).isEqualTo("meta.schema");
    assertThat(proc.getMinArgs()).isEqualTo(0);
    assertThat(proc.getMaxArgs()).isEqualTo(0);
    assertThat(proc.getYieldFields()).contains("value");
  }

  @Test
  void metaStatsProcedureMetadata() {
    final CypherProcedure proc = CypherProcedureRegistry.get("meta.stats");
    assertThat(proc).isNotNull();
    assertThat(proc.getName()).isEqualTo("meta.stats");
    assertThat(proc.getMinArgs()).isEqualTo(0);
    assertThat(proc.getMaxArgs()).isEqualTo(0);
    assertThat(proc.getYieldFields()).contains("value");
  }

  @Test
  void metaNodeTypePropertiesProcedureMetadata() {
    final CypherProcedure proc = CypherProcedureRegistry.get("meta.nodetypeproperties");
    assertThat(proc).isNotNull();
    assertThat(proc.getName()).isEqualTo("meta.nodetypeproperties");
    assertThat(proc.getMinArgs()).isEqualTo(0);
    assertThat(proc.getMaxArgs()).isEqualTo(0);
    assertThat(proc.getYieldFields()).contains("nodeType", "propertyName", "propertyTypes", "mandatory");
  }

  @Test
  void metaRelTypePropertiesProcedureMetadata() {
    final CypherProcedure proc = CypherProcedureRegistry.get("meta.reltypeproperties");
    assertThat(proc).isNotNull();
    assertThat(proc.getName()).isEqualTo("meta.reltypeproperties");
    assertThat(proc.getMinArgs()).isEqualTo(0);
    assertThat(proc.getMaxArgs()).isEqualTo(0);
    assertThat(proc.getYieldFields()).contains("relType", "propertyName", "propertyTypes", "mandatory");
  }

  // Note: Integration tests for Cypher queries with built-in functions
  // should be placed in a test class that has access to the Cypher query engine.
  // The tests above verify the function implementations directly.
}
