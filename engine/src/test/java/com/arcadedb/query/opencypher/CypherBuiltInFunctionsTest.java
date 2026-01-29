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
import com.arcadedb.query.opencypher.functions.CypherFunction;
import com.arcadedb.query.opencypher.functions.CypherFunctionRegistry;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.opencypher.procedures.CypherProcedureRegistry;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Cypher built-in functions and procedures.
 *
 * @author ArcadeDB Team
 */
public class CypherBuiltInFunctionsTest extends TestHelper {

  // ===================== REGISTRY TESTS =====================

  @Test
  public void testFunctionRegistryHasTextFunctions() {
    assertTrue(CypherFunctionRegistry.hasFunction("text.indexOf"));
    assertTrue(CypherFunctionRegistry.hasFunction("text.join"));
    assertTrue(CypherFunctionRegistry.hasFunction("text.split"));
    assertTrue(CypherFunctionRegistry.hasFunction("text.capitalize"));
    assertTrue(CypherFunctionRegistry.hasFunction("text.camelCase"));
    assertTrue(CypherFunctionRegistry.hasFunction("text.levenshteinDistance"));
  }

  @Test
  public void testFunctionRegistryHasMapFunctions() {
    assertTrue(CypherFunctionRegistry.hasFunction("map.merge"));
    assertTrue(CypherFunctionRegistry.hasFunction("map.fromLists"));
    assertTrue(CypherFunctionRegistry.hasFunction("map.setKey"));
    assertTrue(CypherFunctionRegistry.hasFunction("map.flatten"));
  }

  @Test
  public void testFunctionRegistryHasMathFunctions() {
    assertTrue(CypherFunctionRegistry.hasFunction("math.sigmoid"));
    assertTrue(CypherFunctionRegistry.hasFunction("math.tanh"));
    assertTrue(CypherFunctionRegistry.hasFunction("math.maxLong"));
  }

  @Test
  public void testFunctionRegistryHasConvertFunctions() {
    assertTrue(CypherFunctionRegistry.hasFunction("convert.toJson"));
    assertTrue(CypherFunctionRegistry.hasFunction("convert.fromJsonMap"));
    assertTrue(CypherFunctionRegistry.hasFunction("convert.toList"));
  }

  @Test
  public void testProcedureRegistryHasMergeProcedures() {
    assertTrue(CypherProcedureRegistry.hasProcedure("merge.relationship"));
    assertTrue(CypherProcedureRegistry.hasProcedure("merge.node"));
  }

  // ===================== TEXT FUNCTION TESTS =====================

  @Test
  public void testTextIndexOf() {
    final CypherFunction fn = CypherFunctionRegistry.get("text.indexOf");
    assertEquals(0L, fn.execute(new Object[]{"hello", "h"}, null));
    assertEquals(2L, fn.execute(new Object[]{"hello", "l"}, null));
    assertEquals(-1L, fn.execute(new Object[]{"hello", "x"}, null));
    assertEquals(3L, fn.execute(new Object[]{"hello", "l", 3}, null));
  }

  @Test
  public void testTextJoin() {
    final CypherFunction fn = CypherFunctionRegistry.get("text.join");
    assertEquals("a,b,c", fn.execute(new Object[]{List.of("a", "b", "c"), ","}, null));
    assertEquals("abc", fn.execute(new Object[]{List.of("a", "b", "c"), ""}, null));
  }

  @Test
  public void testTextSplit() {
    final CypherFunction fn = CypherFunctionRegistry.get("text.split");
    assertEquals(List.of("a", "b", "c"), fn.execute(new Object[]{"a,b,c", ","}, null));
  }

  @Test
  public void testTextReplace() {
    final CypherFunction fn = CypherFunctionRegistry.get("text.replace");
    assertEquals("hXllo", fn.execute(new Object[]{"hello", "e", "X"}, null));
    assertEquals("helloworld", fn.execute(new Object[]{"hello world", " ", ""}, null));
  }

  @Test
  public void testTextRegexReplace() {
    final CypherFunction fn = CypherFunctionRegistry.get("text.regexReplace");
    assertEquals("hXllX", fn.execute(new Object[]{"hello", "[aeiou]", "X"}, null));
  }

  @Test
  public void testTextCapitalize() {
    final CypherFunction fn = CypherFunctionRegistry.get("text.capitalize");
    assertEquals("Hello", fn.execute(new Object[]{"hello"}, null));
    assertEquals("Hello world", fn.execute(new Object[]{"hello world"}, null));
  }

  @Test
  public void testTextCapitalizeAll() {
    final CypherFunction fn = CypherFunctionRegistry.get("text.capitalizeAll");
    assertEquals("Hello World", fn.execute(new Object[]{"hello world"}, null));
  }

  @Test
  public void testTextCamelCase() {
    final CypherFunction fn = CypherFunctionRegistry.get("text.camelCase");
    assertEquals("helloWorld", fn.execute(new Object[]{"hello world"}, null));
    assertEquals("helloWorld", fn.execute(new Object[]{"hello_world"}, null));
  }

  @Test
  public void testTextSnakeCase() {
    final CypherFunction fn = CypherFunctionRegistry.get("text.snakeCase");
    assertEquals("hello_world", fn.execute(new Object[]{"helloWorld"}, null));
    assertEquals("hello_world", fn.execute(new Object[]{"hello world"}, null));
  }

  @Test
  public void testTextUpperCamelCase() {
    final CypherFunction fn = CypherFunctionRegistry.get("text.upperCamelCase");
    assertEquals("HelloWorld", fn.execute(new Object[]{"hello world"}, null));
  }

  @Test
  public void testTextLpad() {
    final CypherFunction fn = CypherFunctionRegistry.get("text.lpad");
    assertEquals("00042", fn.execute(new Object[]{"42", 5, "0"}, null));
    assertEquals("42", fn.execute(new Object[]{"42", 2, "0"}, null));
  }

  @Test
  public void testTextRpad() {
    final CypherFunction fn = CypherFunctionRegistry.get("text.rpad");
    assertEquals("42000", fn.execute(new Object[]{"42", 5, "0"}, null));
  }

  @Test
  public void testTextSlug() {
    final CypherFunction fn = CypherFunctionRegistry.get("text.slug");
    assertEquals("hello-world", fn.execute(new Object[]{"Hello World!"}, null));
    assertEquals("hello_world", fn.execute(new Object[]{"Hello World!", "_"}, null));
  }

  @Test
  public void testTextLevenshteinDistance() {
    final CypherFunction fn = CypherFunctionRegistry.get("text.levenshteinDistance");
    assertEquals(0L, fn.execute(new Object[]{"hello", "hello"}, null));
    assertEquals(1L, fn.execute(new Object[]{"hello", "hallo"}, null));
    assertEquals(3L, fn.execute(new Object[]{"kitten", "sitting"}, null));
  }

  @Test
  public void testTextLevenshteinSimilarity() {
    final CypherFunction fn = CypherFunctionRegistry.get("text.levenshteinSimilarity");
    assertEquals(1.0, fn.execute(new Object[]{"hello", "hello"}, null));
    // hello vs hallo has distance 1, max length 5, so similarity = 1 - 1/5 = 0.8
    assertEquals(0.8, (Double) fn.execute(new Object[]{"hello", "hallo"}, null), 0.001);
  }

  @Test
  public void testTextHammingDistance() {
    final CypherFunction fn = CypherFunctionRegistry.get("text.hammingDistance");
    assertEquals(0L, fn.execute(new Object[]{"hello", "hello"}, null));
    assertEquals(1L, fn.execute(new Object[]{"hello", "hallo"}, null));
  }

  @Test
  public void testTextCharAt() {
    final CypherFunction fn = CypherFunctionRegistry.get("text.charAt");
    assertEquals("h", fn.execute(new Object[]{"hello", 0}, null));
    assertEquals("o", fn.execute(new Object[]{"hello", 4}, null));
    assertNull(fn.execute(new Object[]{"hello", 10}, null));
  }

  @Test
  public void testTextCode() {
    final CypherFunction fn = CypherFunctionRegistry.get("text.code");
    assertEquals(65L, fn.execute(new Object[]{"A"}, null));
    assertEquals(97L, fn.execute(new Object[]{"a"}, null));
  }

  // ===================== MAP FUNCTION TESTS =====================

  @Test
  public void testMapMerge() {
    final CypherFunction fn = CypherFunctionRegistry.get("map.merge");
    @SuppressWarnings("unchecked")
    final Map<String, Object> result = (Map<String, Object>) fn.execute(
        new Object[]{Map.of("a", 1), Map.of("b", 2)}, null);
    assertEquals(1, result.get("a"));
    assertEquals(2, result.get("b"));
  }

  @Test
  public void testMapMergeOverride() {
    final CypherFunction fn = CypherFunctionRegistry.get("map.merge");
    @SuppressWarnings("unchecked")
    final Map<String, Object> result = (Map<String, Object>) fn.execute(
        new Object[]{Map.of("a", 1), Map.of("a", 2)}, null);
    assertEquals(2, result.get("a"));
  }

  @Test
  public void testMapFromLists() {
    final CypherFunction fn = CypherFunctionRegistry.get("map.fromLists");
    @SuppressWarnings("unchecked")
    final Map<String, Object> result = (Map<String, Object>) fn.execute(
        new Object[]{List.of("a", "b"), List.of(1, 2)}, null);
    assertEquals(1, result.get("a"));
    assertEquals(2, result.get("b"));
  }

  @Test
  public void testMapFromPairs() {
    final CypherFunction fn = CypherFunctionRegistry.get("map.fromPairs");
    @SuppressWarnings("unchecked")
    final Map<String, Object> result = (Map<String, Object>) fn.execute(
        new Object[]{List.of(List.of("a", 1), List.of("b", 2))}, null);
    assertEquals(1, result.get("a"));
    assertEquals(2, result.get("b"));
  }

  @Test
  public void testMapSetKey() {
    final CypherFunction fn = CypherFunctionRegistry.get("map.setKey");
    @SuppressWarnings("unchecked")
    final Map<String, Object> result = (Map<String, Object>) fn.execute(
        new Object[]{Map.of("a", 1), "b", 2}, null);
    assertEquals(1, result.get("a"));
    assertEquals(2, result.get("b"));
  }

  @Test
  public void testMapRemoveKey() {
    final CypherFunction fn = CypherFunctionRegistry.get("map.removeKey");
    @SuppressWarnings("unchecked")
    final Map<String, Object> result = (Map<String, Object>) fn.execute(
        new Object[]{Map.of("a", 1, "b", 2), "a"}, null);
    assertFalse(result.containsKey("a"));
    assertEquals(2, result.get("b"));
  }

  @Test
  public void testMapFlatten() {
    final CypherFunction fn = CypherFunctionRegistry.get("map.flatten");
    @SuppressWarnings("unchecked")
    final Map<String, Object> result = (Map<String, Object>) fn.execute(
        new Object[]{Map.of("a", Map.of("b", 1)), "."}, null);
    assertEquals(1, result.get("a.b"));
  }

  @Test
  public void testMapSubmap() {
    final CypherFunction fn = CypherFunctionRegistry.get("map.submap");
    @SuppressWarnings("unchecked")
    final Map<String, Object> result = (Map<String, Object>) fn.execute(
        new Object[]{Map.of("a", 1, "b", 2, "c", 3), List.of("a", "c")}, null);
    assertEquals(2, result.size());
    assertEquals(1, result.get("a"));
    assertEquals(3, result.get("c"));
    assertFalse(result.containsKey("b"));
  }

  // ===================== MATH FUNCTION TESTS =====================

  @Test
  public void testMathSigmoid() {
    final CypherFunction fn = CypherFunctionRegistry.get("math.sigmoid");
    assertEquals(0.5, fn.execute(new Object[]{0.0}, null));
    assertTrue((Double) fn.execute(new Object[]{10.0}, null) > 0.99);
    assertTrue((Double) fn.execute(new Object[]{-10.0}, null) < 0.01);
  }

  @Test
  public void testMathTanh() {
    final CypherFunction fn = CypherFunctionRegistry.get("math.tanh");
    assertEquals(0.0, (Double) fn.execute(new Object[]{0.0}, null), 0.001);
  }

  @Test
  public void testMathMaxLong() {
    final CypherFunction fn = CypherFunctionRegistry.get("math.maxLong");
    assertEquals(Long.MAX_VALUE, fn.execute(new Object[]{}, null));
  }

  @Test
  public void testMathMinLong() {
    final CypherFunction fn = CypherFunctionRegistry.get("math.minLong");
    assertEquals(Long.MIN_VALUE, fn.execute(new Object[]{}, null));
  }

  @Test
  public void testMathMaxDouble() {
    final CypherFunction fn = CypherFunctionRegistry.get("math.maxDouble");
    assertEquals(Double.MAX_VALUE, fn.execute(new Object[]{}, null));
  }

  // ===================== CONVERT FUNCTION TESTS =====================

  @Test
  public void testConvertToJson() {
    final CypherFunction fn = CypherFunctionRegistry.get("convert.toJson");
    assertEquals("{\"a\":1}", fn.execute(new Object[]{Map.of("a", 1)}, null));
    assertEquals("[1,2,3]", fn.execute(new Object[]{List.of(1, 2, 3)}, null));
  }

  @Test
  public void testConvertFromJsonMap() {
    final CypherFunction fn = CypherFunctionRegistry.get("convert.fromJsonMap");
    @SuppressWarnings("unchecked")
    final Map<String, Object> result = (Map<String, Object>) fn.execute(
        new Object[]{"{\"a\":1}"}, null);
    assertEquals(1, ((Number) result.get("a")).intValue());
  }

  @Test
  public void testConvertFromJsonList() {
    final CypherFunction fn = CypherFunctionRegistry.get("convert.fromJsonList");
    @SuppressWarnings("unchecked")
    final List<Object> result = (List<Object>) fn.execute(new Object[]{"[1,2,3]"}, null);
    assertEquals(3, result.size());
    assertEquals(1, ((Number) result.get(0)).intValue());
  }

  @Test
  public void testConvertToList() {
    final CypherFunction fn = CypherFunctionRegistry.get("convert.toList");
    @SuppressWarnings("unchecked")
    final List<Object> result = (List<Object>) fn.execute(new Object[]{42}, null);
    assertEquals(1, result.size());
    assertEquals(42, result.get(0));
  }

  @Test
  public void testConvertToSet() {
    final CypherFunction fn = CypherFunctionRegistry.get("convert.toSet");
    @SuppressWarnings("unchecked")
    final List<Object> result = (List<Object>) fn.execute(
        new Object[]{List.of(1, 2, 2, 3, 3, 3)}, null);
    assertEquals(3, result.size());
  }

  @Test
  public void testConvertToBoolean() {
    final CypherFunction fn = CypherFunctionRegistry.get("convert.toBoolean");
    assertEquals(true, fn.execute(new Object[]{"true"}, null));
    assertEquals(false, fn.execute(new Object[]{"false"}, null));
    assertEquals(true, fn.execute(new Object[]{1}, null));
    assertEquals(false, fn.execute(new Object[]{0}, null));
  }

  @Test
  public void testConvertToInteger() {
    final CypherFunction fn = CypherFunctionRegistry.get("convert.toInteger");
    assertEquals(42L, fn.execute(new Object[]{"42"}, null));
    assertEquals(42L, fn.execute(new Object[]{42.9}, null));
    assertEquals(1L, fn.execute(new Object[]{true}, null));
  }

  @Test
  public void testConvertToFloat() {
    final CypherFunction fn = CypherFunctionRegistry.get("convert.toFloat");
    assertEquals(42.0, fn.execute(new Object[]{"42"}, null));
    assertEquals(42.5, fn.execute(new Object[]{"42.5"}, null));
  }

  // Note: Integration tests for Cypher queries with built-in functions
  // should be placed in a test class that has access to the Cypher query engine.
  // The tests above verify the function implementations directly.
}
