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

import java.util.Arrays;
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
  public void testApocPrefixCompatibilityForFunctions() {
    // Test that functions can be accessed with "apoc." prefix
    assertTrue(CypherFunctionRegistry.hasFunction("apoc.text.indexOf"));
    assertTrue(CypherFunctionRegistry.hasFunction("apoc.map.merge"));
    assertTrue(CypherFunctionRegistry.hasFunction("apoc.math.sigmoid"));
    assertTrue(CypherFunctionRegistry.hasFunction("apoc.convert.toJson"));
    assertTrue(CypherFunctionRegistry.hasFunction("apoc.date.currentTimestamp"));
    assertTrue(CypherFunctionRegistry.hasFunction("apoc.util.md5"));
    assertTrue(CypherFunctionRegistry.hasFunction("apoc.agg.median"));

    // Verify same function is returned with or without prefix
    assertSame(CypherFunctionRegistry.get("text.indexOf"), CypherFunctionRegistry.get("apoc.text.indexOf"));
    assertSame(CypherFunctionRegistry.get("map.merge"), CypherFunctionRegistry.get("apoc.map.merge"));

    // Test case insensitivity
    assertSame(CypherFunctionRegistry.get("TEXT.INDEXOF"), CypherFunctionRegistry.get("APOC.TEXT.INDEXOF"));
  }

  @Test
  public void testApocPrefixCompatibilityForProcedures() {
    // Test that procedures can be accessed with "apoc." prefix
    assertTrue(CypherProcedureRegistry.hasProcedure("apoc.merge.relationship"));
    assertTrue(CypherProcedureRegistry.hasProcedure("apoc.merge.node"));

    // Verify same procedure is returned with or without prefix
    assertSame(CypherProcedureRegistry.get("merge.relationship"), CypherProcedureRegistry.get("apoc.merge.relationship"));
    assertSame(CypherProcedureRegistry.get("merge.node"), CypherProcedureRegistry.get("apoc.merge.node"));

    // Test case insensitivity
    assertSame(CypherProcedureRegistry.get("MERGE.RELATIONSHIP"), CypherProcedureRegistry.get("APOC.MERGE.RELATIONSHIP"));
  }

  @Test
  public void testApocPrefixFunctionExecution() {
    // Test that functions accessed via apoc prefix work correctly
    final CypherFunction fn = CypherFunctionRegistry.get("apoc.text.indexOf");
    assertNotNull(fn);
    assertEquals(0L, fn.execute(new Object[]{"hello", "h"}, null));

    final CypherFunction md5 = CypherFunctionRegistry.get("apoc.util.md5");
    assertNotNull(md5);
    assertEquals("5d41402abc4b2a76b9719d911017c592", md5.execute(new Object[]{"hello"}, null));
  }

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

  // ===================== DATE FUNCTION TESTS =====================

  @Test
  public void testFunctionRegistryHasDateFunctions() {
    assertTrue(CypherFunctionRegistry.hasFunction("date.format"));
    assertTrue(CypherFunctionRegistry.hasFunction("date.parse"));
    assertTrue(CypherFunctionRegistry.hasFunction("date.add"));
    assertTrue(CypherFunctionRegistry.hasFunction("date.convert"));
    assertTrue(CypherFunctionRegistry.hasFunction("date.field"));
    assertTrue(CypherFunctionRegistry.hasFunction("date.fields"));
    assertTrue(CypherFunctionRegistry.hasFunction("date.currentTimestamp"));
    assertTrue(CypherFunctionRegistry.hasFunction("date.toISO8601"));
    assertTrue(CypherFunctionRegistry.hasFunction("date.fromISO8601"));
    assertTrue(CypherFunctionRegistry.hasFunction("date.systemTimezone"));
  }

  @Test
  public void testDateCurrentTimestamp() {
    final CypherFunction fn = CypherFunctionRegistry.get("date.currentTimestamp");
    final long before = System.currentTimeMillis();
    final long result = (Long) fn.execute(new Object[]{}, null);
    final long after = System.currentTimeMillis();
    assertTrue(result >= before && result <= after);
  }

  @Test
  public void testDateSystemTimezone() {
    final CypherFunction fn = CypherFunctionRegistry.get("date.systemTimezone");
    final String result = (String) fn.execute(new Object[]{}, null);
    assertNotNull(result);
    assertFalse(result.isEmpty());
  }

  @Test
  public void testDateFormat() {
    final CypherFunction fn = CypherFunctionRegistry.get("date.format");
    // Test with a known timestamp (2024-01-15T10:30:00 UTC = 1705314600000)
    final String result = (String) fn.execute(new Object[]{1705314600000L, "ms", "yyyy-MM-dd"}, null);
    assertNotNull(result);
    assertTrue(result.matches("\\d{4}-\\d{2}-\\d{2}"));
  }

  @Test
  public void testDateField() {
    final CypherFunction fn = CypherFunctionRegistry.get("date.field");
    // Test extracting year from a timestamp
    final Long year = (Long) fn.execute(new Object[]{1705314600000L, "year"}, null);
    assertEquals(2024L, year);
  }

  @Test
  public void testDateConvert() {
    final CypherFunction fn = CypherFunctionRegistry.get("date.convert");
    // Convert 1000 ms to seconds
    final Long result = (Long) fn.execute(new Object[]{1000L, "ms", "s"}, null);
    assertEquals(1L, result);

    // Convert 60 seconds to minutes
    final Long result2 = (Long) fn.execute(new Object[]{60L, "s", "m"}, null);
    assertEquals(1L, result2);
  }

  @Test
  public void testDateAdd() {
    final CypherFunction fn = CypherFunctionRegistry.get("date.add");
    // Add 1 day (86400000 ms) to a timestamp
    final long timestamp = 1705314600000L;
    final Long result = (Long) fn.execute(new Object[]{timestamp, 1L, "d"}, null);
    assertEquals(timestamp + 86400000L, result);
  }

  // ===================== UTIL FUNCTION TESTS =====================

  @Test
  public void testFunctionRegistryHasUtilFunctions() {
    assertTrue(CypherFunctionRegistry.hasFunction("util.md5"));
    assertTrue(CypherFunctionRegistry.hasFunction("util.sha1"));
    assertTrue(CypherFunctionRegistry.hasFunction("util.sha256"));
    assertTrue(CypherFunctionRegistry.hasFunction("util.sha512"));
    assertTrue(CypherFunctionRegistry.hasFunction("util.compress"));
    assertTrue(CypherFunctionRegistry.hasFunction("util.decompress"));
    assertTrue(CypherFunctionRegistry.hasFunction("util.sleep"));
    assertTrue(CypherFunctionRegistry.hasFunction("util.validate"));
  }

  @Test
  public void testUtilMd5() {
    final CypherFunction fn = CypherFunctionRegistry.get("util.md5");
    final String result = (String) fn.execute(new Object[]{"hello"}, null);
    assertEquals("5d41402abc4b2a76b9719d911017c592", result);
  }

  @Test
  public void testUtilSha1() {
    final CypherFunction fn = CypherFunctionRegistry.get("util.sha1");
    final String result = (String) fn.execute(new Object[]{"hello"}, null);
    assertEquals("aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d", result);
  }

  @Test
  public void testUtilSha256() {
    final CypherFunction fn = CypherFunctionRegistry.get("util.sha256");
    final String result = (String) fn.execute(new Object[]{"hello"}, null);
    assertEquals("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824", result);
  }

  @Test
  public void testUtilSha512() {
    final CypherFunction fn = CypherFunctionRegistry.get("util.sha512");
    final String result = (String) fn.execute(new Object[]{"hello"}, null);
    assertEquals("9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043", result);
  }

  @Test
  public void testUtilCompressDecompress() {
    final CypherFunction compress = CypherFunctionRegistry.get("util.compress");
    final CypherFunction decompress = CypherFunctionRegistry.get("util.decompress");

    final String original = "Hello, World! This is a test string for compression.";
    final String compressed = (String) compress.execute(new Object[]{original, "gzip"}, null);
    assertNotNull(compressed);

    final String decompressed = (String) decompress.execute(new Object[]{compressed, "gzip"}, null);
    assertEquals(original, decompressed);
  }

  @Test
  public void testUtilValidateSuccess() {
    final CypherFunction fn = CypherFunctionRegistry.get("util.validate");
    final Boolean result = (Boolean) fn.execute(new Object[]{true, "Should not throw"}, null);
    assertTrue(result);
  }

  @Test
  public void testUtilValidateFailure() {
    final CypherFunction fn = CypherFunctionRegistry.get("util.validate");
    assertThrows(IllegalArgumentException.class, () -> {
      fn.execute(new Object[]{false, "Validation failed!"}, null);
    });
  }

  // ===================== AGG FUNCTION TESTS =====================

  @Test
  public void testFunctionRegistryHasAggFunctions() {
    assertTrue(CypherFunctionRegistry.hasFunction("agg.first"));
    assertTrue(CypherFunctionRegistry.hasFunction("agg.last"));
    assertTrue(CypherFunctionRegistry.hasFunction("agg.nth"));
    assertTrue(CypherFunctionRegistry.hasFunction("agg.slice"));
    assertTrue(CypherFunctionRegistry.hasFunction("agg.median"));
    assertTrue(CypherFunctionRegistry.hasFunction("agg.percentiles"));
    assertTrue(CypherFunctionRegistry.hasFunction("agg.statistics"));
    assertTrue(CypherFunctionRegistry.hasFunction("agg.product"));
    assertTrue(CypherFunctionRegistry.hasFunction("agg.minItems"));
    assertTrue(CypherFunctionRegistry.hasFunction("agg.maxItems"));
  }

  @Test
  public void testAggFirst() {
    final CypherFunction fn = CypherFunctionRegistry.get("agg.first");
    assertEquals("a", fn.execute(new Object[]{List.of("a", "b", "c")}, null));
    assertEquals("b", fn.execute(new Object[]{Arrays.asList(null, "b", "c")}, null));
  }

  @Test
  public void testAggLast() {
    final CypherFunction fn = CypherFunctionRegistry.get("agg.last");
    assertEquals("c", fn.execute(new Object[]{List.of("a", "b", "c")}, null));
  }

  @Test
  public void testAggNth() {
    final CypherFunction fn = CypherFunctionRegistry.get("agg.nth");
    assertEquals("b", fn.execute(new Object[]{List.of("a", "b", "c"), 1}, null));
    assertNull(fn.execute(new Object[]{List.of("a", "b", "c"), 10}, null));
  }

  @Test
  public void testAggSlice() {
    final CypherFunction fn = CypherFunctionRegistry.get("agg.slice");
    @SuppressWarnings("unchecked")
    final List<Object> result = (List<Object>) fn.execute(new Object[]{List.of(1, 2, 3, 4, 5), 1, 4}, null);
    assertEquals(List.of(2, 3, 4), result);
  }

  @Test
  public void testAggMedian() {
    final CypherFunction fn = CypherFunctionRegistry.get("agg.median");
    // Odd number of elements
    assertEquals(3.0, fn.execute(new Object[]{List.of(1, 2, 3, 4, 5)}, null));
    // Even number of elements
    assertEquals(2.5, fn.execute(new Object[]{List.of(1, 2, 3, 4)}, null));
  }

  @Test
  public void testAggPercentiles() {
    final CypherFunction fn = CypherFunctionRegistry.get("agg.percentiles");
    @SuppressWarnings("unchecked")
    final List<Double> result = (List<Double>) fn.execute(
        new Object[]{List.of(1.0, 2.0, 3.0, 4.0, 5.0), List.of(0.5)}, null);
    assertEquals(1, result.size());
    assertEquals(3.0, result.get(0), 0.001);
  }

  @Test
  public void testAggStatistics() {
    final CypherFunction fn = CypherFunctionRegistry.get("agg.statistics");
    @SuppressWarnings("unchecked")
    final Map<String, Object> result = (Map<String, Object>) fn.execute(
        new Object[]{List.of(1.0, 2.0, 3.0, 4.0, 5.0)}, null);

    assertEquals(5L, result.get("count"));
    assertEquals(1.0, result.get("min"));
    assertEquals(5.0, result.get("max"));
    assertEquals(15.0, result.get("sum"));
    assertEquals(3.0, result.get("mean"));
  }

  @Test
  public void testAggProduct() {
    final CypherFunction fn = CypherFunctionRegistry.get("agg.product");
    assertEquals(120.0, fn.execute(new Object[]{List.of(1.0, 2.0, 3.0, 4.0, 5.0)}, null));
  }

  @Test
  public void testAggMinItems() {
    final CypherFunction fn = CypherFunctionRegistry.get("agg.minItems");
    @SuppressWarnings("unchecked")
    final Map<String, Object> result = (Map<String, Object>) fn.execute(
        new Object[]{List.of(3.0, 1.0, 2.0, 1.0, 4.0), List.of("c", "a", "b", "d", "e")}, null);

    assertEquals(1.0, result.get("value"));
    @SuppressWarnings("unchecked")
    final List<Object> items = (List<Object>) result.get("items");
    assertEquals(2, items.size());
    assertTrue(items.contains("a"));
    assertTrue(items.contains("d"));
  }

  @Test
  public void testAggMaxItems() {
    final CypherFunction fn = CypherFunctionRegistry.get("agg.maxItems");
    @SuppressWarnings("unchecked")
    final Map<String, Object> result = (Map<String, Object>) fn.execute(
        new Object[]{List.of(3.0, 1.0, 5.0, 2.0, 5.0), List.of("a", "b", "c", "d", "e")}, null);

    assertEquals(5.0, result.get("value"));
    @SuppressWarnings("unchecked")
    final List<Object> items = (List<Object>) result.get("items");
    assertEquals(2, items.size());
    assertTrue(items.contains("c"));
    assertTrue(items.contains("e"));
  }

  // Note: Integration tests for Cypher queries with built-in functions
  // should be placed in a test class that has access to the Cypher query engine.
  // The tests above verify the function implementations directly.
}
