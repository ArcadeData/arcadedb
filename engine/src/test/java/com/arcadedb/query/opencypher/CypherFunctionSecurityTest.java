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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for security boundary conditions in Cypher functions.
 */
public class CypherFunctionSecurityTest {
  private Database database;

  @BeforeEach
  public void setup() {
    database = new DatabaseFactory("./databases/test-function-security").create();
  }

  @AfterEach
  public void teardown() {
    if (database != null) {
      database.drop();
    }
  }

  @Test
  public void testUtilSleepMaxDuration() {
    // Test that sleep duration is limited to prevent DoS
    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
      database.query("opencypher", "RETURN util.sleep(999999999999) AS result");
    });
    assertTrue(exception.getMessage().contains("Sleep duration exceeds maximum allowed"));
  }

  @Test
  public void testUtilSleepValidDuration() {
    // Test that valid sleep durations work (e.g., 100ms)
    final ResultSet resultSet = database.query("opencypher", "RETURN util.sleep(100) AS result");
    assertTrue(resultSet.hasNext());
    assertNull(resultSet.next().getProperty("result"));
  }

  @Test
  public void testUtilSleepNegativeDuration() {
    // Test that negative durations are handled gracefully
    final ResultSet resultSet = database.query("opencypher", "RETURN util.sleep(-100) AS result");
    assertTrue(resultSet.hasNext());
    assertNull(resultSet.next().getProperty("result"));
  }

  @Test
  public void testUtilSleepZeroDuration() {
    // Test that zero duration is handled
    final ResultSet resultSet = database.query("opencypher", "RETURN util.sleep(0) AS result");
    assertTrue(resultSet.hasNext());
    assertNull(resultSet.next().getProperty("result"));
  }

  @Test
  public void testUtilCompressInputSizeLimit() {
    // Test that compression has input size limits to prevent DoS
    // Create a string larger than max allowed size (11MB exceeds 10MB limit)
    final int SIZE = 11 * 1024 * 1024;
    final char[] largeChars = new char[SIZE];
    java.util.Arrays.fill(largeChars, 'x');
    final String largeString = new String(largeChars);

    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
      database.query("opencypher", "RETURN util.compress($data) AS result", "data", largeString);
    });
    assertTrue(exception.getMessage().contains("Input size exceeds maximum allowed"));
  }

  @Test
  public void testUtilCompressValidSize() {
    // Test that valid compression works
    final ResultSet resultSet = database.query("opencypher", "RETURN util.compress('Hello World') AS result");
    assertTrue(resultSet.hasNext());
    assertNotNull(resultSet.next().getProperty("result"));
  }

  @Test
  public void testUtilDecompressOutputSizeLimit() {
    // Test that decompression has output size limits to prevent zip bomb attacks
    // First, compress a small string
    final ResultSet compressResult = database.query("opencypher", "RETURN util.compress('test') AS compressed");
    final String compressed = compressResult.next().getProperty("compressed").toString();

    // For now, just verify decompression works with valid data
    final ResultSet decompressResult = database.query("opencypher",
        "RETURN util.decompress('" + compressed + "') AS result");
    assertTrue(decompressResult.hasNext());
    assertEquals("test", decompressResult.next().getProperty("result"));
  }

  @Test
  public void testTextLpadMaxLength() {
    // Test that lpad has length limits to prevent excessive memory allocation
    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
      database.query("opencypher", "RETURN text.lpad('x', 999999999, ' ') AS result");
    });
    assertTrue(exception.getMessage().contains("length exceeds maximum allowed") ||
               exception.getMessage().contains("Invalid length"));
  }

  @Test
  public void testTextLpadNegativeLength() {
    // Test that negative lengths are rejected
    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
      database.query("opencypher", "RETURN text.lpad('x', -100, ' ') AS result");
    });
    assertTrue(exception.getMessage().contains("Invalid length") ||
               exception.getMessage().contains("negative"));
  }

  @Test
  public void testTextLpadValidLength() {
    // Test that valid padding works
    final ResultSet resultSet = database.query("opencypher", "RETURN text.lpad('x', 5, ' ') AS result");
    assertTrue(resultSet.hasNext());
    assertEquals("    x", resultSet.next().getProperty("result"));
  }

  @Test
  public void testTextRpadMaxLength() {
    // Test that rpad has length limits to prevent excessive memory allocation
    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
      database.query("opencypher", "RETURN text.rpad('x', 999999999, ' ') AS result");
    });
    assertTrue(exception.getMessage().contains("length exceeds maximum allowed") ||
               exception.getMessage().contains("Invalid length"));
  }

  @Test
  public void testTextRpadNegativeLength() {
    // Test that negative lengths are rejected
    final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
      database.query("opencypher", "RETURN text.rpad('x', -100, ' ') AS result");
    });
    assertTrue(exception.getMessage().contains("Invalid length") ||
               exception.getMessage().contains("negative"));
  }

  @Test
  public void testTextRpadValidLength() {
    // Test that valid padding works
    final ResultSet resultSet = database.query("opencypher", "RETURN text.rpad('x', 5, ' ') AS result");
    assertTrue(resultSet.hasNext());
    assertEquals("x    ", resultSet.next().getProperty("result"));
  }

  @Test
  public void testTextRegexReplaceCatastrophicBacktracking() {
    // Test ReDoS protection - catastrophic backtracking pattern
    final Exception exception = assertThrows(Exception.class, () -> {
      database.query("opencypher", "RETURN text.regexReplace($str, $pattern, 'x') AS result",
          "str", "aaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          "pattern", "(a+)+b");
    });
    assertTrue(exception.getMessage().contains("pattern") ||
               exception.getMessage().contains("regex") ||
               exception.getMessage().contains("timeout") ||
               exception.getMessage().contains("too long"),
        "Expected regex-related error but got: " + exception.getMessage());
  }

  @Test
  public void testTextRegexReplaceTooLongPattern() {
    // Test that excessively long patterns are rejected
    final String longPattern = "a".repeat(1000);
    final Exception exception = assertThrows(Exception.class, () -> {
      database.query("opencypher", "RETURN text.regexReplace('test', $pattern, 'x') AS result",
          "pattern", longPattern);
    });
    assertTrue(exception.getMessage().contains("pattern") ||
               exception.getMessage().contains("regex") ||
               exception.getMessage().contains("exceeds"),
        "Expected pattern length error but got: " + exception.getMessage());
  }

  @Test
  public void testTextRegexReplaceValidPattern() {
    // Test that valid patterns work correctly
    final ResultSet resultSet = database.query("opencypher",
        "RETURN text.regexReplace('hello world', 'world', 'universe') AS result");
    assertTrue(resultSet.hasNext());
    assertEquals("hello universe", resultSet.next().getProperty("result"));
  }

  @Test
  public void testDateAddOverflow() {
    // Test that integer overflow is caught in date arithmetic
    final Exception exception = assertThrows(Exception.class, () -> {
      database.query("opencypher", "RETURN date.add(9223372036854775807, 1, 'ms') AS result");
    });
    assertTrue(exception.getMessage().contains("overflow") ||
               exception.getMessage().contains("ArithmeticException"),
        "Expected overflow error but got: " + exception.getMessage());
  }

  @Test
  public void testDateAddMultiplicationOverflow() {
    // Test that multiplication overflow is caught (large value * unit conversion)
    final Exception exception = assertThrows(Exception.class, () -> {
      database.query("opencypher", "RETURN date.add(0, 9223372036854775807, 'h') AS result");
    });
    assertTrue(exception.getMessage().contains("overflow") ||
               exception.getMessage().contains("ArithmeticException"),
        "Expected overflow error but got: " + exception.getMessage());
  }

  @Test
  public void testDateAddValidOperation() {
    // Test that valid date operations work
    final ResultSet resultSet = database.query("opencypher",
        "RETURN date.add(1000, 500, 'ms') AS result");
    assertTrue(resultSet.hasNext());
    assertEquals(1500L, resultSet.next().<Long>getProperty("result"));
  }

  @Test
  public void testTextFormatInvalidFormat() {
    // Test that invalid format strings are handled gracefully
    final Exception exception = assertThrows(Exception.class, () -> {
      database.query("opencypher", "RETURN text.format('%s %s', 'only one arg') AS result");
    });
    assertTrue(exception.getMessage().contains("format") ||
               exception.getMessage().contains("MissingFormatArgumentException"),
        "Expected format error but got: " + exception.getMessage());
  }

  @Test
  public void testTextFormatIllegalFormatConversion() {
    // Test that illegal format conversions are caught
    final Exception exception = assertThrows(Exception.class, () -> {
      database.query("opencypher", "RETURN text.format('%d', 'not a number') AS result");
    });
    assertTrue(exception.getMessage().contains("format") ||
               exception.getMessage().contains("IllegalFormatConversionException"),
        "Expected format conversion error but got: " + exception.getMessage());
  }

  @Test
  public void testTextFormatValidUsage() {
    // Test that valid formatting works
    final ResultSet resultSet = database.query("opencypher",
        "RETURN text.format('Hello %s, you are %d years old', 'Alice', 30) AS result");
    assertTrue(resultSet.hasNext());
    assertEquals("Hello Alice, you are 30 years old", resultSet.next().getProperty("result"));
  }

  @Test
  public void testTextLevenshteinDistanceMaxLength() {
    // Test that excessively long strings are rejected for Levenshtein distance
    final String longString = "a".repeat(15000);
    final Exception exception = assertThrows(Exception.class, () -> {
      database.query("opencypher", "RETURN text.levenshteinDistance($str1, $str2) AS result",
          "str1", longString,
          "str2", "test");
    });
    assertTrue(exception.getMessage().contains("exceeds maximum allowed") ||
               exception.getMessage().contains("String length"),
        "Expected string length error but got: " + exception.getMessage());
  }

  @Test
  public void testTextLevenshteinDistanceValidStrings() {
    // Test that valid string comparison works
    final ResultSet resultSet = database.query("opencypher",
        "RETURN text.levenshteinDistance('kitten', 'sitting') AS result");
    assertTrue(resultSet.hasNext());
    assertEquals(3L, resultSet.next().<Long>getProperty("result"));
  }

  @Test
  public void testDateFieldsInvalidTimezone() {
    // Test that invalid timezone IDs are rejected
    final Exception exception = assertThrows(Exception.class, () -> {
      database.query("opencypher",
          "RETURN date.fields('2024-01-15', 'yyyy-MM-dd', 'InvalidTimezone') AS result");
    });
    assertTrue(exception.getMessage().contains("timezone") ||
               exception.getMessage().contains("Invalid"),
        "Expected timezone error but got: " + exception.getMessage());
  }

  @Test
  public void testDateFieldsValidTimezone() {
    // Test that valid timezone handling works
    final ResultSet resultSet = database.query("opencypher",
        "RETURN date.fields('2024-01-15', 'yyyy-MM-dd', 'UTC') AS result");
    assertTrue(resultSet.hasNext());
    final Object result = resultSet.next().getProperty("result");
    assertNotNull(result);
    assertTrue(result instanceof java.util.Map);
  }

  @Test
  public void testTextRegexReplaceNullHandling() {
    // Test null handling in regex replace
    final ResultSet resultSet = database.query("opencypher",
        "RETURN text.regexReplace(null, 'pattern', 'replace') AS result");
    assertTrue(resultSet.hasNext());
    assertNull(resultSet.next().getProperty("result"));
  }

  @Test
  public void testDateAddNullHandling() {
    // Test null handling in date add
    final ResultSet resultSet = database.query("opencypher",
        "RETURN date.add(null, 100, 'ms') AS result");
    assertTrue(resultSet.hasNext());
    assertNull(resultSet.next().getProperty("result"));
  }
}
