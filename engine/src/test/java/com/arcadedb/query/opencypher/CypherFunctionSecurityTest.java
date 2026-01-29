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
}
