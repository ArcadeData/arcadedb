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
package com.arcadedb.exception;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for enhanced ArcadeDBException.
 */
class ArcadeDBExceptionTest {

  @Test
  void testBackwardCompatibility() {
    final ArcadeDBException ex = new ArcadeDBException("Test message");
    assertEquals("Test message", ex.getMessage());
    assertEquals(ErrorCode.UNKNOWN_ERROR, ex.getErrorCode());
    assertNotNull(ex.getContext());
    assertTrue(ex.getContext().isEmpty());
  }

  @Test
  void testWithErrorCode() {
    final ArcadeDBException ex = new ArcadeDBException(ErrorCode.DATABASE_NOT_FOUND, "Database 'test' not found");
    assertEquals(ErrorCode.DATABASE_NOT_FOUND, ex.getErrorCode());
    assertEquals("Database 'test' not found", ex.getMessage());
  }

  @Test
  void testWithCause() {
    final Exception cause = new RuntimeException("Root cause");
    final ArcadeDBException ex = new ArcadeDBException(ErrorCode.IO_ERROR, "I/O failed", cause);
    assertEquals(ErrorCode.IO_ERROR, ex.getErrorCode());
    assertEquals("I/O failed", ex.getMessage());
    assertEquals(cause, ex.getCause());
  }

  @Test
  void testWithContext() {
    final Map<String, Object> context = new HashMap<>();
    context.put("databaseName", "testdb");
    context.put("path", "/data/databases");
    
    final ArcadeDBException ex = new ArcadeDBException(ErrorCode.DATABASE_NOT_FOUND, "Database not found", context);
    assertEquals(2, ex.getContext().size());
    assertEquals("testdb", ex.getContext().get("databaseName"));
    assertEquals("/data/databases", ex.getContext().get("path"));
  }

  @Test
  void testAddContext() {
    final ArcadeDBException ex = new ArcadeDBException(ErrorCode.DATABASE_NOT_FOUND, "Database not found");
    ex.addContext("databaseName", "testdb");
    ex.addContext("attempt", 3);
    
    assertEquals(2, ex.getContext().size());
    assertEquals("testdb", ex.getContext().get("databaseName"));
    assertEquals(3, ex.getContext().get("attempt"));
  }

  @Test
  void testContextIsImmutable() {
    final ArcadeDBException ex = new ArcadeDBException(ErrorCode.DATABASE_NOT_FOUND, "Database not found");
    final Map<String, Object> context = ex.getContext();
    
    assertThrows(UnsupportedOperationException.class, () -> {
      context.put("key", "value");
    });
  }

  @Test
  void testToJSON() {
    final ArcadeDBException ex = new ArcadeDBException(ErrorCode.DATABASE_NOT_FOUND, "Database 'test' not found");
    ex.addContext("databaseName", "test");
    ex.addContext("retries", 3);
    
    final String json = ex.toJSON();
    assertTrue(json.contains("\"errorCode\":1001"));
    assertTrue(json.contains("\"errorName\":\"DATABASE_NOT_FOUND\""));
    assertTrue(json.contains("\"category\":\"Database\""));
    assertTrue(json.contains("\"message\":\"Database 'test' not found\""));
    assertTrue(json.contains("\"databaseName\":\"test\""));
    assertTrue(json.contains("\"retries\":3"));
  }

  @Test
  void testToJSONWithCause() {
    final Exception cause = new RuntimeException("Connection refused");
    final ArcadeDBException ex = new ArcadeDBException(ErrorCode.NETWORK_ERROR, "Network failure", cause);
    
    final String json = ex.toJSON();
    assertTrue(json.contains("\"errorCode\":6001"));
    assertTrue(json.contains("\"cause\":\"Connection refused\""));
  }

  @Test
  void testToJSONEscaping() {
    final ArcadeDBException ex = new ArcadeDBException(ErrorCode.QUERY_PARSING_ERROR, "Invalid query: \"SELECT * FROM test\"");
    
    final String json = ex.toJSON();
    assertTrue(json.contains("\\\"SELECT"));
  }

  @Test
  void testToString() {
    final ArcadeDBException ex = new ArcadeDBException(ErrorCode.DATABASE_NOT_FOUND, "Database not found");
    final String str = ex.toString();
    
    assertTrue(str.contains("ArcadeDBException"));
    assertTrue(str.contains("Database"));
    assertTrue(str.contains("1001"));
    assertTrue(str.contains("Database not found"));
  }

  @Test
  void testNullSafety() {
    final ArcadeDBException ex = new ArcadeDBException(null, "Test message");
    assertEquals(ErrorCode.UNKNOWN_ERROR, ex.getErrorCode());
    
    ex.addContext(null, "value");
    assertEquals(0, ex.getContext().size());
  }

  @Test
  void testContextWithNullValues() {
    final ArcadeDBException ex = new ArcadeDBException(ErrorCode.DATABASE_NOT_FOUND, "Test");
    ex.addContext("key1", null);
    ex.addContext("key2", "value");
    
    assertEquals(2, ex.getContext().size());
    assertNull(ex.getContext().get("key1"));
    assertEquals("value", ex.getContext().get("key2"));
  }

  @Test
  void testJSONWithNullContext() {
    final ArcadeDBException ex = new ArcadeDBException(ErrorCode.DATABASE_NOT_FOUND, "Test");
    ex.addContext("nullValue", null);
    
    final String json = ex.toJSON();
    assertTrue(json.contains("\"nullValue\":null"));
  }

  @Test
  void testJSONWithBooleanContext() {
    final ArcadeDBException ex = new ArcadeDBException(ErrorCode.DATABASE_NOT_FOUND, "Test");
    ex.addContext("flag", true);
    
    final String json = ex.toJSON();
    assertTrue(json.contains("\"flag\":true"));
  }

  @Test
  void testJSONWithNumberContext() {
    final ArcadeDBException ex = new ArcadeDBException(ErrorCode.DATABASE_NOT_FOUND, "Test");
    ex.addContext("count", 42);
    ex.addContext("ratio", 3.14);
    
    final String json = ex.toJSON();
    assertTrue(json.contains("\"count\":42"));
    assertTrue(json.contains("\"ratio\":3.14"));
  }
}
