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

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for ExceptionBuilder.
 */
class ExceptionBuilderTest {

  @Test
  void testBasicBuilder() {
    final ArcadeDBException ex = ExceptionBuilder.create()
        .errorCode(ErrorCode.DATABASE_NOT_FOUND)
        .message("Database not found")
        .build();
    
    assertEquals(ErrorCode.DATABASE_NOT_FOUND, ex.getErrorCode());
    assertEquals("Database not found", ex.getMessage());
  }

  @Test
  void testBuilderWithFormatMessage() {
    final ArcadeDBException ex = ExceptionBuilder.create()
        .errorCode(ErrorCode.DATABASE_NOT_FOUND)
        .message("Database '%s' not found at path '%s'", "testdb", "/data")
        .build();
    
    assertEquals("Database 'testdb' not found at path '/data'", ex.getMessage());
  }

  @Test
  void testBuilderWithContext() {
    final ArcadeDBException ex = ExceptionBuilder.create()
        .errorCode(ErrorCode.DATABASE_NOT_FOUND)
        .message("Database not found")
        .addContext("databaseName", "testdb")
        .addContext("path", "/data/databases")
        .addContext("retries", 3)
        .build();
    
    assertEquals(3, ex.getContext().size());
    assertEquals("testdb", ex.getContext().get("databaseName"));
    assertEquals("/data/databases", ex.getContext().get("path"));
    assertEquals(3, ex.getContext().get("retries"));
  }

  @Test
  void testBuilderWithCause() {
    final Exception cause = new RuntimeException("Connection failed");
    final ArcadeDBException ex = ExceptionBuilder.create()
        .errorCode(ErrorCode.NETWORK_ERROR)
        .message("Network error")
        .cause(cause)
        .build();
    
    assertEquals(ErrorCode.NETWORK_ERROR, ex.getErrorCode());
    assertEquals("Network error", ex.getMessage());
    assertEquals(cause, ex.getCause());
  }

  @Test
  void testBuilderWithDefaultMessage() {
    final ArcadeDBException ex = ExceptionBuilder.create()
        .errorCode(ErrorCode.DATABASE_NOT_FOUND)
        .build();
    
    assertEquals("Database not found", ex.getMessage());
  }

  @Test
  void testBuilderWithoutErrorCode() {
    final ArcadeDBException ex = ExceptionBuilder.create()
        .message("Some error")
        .build();
    
    assertEquals(ErrorCode.UNKNOWN_ERROR, ex.getErrorCode());
  }

  @Test
  void testStaticFactoryWithErrorCode() {
    final ArcadeDBException ex = ExceptionBuilder.create(ErrorCode.TRANSACTION_FAILED)
        .message("Transaction failed")
        .build();
    
    assertEquals(ErrorCode.TRANSACTION_FAILED, ex.getErrorCode());
  }

  @Test
  void testBuildAndThrow() {
    assertThrows(ArcadeDBException.class, () -> {
      ExceptionBuilder.create()
          .errorCode(ErrorCode.DATABASE_NOT_FOUND)
          .message("Database not found")
          .buildAndThrow();
    });
  }

  @Test
  void testMethodChaining() {
    final ArcadeDBException ex = ExceptionBuilder.create()
        .errorCode(ErrorCode.QUERY_PARSING_ERROR)
        .message("Invalid query")
        .addContext("query", "SELECT * FROM invalid")
        .addContext("line", 1)
        .addContext("column", 15)
        .build();
    
    assertEquals(ErrorCode.QUERY_PARSING_ERROR, ex.getErrorCode());
    assertEquals(3, ex.getContext().size());
  }

  @Test
  void testNullErrorCode() {
    final ArcadeDBException ex = ExceptionBuilder.create()
        .errorCode(null)
        .message("Test")
        .build();
    
    assertEquals(ErrorCode.UNKNOWN_ERROR, ex.getErrorCode());
  }

  @Test
  void testNullContextKey() {
    final ArcadeDBException ex = ExceptionBuilder.create()
        .errorCode(ErrorCode.DATABASE_NOT_FOUND)
        .addContext(null, "value")
        .build();
    
    assertEquals(0, ex.getContext().size());
  }

  @Test
  void testCompleteExample() {
    final Exception cause = new RuntimeException("Root cause");
    final ArcadeDBException ex = ExceptionBuilder.create()
        .errorCode(ErrorCode.REPLICATION_ERROR)
        .message("Replication failed for database '%s'", "maindb")
        .cause(cause)
        .addContext("database", "maindb")
        .addContext("node", "node1")
        .addContext("attempt", 5)
        .addContext("lastError", "Connection timeout")
        .build();
    
    assertEquals(ErrorCode.REPLICATION_ERROR, ex.getErrorCode());
    assertTrue(ex.getMessage().contains("maindb"));
    assertEquals(cause, ex.getCause());
    assertEquals(4, ex.getContext().size());
    
    final String json = ex.toJSON();
    assertTrue(json.contains("REPLICATION_ERROR"));
    assertTrue(json.contains("database"));
  }
}
