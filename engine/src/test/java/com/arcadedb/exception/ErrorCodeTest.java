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
 * Tests for ErrorCode enum.
 */
class ErrorCodeTest {

  @Test
  void testErrorCodeCategories() {
    assertEquals("Database", ErrorCode.DATABASE_NOT_FOUND.getCategory());
    assertEquals("Transaction", ErrorCode.TRANSACTION_FAILED.getCategory());
    assertEquals("Query", ErrorCode.QUERY_PARSING_ERROR.getCategory());
    assertEquals("Security", ErrorCode.AUTHENTICATION_FAILED.getCategory());
    assertEquals("I/O", ErrorCode.IO_ERROR.getCategory());
    assertEquals("Network", ErrorCode.NETWORK_ERROR.getCategory());
  }

  @Test
  void testErrorCodeRanges() {
    assertTrue(ErrorCode.DATABASE_NOT_FOUND.getCode() >= 1000 && ErrorCode.DATABASE_NOT_FOUND.getCode() < 2000);
    assertTrue(ErrorCode.TRANSACTION_FAILED.getCode() >= 2000 && ErrorCode.TRANSACTION_FAILED.getCode() < 3000);
    assertTrue(ErrorCode.QUERY_PARSING_ERROR.getCode() >= 3000 && ErrorCode.QUERY_PARSING_ERROR.getCode() < 4000);
    assertTrue(ErrorCode.AUTHENTICATION_FAILED.getCode() >= 4000 && ErrorCode.AUTHENTICATION_FAILED.getCode() < 5000);
    assertTrue(ErrorCode.IO_ERROR.getCode() >= 5000 && ErrorCode.IO_ERROR.getCode() < 6000);
    assertTrue(ErrorCode.NETWORK_ERROR.getCode() >= 6000 && ErrorCode.NETWORK_ERROR.getCode() < 7000);
  }

  @Test
  void testErrorCodeDescriptions() {
    assertNotNull(ErrorCode.DATABASE_NOT_FOUND.getDescription());
    assertFalse(ErrorCode.DATABASE_NOT_FOUND.getDescription().isEmpty());
    assertEquals("Database not found", ErrorCode.DATABASE_NOT_FOUND.getDescription());
  }

  @Test
  void testErrorCodeToString() {
    final String str = ErrorCode.DATABASE_NOT_FOUND.toString();
    assertTrue(str.contains("DATABASE_NOT_FOUND"));
    assertTrue(str.contains("1001"));
    assertTrue(str.contains("Database not found"));
  }

  @Test
  void testAllErrorCodesHaveValidCategories() {
    for (final ErrorCode errorCode : ErrorCode.values()) {
      assertNotNull(errorCode.getCategory());
      assertFalse(errorCode.getCategory().isEmpty());
    }
  }

  @Test
  void testUnknownErrorCode() {
    assertEquals(9999, ErrorCode.UNKNOWN_ERROR.getCode());
    assertEquals("Unknown", ErrorCode.UNKNOWN_ERROR.getCategory());
  }
}
