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
package com.arcadedb.bolt;

import com.arcadedb.exception.ArcadeDBException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for BoltException covering all constructors and methods.
 */
class BoltExceptionTest {

  @Test
  void constructorWithMessage() {
    final BoltException ex = new BoltException("Test error");
    assertThat(ex.getMessage()).isEqualTo("Test error");
    assertThat(ex.getErrorCode()).isEqualTo(BoltErrorCodes.DATABASE_ERROR);
  }

  @Test
  void constructorWithCodeAndMessage() {
    final BoltException ex = new BoltException(BoltErrorCodes.SYNTAX_ERROR, "Syntax problem");
    assertThat(ex.getMessage()).isEqualTo("Syntax problem");
    assertThat(ex.getErrorCode()).isEqualTo(BoltErrorCodes.SYNTAX_ERROR);
  }

  @Test
  void constructorWithMessageAndCause() {
    final RuntimeException cause = new RuntimeException("Root cause");
    final BoltException ex = new BoltException("Wrapped error", cause);
    assertThat(ex.getMessage()).isEqualTo("Wrapped error");
    assertThat(ex.getErrorCode()).isEqualTo(BoltErrorCodes.DATABASE_ERROR);
    assertThat(ex.getCause()).isEqualTo(cause);
  }

  @Test
  void constructorWithCodeMessageAndCause() {
    final RuntimeException cause = new RuntimeException("Root cause");
    final BoltException ex = new BoltException(BoltErrorCodes.AUTHENTICATION_ERROR, "Auth failed", cause);
    assertThat(ex.getMessage()).isEqualTo("Auth failed");
    assertThat(ex.getErrorCode()).isEqualTo(BoltErrorCodes.AUTHENTICATION_ERROR);
    assertThat(ex.getCause()).isEqualTo(cause);
  }

  @Test
  void deprecatedStaticFields() {
    // Test deprecated static fields for backward compatibility
    assertThat(BoltException.AUTHENTICATION_ERROR).isEqualTo(BoltErrorCodes.AUTHENTICATION_ERROR);
    assertThat(BoltException.SYNTAX_ERROR).isEqualTo(BoltErrorCodes.SYNTAX_ERROR);
    assertThat(BoltException.SEMANTIC_ERROR).isEqualTo(BoltErrorCodes.SEMANTIC_ERROR);
    assertThat(BoltException.DATABASE_ERROR).isEqualTo(BoltErrorCodes.DATABASE_ERROR);
    assertThat(BoltException.TRANSACTION_ERROR).isEqualTo(BoltErrorCodes.TRANSACTION_ERROR);
    assertThat(BoltException.FORBIDDEN_ERROR).isEqualTo(BoltErrorCodes.FORBIDDEN_ERROR);
    assertThat(BoltException.PROTOCOL_ERROR).isEqualTo(BoltErrorCodes.PROTOCOL_ERROR);
  }

  @Test
  void allErrorCodes() {
    // Test all error codes from BoltErrorCodes
    assertThat(BoltErrorCodes.AUTHENTICATION_ERROR).isEqualTo("Neo.ClientError.Security.Unauthorized");
    assertThat(BoltErrorCodes.SYNTAX_ERROR).isEqualTo("Neo.ClientError.Statement.SyntaxError");
    assertThat(BoltErrorCodes.SEMANTIC_ERROR).isEqualTo("Neo.ClientError.Statement.SemanticError");
    assertThat(BoltErrorCodes.DATABASE_ERROR).isEqualTo("Neo.DatabaseError.General.UnknownError");
    assertThat(BoltErrorCodes.TRANSACTION_ERROR).isEqualTo("Neo.ClientError.Transaction.TransactionNotFound");
    assertThat(BoltErrorCodes.FORBIDDEN_ERROR).isEqualTo("Neo.ClientError.Security.Forbidden");
    assertThat(BoltErrorCodes.PROTOCOL_ERROR).isEqualTo("Neo.ClientError.Request.Invalid");
  }

  @Test
  void exceptionIsArcadeDBException() {
    final BoltException ex = new BoltException("test");
    assertThat(ex).isInstanceOf(ArcadeDBException.class);
  }
}
