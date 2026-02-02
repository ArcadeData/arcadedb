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
package com.arcadedb.postgres;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for PostgresProtocolException.
 */
class PostgresProtocolExceptionTest {

  @Test
  void constructorWithMessage() {
    PostgresProtocolException exception = new PostgresProtocolException("Test error message");
    assertThat(exception.getMessage()).isEqualTo("Test error message");
    assertThat(exception.getCause()).isNull();
  }

  @Test
  void constructorWithMessageAndCause() {
    RuntimeException cause = new RuntimeException("Root cause");
    PostgresProtocolException exception = new PostgresProtocolException("Wrapper message", cause);
    assertThat(exception.getMessage()).isEqualTo("Wrapper message");
    assertThat(exception.getCause()).isEqualTo(cause);
    assertThat(exception.getCause().getMessage()).isEqualTo("Root cause");
  }

  @Test
  void exceptionIsRuntimeException() {
    PostgresProtocolException exception = new PostgresProtocolException("Test");
    assertThat(exception).isInstanceOf(RuntimeException.class);
  }

  @Test
  void exceptionWithIOExceptionCause() {
    IOException ioCause = new IOException("IO error");
    PostgresProtocolException exception = new PostgresProtocolException("Protocol error", ioCause);
    assertThat(exception.getCause()).isInstanceOf(IOException.class);
  }
}
