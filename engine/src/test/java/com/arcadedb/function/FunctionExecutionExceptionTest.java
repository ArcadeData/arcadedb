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
package com.arcadedb.function;

import com.arcadedb.exception.ArcadeDBException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FunctionExecutionExceptionTest {

  @Test
  void constructorWithMessage() {
    final FunctionExecutionException ex = new FunctionExecutionException("Function failed");

    assertThat(ex.getMessage()).isEqualTo("Function failed");
    assertThat(ex.getCause()).isNull();
    assertThat(ex).isInstanceOf(ArcadeDBException.class);
  }

  @Test
  void constructorWithMessageAndCause() {
    final RuntimeException cause = new RuntimeException("Root cause");
    final FunctionExecutionException ex = new FunctionExecutionException("Function failed", cause);

    assertThat(ex.getMessage()).isEqualTo("Function failed");
    assertThat(ex.getCause()).isSameAs(cause);
  }

  @Test
  void constructorWithCause() {
    final RuntimeException cause = new RuntimeException("Root cause");
    final FunctionExecutionException ex = new FunctionExecutionException(cause);

    assertThat(ex.getCause()).isSameAs(cause);
    assertThat(ex.getMessage()).contains("Root cause");
  }

  @Test
  void exceptionIsRuntimeException() {
    final FunctionExecutionException ex = new FunctionExecutionException("Test");

    assertThat(ex).isInstanceOf(RuntimeException.class);
  }

  @Test
  void canBeThrownAndCaught() {
    try {
      throw new FunctionExecutionException("Test error");
    } catch (final FunctionExecutionException e) {
      assertThat(e.getMessage()).isEqualTo("Test error");
    }
  }
}
