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
package com.arcadedb.function.math;

import com.arcadedb.query.sql.executor.CommandContext;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4556: {@code AbstractMathFunction.asDouble(null)} must not silently
 * coerce null into {@code 0.0}. Per the Cypher spec (and matching PostgreSQL/MySQL, where math
 * functions are null-in null-out) null must propagate, so the helper now returns a boxed
 * {@code null} that callers short-circuit into a null result instead of computing on a phantom 0.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AbstractMathFunctionTest {

  /** Minimal concrete subclass to exercise the protected helper. */
  private static class TestMathFunction extends AbstractMathFunction {
    @Override
    protected String getSimpleName() {
      return "test";
    }

    @Override
    public Object execute(final Object[] args, final CommandContext context) {
      return asDouble(args[0]);
    }

    Double callAsDouble(final Object arg) {
      return asDouble(arg);
    }
  }

  @Test
  void asDoubleReturnsNullOnNullInsteadOfZero() {
    final TestMathFunction fn = new TestMathFunction();

    assertThat(fn.callAsDouble(null)).isNull();
  }

  @Test
  void asDoubleAcceptsNumbers() {
    final TestMathFunction fn = new TestMathFunction();

    assertThat(fn.callAsDouble(42)).isEqualTo(42.0);
    assertThat(fn.callAsDouble(3.5)).isEqualTo(3.5);
    assertThat(fn.callAsDouble(7L)).isEqualTo(7.0);
  }

  @Test
  void asDoubleParsesNumericStrings() {
    final TestMathFunction fn = new TestMathFunction();

    assertThat(fn.callAsDouble("2.5")).isEqualTo(2.5);
  }
}
