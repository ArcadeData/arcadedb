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
package com.arcadedb.query.sql.method;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.SQLMethod;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for #5885: calling one of the 19 listed SQL methods with too few arguments, or with more than the
 * declared maximum, used to either surface a raw JDK exception or be silently accepted, because
 * {@link com.arcadedb.query.sql.parser.MethodCall} never called {@link SQLMethod#checkArity} before invoking the
 * method - even though {@link AbstractSQLMethod} already stored the correct {@code minParams}/{@code maxParams} for
 * every one of them (only {@code ifempty} had them wrong).
 * <p>
 * Driven off the declared bounds themselves (not a hand-picked argument count per method), so a method whose
 * declaration regresses stays covered.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MethodArgumentValidationRegressionTest extends TestHelper {

  /**
   * The 19 methods #5885 found affected, called against a plain string receiver.
   */
  @ParameterizedTest
  @ValueSource(strings = { //
      "append", "asdate", "asdatetime", "charat", "convert", "field", "format", "ifempty", "include", //
      "indexof", "lastindexof", "left", "prefix", "replace", "right", "split", "substring", "trimprefix", //
      "trimsuffix" })
  void tooFewArgumentsIsAValidationErrorNotARawException(final String methodName) {
    final SQLMethod method = methodInstance(methodName);
    final int minParams = method.getMinParams();
    if (minParams == 0)
      return; // nothing to under-supply

    final String query = "SELECT 'abc'." + methodName + "(" + placeholders(minParams - 1) + ") AS r";
    assertThatThrownBy(() -> consume(query)) //
        .as("%s called with %d (< %d required) arguments", methodName, minParams - 1, minParams) //
        .isInstanceOf(CommandSemanticException.class) //
        .hasMessageContaining(methodName);
  }

  @ParameterizedTest
  @ValueSource(strings = { //
      "append", "asdate", "asdatetime", "charat", "convert", "field", "format", "ifempty", "include", //
      "indexof", "lastindexof", "left", "prefix", "replace", "right", "split", "substring", "trimprefix", //
      "trimsuffix" })
  void tooManyArgumentsIsAValidationError(final String methodName) {
    final SQLMethod method = methodInstance(methodName);
    final int maxParams = method.getMaxParams();
    if (maxParams < 0 || maxParams == Integer.MAX_VALUE)
      return; // variadic: no upper bound to exceed

    final String query = "SELECT 'abc'." + methodName + "(" + placeholders(maxParams + 1) + ") AS r";
    assertThatThrownBy(() -> consume(query)) //
        .as("%s called with %d (> %d allowed) arguments", methodName, maxParams + 1, maxParams) //
        .isInstanceOf(CommandSemanticException.class) //
        .hasMessageContaining(methodName);
  }

  @Test
  void ifEmptyDeclaresExactlyOneRequiredParameter() {
    // The one method in the 19 whose declared bounds (not just their enforcement) were wrong: super(NAME) defaulted
    // to (0, 0), while execute() unconditionally reads params[0].
    final SQLMethod ifEmpty = methodInstance("ifempty");
    assertThat(ifEmpty.getMinParams()).isEqualTo(1);
    assertThat(ifEmpty.getMaxParams()).isEqualTo(1);
  }

  @Test
  void leftWithANegativeLengthReturnsEmptyRatherThanThrowing() {
    assertThatCode(() -> assertThat(single("SELECT 'abcdef'.left(-3) AS r")).isEqualTo("")) //
        .doesNotThrowAnyException();
  }

  @Test
  void rightWithANegativeOffsetReturnsEmptyRatherThanThrowing() {
    assertThatCode(() -> assertThat(single("SELECT 'abcdef'.right(-3) AS r")).isEqualTo("")) //
        .doesNotThrowAnyException();
  }

  @Test
  void charAtWithANegativeOrOutOfRangeIndexReturnsNullRatherThanThrowing() {
    assertThatCode(() -> assertThat(single("SELECT 'abcdef'.charat(-1) AS r")).isNull()) //
        .doesNotThrowAnyException();
    assertThatCode(() -> assertThat(single("SELECT 'abcdef'.charat(99) AS r")).isNull()) //
        .doesNotThrowAnyException();
  }

  private static SQLMethod methodInstance(final String name) {
    return DefaultSQLMethodFactory.getInstance().createMethod(name);
  }

  private static String placeholders(final int count) {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < count; i++) {
      if (i > 0)
        sb.append(", ");
      sb.append("'x'");
    }
    return sb.toString();
  }

  private Object single(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next().getProperty("r");
    }
  }

  private void consume(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext())
        rs.next();
    }
  }
}
