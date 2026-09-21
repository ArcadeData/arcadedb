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
package com.arcadedb.query.sql.parser;

import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Regression test for issue #8061: {@code DeleteFunctionStatement.copy()} built and returned a
 * {@code DefineFunctionStatement} - the opposite statement, one that REGISTERS a function rather than deleting one -
 * and {@code equals()} cast its operand to {@code DefineFunctionStatement} right after a {@code getClass()} guard
 * that proved it was actually a {@code DeleteFunctionStatement}, throwing {@code ClassCastException}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8061DeleteFunctionStatementCopyAndEqualsTest {

  private static DeleteFunctionStatement parse(final String sql) {
    return (DeleteFunctionStatement) new SQLAntlrParser(null).parse(sql);
  }

  @Test
  void copyReturnsADeleteFunctionStatementNotADefineFunctionStatement() {
    final DeleteFunctionStatement original = parse("DELETE FUNCTION lib.fn");

    final Statement copy = original.copy();

    assertThat(copy).isInstanceOf(DeleteFunctionStatement.class);
    assertThat(copy.toString()).isEqualTo("DELETE FUNCTION lib.fn");
  }

  @Test
  void copyRoundTripsThroughAnIfStatementBody() {
    final IfStatement ifStatement = (IfStatement) new SQLAntlrParser(null).parse("IF (1 = 1) { DELETE FUNCTION lib.fn; }");

    final IfStatement copy = ifStatement.copy();

    assertThat(copy.toString()).contains("DELETE FUNCTION lib.fn");
    assertThat(copy.toString()).doesNotContain("DEFINE FUNCTION");
  }

  @Test
  void equalsComparesTwoDeleteFunctionStatementsWithoutThrowing() {
    final DeleteFunctionStatement a = parse("DELETE FUNCTION lib.fn");
    final DeleteFunctionStatement b = parse("DELETE FUNCTION lib.fn");
    final DeleteFunctionStatement c = parse("DELETE FUNCTION lib.other");

    assertThatCode(() -> a.equals(b)).doesNotThrowAnyException();
    assertThat(a).isEqualTo(b);
    assertThat(a).isNotEqualTo(c);
  }
}
