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

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.UnknownFunctionException;
import com.arcadedb.exception.ValidationException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11314 on Bolt: a value a declared constraint refuses is Neo4j's ConstraintValidationFailed, what Neo4j answers for a
 * violated property existence or type constraint, where it was the ArgumentError of every bad argument; and a call of an unknown
 * SQL function is the SyntaxError Neo4j answers an unknown function with, where it was a server error.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11314BoltConstraintCodeTest {

  @Test
  void aViolatedDeclaredConstraintIsConstraintValidationFailed() {
    for (final ValidationException.Kind kind : ValidationException.Kind.values()) {
      final String code = code(new CommandExecutionException("Error", new ValidationException(kind, "refused")));
      assertThat(code).as("%s", kind)
          .isEqualTo(kind.isDeclaredConstraint() ? BoltErrorCodes.CONSTRAINT_VIOLATION_ERROR : BoltErrorCodes.ARGUMENT_ERROR);
    }
  }

  @Test
  void aRefusalOfNoDeclaredConstraintStaysAnArgumentError() {
    assertThat(code(new ValidationException("refused"))).isEqualTo(BoltErrorCodes.ARGUMENT_ERROR);
  }

  @Test
  void anUnknownSqlFunctionIsASyntaxError() {
    assertThat(code(new UnknownFunctionException("foo", "Unknown function name 'foo'"))).isEqualTo(BoltErrorCodes.SYNTAX_ERROR);
  }

  private static String code(final Throwable error) {
    return BoltNetworkExecutor.classifyExecutionError(error, BoltErrorCodes.DATABASE_ERROR);
  }
}
