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

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.TypeMismatchException;
import com.arcadedb.exception.UnknownFunctionException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.exception.ValueOutOfRangeException;
import org.junit.jupiter.api.Test;

import java.time.format.DateTimeParseException;
import java.util.EnumMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11314: every refused value reached a Postgres client as {@code 22023} invalid_parameter_value and a call of an unknown
 * SQL function as {@code XX000} internal_error. They are the SQLSTATEs PostgreSQL answers the same refusal with now.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11314PostgresSqlStateTest {
  private static final Map<ValidationException.Kind, String> EXPECTED = new EnumMap<>(Map.of(//
      ValidationException.Kind.MANDATORY, "23502", //
      ValidationException.Kind.NOT_NULL, "23502", //
      ValidationException.Kind.MIN, "23514", //
      ValidationException.Kind.MAX, "23514", //
      ValidationException.Kind.REGEXP, "23514", //
      ValidationException.Kind.READONLY, "23514", //
      ValidationException.Kind.MAX_LENGTH, "22001", //
      ValidationException.Kind.TYPE, "42804", //
      ValidationException.Kind.ENCODING, "22021"));

  @Test
  void everyConstraintKindHasThePostgresCodeOfTheSameViolation() {
    // ranges over the enum, so a kind added later without a code here is a red build rather than a silent 22023
    for (final ValidationException.Kind kind : ValidationException.Kind.values()) {
      assertThat(EXPECTED).as("no SQLSTATE expected for %s", kind).containsKey(kind);
      assertThat(PostgresNetworkExecutor.sqlStateFor(new ValidationException(kind, "refused"))).as("%s", kind).isEqualTo(EXPECTED.get(kind));
      // the same through the wrapper an engine puts around what it raises
      assertThat(PostgresNetworkExecutor.sqlStateFor(new CommandExecutionException("Error", new ValidationException(kind, "refused"))))
          .as("%s wrapped", kind).isEqualTo(EXPECTED.get(kind));
    }
  }

  @Test
  void aRefusedConversionHasTheCodeOfWhatItFailed() {
    assertThat(PostgresNetworkExecutor.sqlStateFor(new IllegalArgumentException("Cannot convert",
        new ValueOutOfRangeException("Value '99999999999' is out of range for type INTEGER")))).isEqualTo("22003");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new IllegalArgumentException("Cannot convert",
        new NumberFormatException("For input string: \"abc\"")))).isEqualTo("22P02");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new IllegalArgumentException("Cannot convert",
        new IllegalArgumentException("Error in conversion", new DateTimeParseException("Text 'x' could not be parsed", "x", 0)))))
        .isEqualTo("22007");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new TypeMismatchException("Cannot read a property of a list"))).isEqualTo("42804");
  }

  @Test
  void aRefusalThatIsNoDeclaredConstraintKeepsInvalidParameterValue() {
    assertThat(PostgresNetworkExecutor.sqlStateFor(new ValidationException("refused"))).isEqualTo("22023");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new IllegalArgumentException("bad argument"))).isEqualTo("22023");
  }

  @Test
  void anUnknownFunctionIsUndefinedFunctionAndAnyOtherParseFailureASyntaxError() {
    assertThat(PostgresNetworkExecutor.sqlStateFor(new UnknownFunctionException("foo", "Unknown function name 'foo'"))).isEqualTo("42883");
    // openCypher's parse-time refusal carries the same cause
    assertThat(PostgresNetworkExecutor.sqlStateFor(new CommandParsingException("UnknownFunction: Unknown function 'foo'",
        new UnknownFunctionException("foo", "UnknownFunction: Unknown function 'foo'")))).isEqualTo("42883");
    assertThat(PostgresNetworkExecutor.sqlStateFor(new CommandParsingException("syntax error"))).isEqualTo("42601");
  }
}
