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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Regression tests for {@code parse()}.
 * <p>
 * Issue #6423: the parser used to split a {@code SET <param> = <value>} command on EVERY '=', so a value
 * containing a further '=' (e.g. a connection string) was silently truncated - and, because the
 * quote-stripping that followed assumed the truncated value still ended in the closing quote, it chopped
 * off the value's last character too.
 * <p>
 * Issue #6701: a {@code SET} command prefixed with the PostgreSQL {@code SESSION}/{@code LOCAL} scope
 * modifiers must resolve to the same parameter name as a plain {@code SET}, not a mangled
 * {@code "session <name>"}/{@code "local <name>"}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostgresSetCommandParsingTest {

  @Test
  void valueContainingEqualsIsKeptWhole() {
    assertThat(parse("SET search_path = 'a=b'")).containsExactly("search_path", "a=b");
  }

  @Test
  void unquotedDefaultKeywordIsANullValue() {
    // Issue #8217: the keyword resets the parameter, while the quoted string 'DEFAULT' is an ordinary value.
    assertThat(parse("SET search_path TO DEFAULT")).containsExactly("search_path", null);
    assertThat(parse("SET search_path = default")).containsExactly("search_path", null);
    assertThat(parse("SET application_name = 'DEFAULT'")).containsExactly("application_name", "DEFAULT");
  }

  @Test
  void simpleEqualsAssignment() {
    assertThat(parse("SET datestyle = 'ISO'")).containsExactly("datestyle", "ISO");
  }

  @Test
  void toKeywordAssignment() {
    assertThat(parse("SET datestyle TO 'ISO'")).containsExactly("datestyle", "ISO");
  }

  @Test
  void toKeywordAssignmentIsCaseInsensitiveAndSplitsOnFirstOccurrenceOnly() {
    assertThat(parse("SET search_path to 'a TO b'")).containsExactly("search_path", "a TO b");
  }

  @Test
  void toKeywordAssignmentWithValueContainingEqualsIsKeptWhole() {
    // The command uses ' TO ' as its separator, so the '=' inside the value must NOT be mistaken for
    // the (unused) '=' separator - the earlier-occurring separator wins, not '=' unconditionally.
    assertThat(parse("SET search_path TO 'a=b'")).containsExactly("search_path", "a=b");
  }

  @Test
  void equalsAssignmentWithValueContainingToIsKeptWhole() {
    // Symmetric case: the command uses '=' as its separator, so a ' TO ' inside the value must not be
    // mistaken for the (unused) TO separator.
    assertThat(parse("SET search_path = 'a TO b'")).containsExactly("search_path", "a TO b");
  }

  @Test
  void unquotedValueIsNotStripped() {
    assertThat(parse("SET timezone = UTC")).containsExactly("timezone", "UTC");
  }

  @Test
  void paramNameIsLowerCased() {
    assertThat(parse("SET DateStyle = 'ISO'")).containsExactly("datestyle", "ISO");
  }

  @Test
  void noSeparatorReturnsNull() {
    assertThat(parse("SET justaname")).isNull();
  }

  @Test
  void emptyParamNameIsRejected() {
    assertThat(parse("SET = somevalue")).isNull();
  }

  @Test
  void anUnclosedQuoteIsRejectedInsteadOfThrowing() {
    // A single stray quote is not a closed quoted value: substring(1, length - 1) on it used to throw
    // StringIndexOutOfBoundsException instead of being treated as the malformed command it is.
    assertThatCode(() -> parse("SET x = '")).doesNotThrowAnyException();
    assertThat(parse("SET x = '")).isNull();
  }

  @Test
  void mismatchedQuotesAreRejected() {
    assertThat(parse("SET x = 'abc\"")).isNull();
  }

  @Test
  void sessionModifierIsStrippedWithEquals() {
    final String[] parsed = parse("SET SESSION datestyle = 'ISO'");
    assertThat(parsed).as("SESSION modifier must not become part of the parameter name").containsExactly("datestyle", "ISO");
  }

  @Test
  void sessionModifierIsStrippedWithTo() {
    assertThat(parse("SET SESSION datestyle TO 'ISO'")).containsExactly("datestyle", "ISO");
  }

  @Test
  void localModifierIsStripped() {
    final String[] parsed = parse("SET LOCAL datestyle = 'ISO'");
    assertThat(parsed).as("LOCAL modifier must not become part of the parameter name").containsExactly("datestyle", "ISO");
  }

  @Test
  void modifierIsCaseInsensitive() {
    assertThat(parse("SET session datestyle = 'ISO'")[0]).isEqualTo("datestyle");
    assertThat(parse("SET Local datestyle = 'ISO'")[0]).isEqualTo("datestyle");
  }

  @Test
  void aParamNameThatMerelyStartsWithSessionIsNotMistakenForTheModifier() {
    // "sessiontimeout" must not have its first 8 characters ("session ") sliced off as if they were the
    // SESSION modifier: the modifier match requires a following space, which "sessiontimeout" has not got.
    assertThat(parse("SET sessiontimeout = '30'")).containsExactly("sessiontimeout", "30");
  }

  @Test
  void localModifierIsKeptOnTheResult() {
    // Issue #8242: SET LOCAL lasts until the end of the transaction, so the scope must survive parsing.
    assertThat(PostgresNetworkExecutor.parseSetCommand("SET LOCAL search_path TO x").local()).isTrue();
    assertThat(PostgresNetworkExecutor.parseSetCommand("SET SESSION search_path TO x").local()).isFalse();
    assertThat(PostgresNetworkExecutor.parseSetCommand("SET search_path TO x").local()).isFalse();
  }

  @Test
  void resetIsASetToTheResetValue() {
    // Issue #8242
    assertThat(parse("RESET search_path")).containsExactly("search_path", null);
    assertThat(parse("reset DateStyle")).containsExactly("datestyle", null);
    assertThat(PostgresNetworkExecutor.parseSetCommand("RESET ALL")).isSameAs(PostgresSessionSettings.Assignment.RESET_ALL);
    assertThat(parse("RESET a b")).isNull();
  }

  @Test
  void setTimeZoneIsTheTimezoneParameter() {
    assertThat(parse("SET TIME ZONE 'Europe/Rome'")).containsExactly("timezone", "Europe/Rome");
    assertThat(parse("SET time zone LOCAL")).containsExactly("timezone", null);
    assertThat(parse("SET LOCAL TIME ZONE DEFAULT")).containsExactly("timezone", null);
  }

  private static String[] parse(final String query) {
    final PostgresSessionSettings.Assignment assignment = PostgresNetworkExecutor.parseSetCommand(query);
    return assignment == null ? null : new String[] { assignment.name(), assignment.value() };
  }
}
