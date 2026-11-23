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

import com.arcadedb.database.Database;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

  /*
   * Issue #8392: a {@code SET} this server cannot parse, or cannot honour, used to be logged and answered
   * {@code CommandComplete SET} having applied nothing - {@code SET SESSION AUTHORIZATION} and {@code SET ROLE} included, so
   * a client that dropped privileges was told it had. Unit tests of the resolution both protocols share; the wire answers
   * are in {@code PostgresSessionSettingsIT}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #8392
  @ParameterizedTest
  @ValueSource(strings = { "SET x", "SET justaname", "SET = value", "SET x = '", "RESET a b", "SET CONSTRAINTS ALL",
      "SET TRANSACTION ISOLATION LEVEL", "SET TRANSACTION ISOLATION LEVEL READ", "SET TRANSACTION BOGUS",
      "SET TRANSACTION READ SOMETHING", "SET ROLE 'unterminated" })
  void unparseableSetIsASyntaxError(final String query) {
    assertThatThrownBy(() -> PostgresNetworkExecutor.resolveSetCommand(query))
        .isInstanceOf(PostgresSessionSettings.SettingException.class)
        .hasMessageContaining(query)
        .satisfies(e -> assertThat(((PostgresSessionSettings.SettingException) e).sqlState).isEqualTo("42601"));
  }

  // Regression: #8392
  @Test
  void keywordFormsMapOntoTheirParameters() {
    assertAssignment("SET SESSION AUTHORIZATION 'someone'", "session_authorization", "someone");
    assertAssignment("SET SESSION AUTHORIZATION DEFAULT", "session_authorization", null);
    assertAssignment("SET LOCAL SESSION AUTHORIZATION someone", "session_authorization", "someone");
    assertAssignment("SET ROLE readonly", "role", "readonly");
    assertAssignment("SET SESSION ROLE NONE", "role", "none");
    assertAssignment("SET NAMES 'UTF8'", "client_encoding", "UTF8");
    assertAssignment("SET SCHEMA 'public'", "search_path", "public");
    assertAssignment("SET TRANSACTION ISOLATION LEVEL READ COMMITTED", "transaction_isolation", "read committed");
    assertAssignment("set transaction isolation level repeatable read, read write", "transaction_isolation", "repeatable read");
    assertAssignment("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE", "default_transaction_isolation",
        "serializable");
    assertAssignment("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ UNCOMMITTED NOT DEFERRABLE",
        "default_transaction_isolation", "read uncommitted");
  }

  // Regression: #8392
  @Test
  void parameterFormsOfTheSameNamesStayOnTheOrdinaryPath() {
    assertAssignment("SET role = 'readonly'", "role", "readonly");
    assertAssignment("SET role TO readonly", "role", "readonly");
    assertAssignment("SET transaction_isolation = 'serializable'", "transaction_isolation", "serializable");
    assertAssignment("RESET ROLE", "role", null);
    assertAssignment("RESET SESSION AUTHORIZATION", "session_authorization", null);
  }

  // Regression: #8392
  @Test
  void setConstraintsAllIsAcceptedWithoutEffect() {
    // NO CONSTRAINT OF THIS SERVER IS DEFERRABLE, AND POSTGRESQL ACCEPTS THE STATEMENT WITHOUT EFFECT ON THOSE
    assertThat(PostgresNetworkExecutor.resolveSetCommand("SET CONSTRAINTS ALL DEFERRED")).isSameAs(PostgresSessionSettings.Assignment.NO_OP);
    assertThat(PostgresNetworkExecutor.resolveSetCommand("SET CONSTRAINTS ALL IMMEDIATE")).isSameAs(PostgresSessionSettings.Assignment.NO_OP);
    assertThat(PostgresNetworkExecutor.resolveSetCommand("SET TRANSACTION READ WRITE")).isSameAs(PostgresSessionSettings.Assignment.NO_OP);
  }

  // Regression: #8392
  @Test
  void setConstraintsByNameIsAnUndefinedObject() {
    assertRefused(() -> PostgresNetworkExecutor.resolveSetCommand("SET CONSTRAINTS fk_a, fk_b DEFERRED"), "42704", "fk_a");
  }

  // Regression: #8392
  @Test
  void readOnlyTransactionIsRefused() {
    assertRefused(() -> PostgresNetworkExecutor.resolveSetCommand("SET TRANSACTION READ ONLY"), "0A000", "read-only");
    assertRefused(() -> PostgresNetworkExecutor.resolveSetCommand("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY"), "0A000",
        "read-only");
    assertRefused(() -> PostgresNetworkExecutor.resolveSetCommand("SET TRANSACTION SNAPSHOT '00000003-0000001B-1'"), "0A000",
        "SNAPSHOT");
  }

  // Regression: #8392
  @Test
  void roleAndSessionAuthorizationAreAcceptedOnlyToReset() {
    final PostgresSessionSettings settings = settings();
    assertRefused(() -> settings.apply(PostgresNetworkExecutor.resolveSetCommand("SET ROLE readonly")), "0A000", "SET ROLE");
    assertRefused(() -> settings.apply(PostgresNetworkExecutor.resolveSetCommand("SET SESSION AUTHORIZATION 'someone'")), "0A000",
        "SESSION AUTHORIZATION");
    assertRefused(() -> settings.set("role", "readonly"), "0A000", "SET ROLE");
    assertRefused(() -> settings.set("session_authorization", "someone"), "0A000", "SESSION AUTHORIZATION");

    settings.apply(PostgresNetworkExecutor.resolveSetCommand("SET ROLE NONE"));
    settings.apply(PostgresNetworkExecutor.resolveSetCommand("SET SESSION AUTHORIZATION DEFAULT"));
    settings.apply(PostgresNetworkExecutor.resolveSetCommand("RESET ROLE"));

    assertThat(settings.show("role")).isEqualTo("none");
    assertThat(settings.show("session_authorization")).isEqualTo("alice");
  }

  // Regression: #8392
  @Test
  void isolationIsAcceptedOnlyForTheLevelTransactionsRunAt() {
    final PostgresSessionSettings settings = settings();
    settings.apply(PostgresNetworkExecutor.resolveSetCommand("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"));
    // POSTGRESQL'S READ UNCOMMITTED BEHAVES AS READ COMMITTED
    settings.apply(PostgresNetworkExecutor.resolveSetCommand("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"));
    settings.set("default_transaction_isolation", "read committed");

    assertRefused(() -> settings.apply(PostgresNetworkExecutor.resolveSetCommand("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")),
        "0A000", "serializable");
    assertRefused(() -> settings.apply(
        PostgresNetworkExecutor.resolveSetCommand("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ")), "0A000",
        "repeatable read");
    assertRefused(() -> settings.set("transaction_isolation", "chaos"), "22023", "chaos");

    assertThat(settings.show("transaction_isolation")).isEqualTo("read committed");
    assertThat(settings.show("default_transaction_isolation")).isEqualTo("read committed");
  }

  // Regression: #8392
  @Test
  void isolationFollowsTheOpenTransaction() {
    final PostgresSessionSettings settings = settings();
    // A BEGIN ISOLATION LEVEL REPEATABLE READ BLOCK: THE OPEN TRANSACTION RUNS AT A LEVEL OTHER THAN THE DEFAULT ONE
    settings.setIsolationLevels(() -> Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ,
        () -> Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);

    settings.apply(PostgresNetworkExecutor.resolveSetCommand("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"));
    assertRefused(() -> settings.apply(
        PostgresNetworkExecutor.resolveSetCommand("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ")), "0A000",
        "read committed");
    assertThat(settings.show("transaction_isolation")).isEqualTo("repeatable read");
    assertThat(settings.show("default_transaction_isolation")).isEqualTo("read committed");
  }

  // Regression: #8392
  @Test
  void readOnlyParametersAcceptOnlyOff() {
    final PostgresSessionSettings settings = settings();
    settings.set("transaction_read_only", "off");
    settings.set("default_transaction_read_only", "false");
    assertRefused(() -> settings.set("transaction_read_only", "on"), "0A000", "read-only");
    assertRefused(() -> settings.set("default_transaction_read_only", "true"), "0A000", "read-only");
    assertRefused(() -> settings.set("default_transaction_read_only", "maybe"), "22023", "Boolean");
    assertThat(settings.show("transaction_read_only")).isEqualTo("off");
  }

  private static PostgresSessionSettings settings() {
    final PostgresSessionSettings settings = new PostgresSessionSettings();
    settings.setSessionUser("alice");
    return settings;
  }

  private static void assertAssignment(final String query, final String name, final String value) {
    final PostgresSessionSettings.Assignment assignment = PostgresNetworkExecutor.resolveSetCommand(query);
    assertThat(assignment.name()).as(query).isEqualTo(name);
    assertThat(assignment.value()).as(query).isEqualTo(value);
  }

  private static void assertRefused(final Runnable action, final String sqlState, final String messagePart) {
    assertThatThrownBy(action::run)
        .isInstanceOf(PostgresSessionSettings.SettingException.class)
        .hasMessageContaining(messagePart)
        .satisfies(e -> assertThat(((PostgresSessionSettings.SettingException) e).sqlState).isEqualTo(sqlState));
  }
}
