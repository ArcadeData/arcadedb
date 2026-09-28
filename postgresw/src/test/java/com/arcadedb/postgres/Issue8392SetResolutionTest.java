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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8392: a {@code SET} this server cannot parse, or cannot honour, used to be logged and answered
 * {@code CommandComplete SET} having applied nothing - {@code SET SESSION AUTHORIZATION} and {@code SET ROLE} included, so
 * a client that dropped privileges was told it had. Unit tests of the resolution both protocols share; the wire answers
 * are in {@code Issue8392UnparseableSetIsRefusedIT}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8392SetResolutionTest {

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

  @Test
  void parameterFormsOfTheSameNamesStayOnTheOrdinaryPath() {
    assertAssignment("SET role = 'readonly'", "role", "readonly");
    assertAssignment("SET role TO readonly", "role", "readonly");
    assertAssignment("SET transaction_isolation = 'serializable'", "transaction_isolation", "serializable");
    assertAssignment("RESET ROLE", "role", null);
    assertAssignment("RESET SESSION AUTHORIZATION", "session_authorization", null);
  }

  @Test
  void setConstraintsAllIsAcceptedWithoutEffect() {
    // NO CONSTRAINT OF THIS SERVER IS DEFERRABLE, AND POSTGRESQL ACCEPTS THE STATEMENT WITHOUT EFFECT ON THOSE
    assertThat(PostgresNetworkExecutor.resolveSetCommand("SET CONSTRAINTS ALL DEFERRED")).isSameAs(PostgresSessionSettings.Assignment.NO_OP);
    assertThat(PostgresNetworkExecutor.resolveSetCommand("SET CONSTRAINTS ALL IMMEDIATE")).isSameAs(PostgresSessionSettings.Assignment.NO_OP);
    assertThat(PostgresNetworkExecutor.resolveSetCommand("SET TRANSACTION READ WRITE")).isSameAs(PostgresSessionSettings.Assignment.NO_OP);
  }

  @Test
  void setConstraintsByNameIsAnUndefinedObject() {
    assertRefused(() -> PostgresNetworkExecutor.resolveSetCommand("SET CONSTRAINTS fk_a, fk_b DEFERRED"), "42704", "fk_a");
  }

  @Test
  void readOnlyTransactionIsRefused() {
    assertRefused(() -> PostgresNetworkExecutor.resolveSetCommand("SET TRANSACTION READ ONLY"), "0A000", "read-only");
    assertRefused(() -> PostgresNetworkExecutor.resolveSetCommand("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY"), "0A000",
        "read-only");
    assertRefused(() -> PostgresNetworkExecutor.resolveSetCommand("SET TRANSACTION SNAPSHOT '00000003-0000001B-1'"), "0A000",
        "SNAPSHOT");
  }

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
