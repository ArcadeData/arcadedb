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

import static com.arcadedb.postgres.PostgresPortal.TransactionControl.BEGIN;
import static com.arcadedb.postgres.PostgresPortal.TransactionControl.COMMIT;
import static com.arcadedb.postgres.PostgresPortal.TransactionControl.ROLLBACK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9631: PostgreSQL's own spellings of the transaction-control statements, which the SQL grammar refuses.
 */
class PostgresTransactionStatementTest {

  @ParameterizedTest
  @ValueSource(strings = { "BEGIN", "BEGIN WORK", "BEGIN TRANSACTION", "BEGIN READ WRITE", "BEGIN NOT DEFERRABLE",
      "BEGIN TRANSACTION DEFERRABLE", "BEGIN READ WRITE, NOT DEFERRABLE" })
  void beginWithoutIsolation(final String text) {
    final PostgresTransactionStatement statement = PostgresTransactionStatement.parse(text);
    assertThat(statement).isNotNull();
    assertThat(statement.control).isEqualTo(BEGIN);
    assertThat(statement.isolation).isNull();
    assertThat(statement.tag).isEqualTo("BEGIN");
    assertThat(statement.refusal()).isNull();
    assertThat(statement.endsBlock()).isFalse();
  }

  @Test
  void beginWithIsolationLevel() {
    assertThat(PostgresTransactionStatement.parse("BEGIN ISOLATION LEVEL READ COMMITTED").isolation)
        .isEqualTo(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    assertThat(PostgresTransactionStatement.parse("BEGIN ISOLATION LEVEL READ UNCOMMITTED").isolation)
        .isEqualTo(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    // WHAT GO'S database/sql SENDS FOR BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
    final PostgresTransactionStatement repeatable = PostgresTransactionStatement.parse("BEGIN ISOLATION LEVEL REPEATABLE READ");
    assertThat(repeatable.isolation).isEqualTo(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    assertThat(repeatable.refusal()).isNull();
    assertThat(PostgresTransactionStatement.parse("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ WRITE").isolation)
        .isEqualTo(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    assertThat(PostgresTransactionStatement.parse("BEGIN ISOLATION LEVEL READ COMMITTED READ WRITE NOT DEFERRABLE").isolation)
        .isEqualTo(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
  }

  @Test
  void startTransaction() {
    final PostgresTransactionStatement plain = PostgresTransactionStatement.parse("START TRANSACTION");
    assertThat(plain.control).isEqualTo(BEGIN);
    assertThat(plain.tag).isEqualTo("START TRANSACTION");
    assertThat(plain.isolation).isNull();

    final PostgresTransactionStatement withModes = PostgresTransactionStatement.parse("START TRANSACTION ISOLATION LEVEL READ COMMITTED, READ WRITE");
    assertThat(withModes.control).isEqualTo(BEGIN);
    assertThat(withModes.tag).isEqualTo("START TRANSACTION");
    assertThat(withModes.isolation).isEqualTo(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    assertThat(withModes.refusal()).isNull();
  }

  @ParameterizedTest
  @ValueSource(strings = { "BEGIN READ ONLY", "BEGIN ISOLATION LEVEL SERIALIZABLE", "BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE",
      "START TRANSACTION READ ONLY", "START TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE" })
  void unsupportedBeginModesAreRefusedWithFeatureNotSupported(final String text) {
    final PostgresTransactionStatement statement = PostgresTransactionStatement.parse(text);
    assertThat(statement).as(text + " is recognized, not left to the grammar").isNotNull();
    assertThat(statement.refusal()).isNotNull();
    assertThat(statement.refusal().sqlState).isEqualTo("0A000");
    assertThatThrownBy(statement::checkSupported).isSameAs(statement.refusal());
  }

  @ParameterizedTest
  @ValueSource(strings = { "COMMIT", "COMMIT WORK", "COMMIT TRANSACTION", "END", "END WORK", "END TRANSACTION", "COMMIT AND NO CHAIN",
      "END TRANSACTION AND NO CHAIN" })
  void commitForms(final String text) {
    final PostgresTransactionStatement statement = PostgresTransactionStatement.parse(text);
    assertThat(statement.control).isEqualTo(COMMIT);
    assertThat(statement.tag).isEqualTo("COMMIT");
    assertThat(statement.refusal()).isNull();
    assertThat(statement.endsBlock()).isTrue();
  }

  @ParameterizedTest
  @ValueSource(strings = { "ROLLBACK", "ROLLBACK WORK", "ROLLBACK TRANSACTION", "ABORT", "ABORT WORK", "ABORT TRANSACTION",
      "ROLLBACK AND NO CHAIN", "ABORT AND NO CHAIN" })
  void rollbackForms(final String text) {
    final PostgresTransactionStatement statement = PostgresTransactionStatement.parse(text);
    assertThat(statement.control).isEqualTo(ROLLBACK);
    assertThat(statement.tag).isEqualTo("ROLLBACK");
    assertThat(statement.refusal()).isNull();
    assertThat(statement.endsBlock()).isTrue();
  }

  @ParameterizedTest
  @ValueSource(strings = { "COMMIT AND CHAIN", "END WORK AND CHAIN", "ROLLBACK AND CHAIN", "ABORT TRANSACTION AND CHAIN" })
  void andChainIsRefusedWithFeatureNotSupported(final String text) {
    final PostgresTransactionStatement statement = PostgresTransactionStatement.parse(text);
    assertThat(statement).isNotNull();
    assertThat(statement.refusal().sqlState).isEqualTo("0A000");
    assertThat(statement.endsBlock()).as("an AND CHAIN this server cannot honor does not end an aborted block").isFalse();
  }

  /**
   * Left to the SQL grammar fallback (issue #8273) or to the engine: ArcadeDB's own spellings, comments, savepoints and
   * anything that merely starts with one of the keywords.
   */
  @ParameterizedTest
  @ValueSource(strings = { "", "BEGIN ISOLATION READ_COMMITTED", "BEGIN ISOLATION REPEATABLE_READ", "COMMIT RETRY 3", "BEGIN -- GO",
      "/* HI */ BEGIN", "ROLLBACK TO SAVEPOINT S1", "ROLLBACK TRANSACTION TO S1", "START", "START WORK", "BEGINS", "ABORTED",
      "COMMIT AND", "COMMIT AND NO", "COMMIT AND CHAIN NOW", "COMMIT, AND CHAIN", "BEGIN, READ WRITE", "BEGIN ISOLATION LEVEL",
      "SELECT 1", "ENDPOINT", "CREATE VERTEX TYPE BEGIN", "BEGIN READ", "BEGIN,", "COMMIT,", "ABORT;", "STARTTRANSACTION",
      "BEGIN TRANSACTION TRANSACTION", "BEGIN WORK WORK", "START TRANSACTION TRANSACTION", "END AND CHAIN NOW", "ROLLBACK AND NO CHAIN X",
      "COMMIT WORK TRANSACTION", "ROLLBACK TO S1", "BEGIN TRANSACTION,READ WRITE" })
  void otherTextIsNotRecognized(final String text) {
    assertThat(PostgresTransactionStatement.parse(text)).as(text).isNull();
  }

  @Test
  void mixedWhitespaceSeparatesTokens() {
    assertThat(PostgresTransactionStatement.parse("BEGIN\tWORK").control).isEqualTo(BEGIN);
    assertThat(PostgresTransactionStatement.parse("START\nTRANSACTION").tag).isEqualTo("START TRANSACTION");
    assertThat(PostgresTransactionStatement.parse("COMMIT\nAND  NO\tCHAIN").refusal()).isNull();
    assertThat(PostgresTransactionStatement.parse("ABORT \t AND\r\nCHAIN").refusal().sqlState).isEqualTo("0A000");
    assertThat(PostgresTransactionStatement.parse("BEGIN\nISOLATION\tLEVEL  REPEATABLE\r\nREAD").isolation)
        .isEqualTo(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
  }

  /**
   * PostgreSQL separates transaction modes with a comma or with whitespace, and refuses a comma that separates nothing.
   */
  @Test
  void modeSeparatorLeniency() {
    assertThat(PostgresTransactionStatement.parse("BEGIN ISOLATION LEVEL READ COMMITTED READ WRITE").refusal()).isNull();
    assertThat(PostgresTransactionStatement.parse("BEGIN READ WRITE,NOT DEFERRABLE").refusal()).isNull();
    assertThat(PostgresTransactionStatement.parse("BEGIN READ WRITE , NOT DEFERRABLE").refusal()).isNull();
    // a comma that separates nothing is PostgreSQL's syntax error, left to the grammar
    assertThat(PostgresTransactionStatement.parse("BEGIN READ WRITE,,DEFERRABLE")).isNull();
    assertThat(PostgresTransactionStatement.parse("BEGIN READ WRITE, ,DEFERRABLE")).isNull();
    assertThat(PostgresTransactionStatement.parse("BEGIN READ WRITE,")).isNull();
    assertThat(PostgresTransactionStatement.parse("START TRANSACTION READ WRITE,,ISOLATION LEVEL READ COMMITTED")).isNull();
  }

  @Test
  void setTransactionSharesTheModeParser() {
    // The SET TRANSACTION path reads the same modes and refuses READ ONLY the same way
    assertThatThrownBy(() -> PostgresNetworkExecutor.resolveSetCommand("SET TRANSACTION READ ONLY"))
        .isInstanceOf(PostgresSessionSettings.SettingException.class)
        .satisfies(e -> assertThat(((PostgresSessionSettings.SettingException) e).sqlState).isEqualTo("0A000"));
    assertThat(PostgresNetworkExecutor.resolveSetCommand("SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ WRITE").value())
        .isEqualTo("read committed");
    assertThatThrownBy(() -> PostgresNetworkExecutor.resolveSetCommand("SET TRANSACTION ISOLATION LEVEL NOPE"))
        .isInstanceOf(PostgresSessionSettings.SettingException.class)
        .satisfies(e -> assertThat(((PostgresSessionSettings.SettingException) e).sqlState).isEqualTo("42601"));
  }
}
