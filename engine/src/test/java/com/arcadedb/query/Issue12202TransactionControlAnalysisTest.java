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
package com.arcadedb.query;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issues #12202 and #12232: the servers refuse a transaction control sent as a command of its own outside a session, and they
 * decide it from the analysis of the statement, {@link QueryEngine.AnalyzedQuery#isTransactionControl()}. It is true for the SQL
 * {@code BEGIN}, {@code COMMIT} and {@code ROLLBACK} and the openCypher {@code START TRANSACTION}, {@code COMMIT} and
 * {@code ROLLBACK}, also behind a prefix that still runs them. A level the engine cannot provide is left to the execution, which
 * refuses it with its own words (issue #10386): the predicate answers false for it in both languages and never throws.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12202TransactionControlAnalysisTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/issue12202-transaction-control-analysis").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @ParameterizedTest
  @ValueSource(strings = { "BEGIN", "begin", "BEGIN ISOLATION READ_COMMITTED", "begin isolation repeatable_read", "COMMIT",
      "COMMIT RETRY 3", "ROLLBACK", "PROFILE BEGIN", "PROFILE ROLLBACK" })
  void aSqlTransactionControlIsRecognized(final String statement) {
    assertThat(isTransactionControl("sql", statement)).as(statement).isTrue();
  }

  @ParameterizedTest
  @ValueSource(strings = { "SELECT 1", "EXPLAIN BEGIN", "EXPLAIN SELECT 1", "PROFILE SELECT 1" })
  void anotherSqlStatementIsNotOne(final String statement) {
    assertThat(isTransactionControl("sql", statement)).as(statement).isFalse();
  }

  @ParameterizedTest
  @ValueSource(strings = { "START TRANSACTION", "start transaction", "START TRANSACTION ISOLATION READ_COMMITTED", "COMMIT", "ROLLBACK",
      "PROFILE START TRANSACTION", "EXPLAIN START TRANSACTION", "EXPLAIN COMMIT" })
  void anOpenCypherTransactionControlIsRecognized(final String statement) {
    assertThat(isTransactionControl("opencypher", statement)).as(statement).isTrue();
  }

  @ParameterizedTest
  @ValueSource(strings = { "RETURN 1", "MATCH (n) RETURN n", "PROFILE RETURN 1", "EXPLAIN RETURN 1" })
  void anotherOpenCypherStatementIsNotOne(final String statement) {
    assertThat(isTransactionControl("opencypher", statement)).as(statement).isFalse();
  }

  @ParameterizedTest
  @ValueSource(strings = { "sql", "opencypher" })
  void aLevelTheEngineCannotProvideIsNotAnsweredByThePredicate(final String language) {
    final String statement = "sql".equals(language) ? "BEGIN ISOLATION SERIALIZABLE" : "START TRANSACTION ISOLATION SERIALIZABLE";

    assertThat(isTransactionControl(language, statement)).as(statement).isFalse();
  }

  private boolean isTransactionControl(final String language, final String statement) {
    return database.getQueryEngine(language).analyze(statement).isTransactionControl();
  }
}
