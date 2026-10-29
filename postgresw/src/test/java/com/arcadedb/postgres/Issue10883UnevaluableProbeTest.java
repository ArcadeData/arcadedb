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
import com.arcadedb.database.DatabaseFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10883: a FROM-less SELECT naming {@code pg_catalog} that the catalog cannot evaluate - a function it does not model, an
 * argument it cannot read - was answered with an empty result set as a success, where PostgreSQL answers exactly one row or an
 * error. {@link PostgresCatalog#resolveProbe} now says {@link PostgresCatalog#DECLINED} for it, which the executor refuses with
 * {@code feature_not_supported}, as it refuses a declined catalog query (issue #10371).
 */
class Issue10883UnevaluableProbeTest {
  private static final String DATABASE_PATH = "./target/databases/Issue10883UnevaluableProbeTest";
  private static final String USER          = "root";

  private Database database;

  @BeforeEach
  void createDatabase() {
    final DatabaseFactory factory = new DatabaseFactory(DATABASE_PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void dropDatabase() {
    if (database != null && database.isOpen())
      database.drop();
  }

  @Test
  void aProbeTheCatalogCannotEvaluateIsDeclinedNotAnsweredEmpty() {
    for (final String query : List.of(//
        "SELECT pg_catalog.no_such_function_10883()",//
        "select pg_catalog.no_such_function_10883(1, 'a');",//
        "SELECT pg_catalog.current_schema(), pg_catalog.no_such_function_10883()",//
        // a function PostgreSQL has and this catalog does not model is not answered with no row either
        "SELECT pg_catalog.pg_postmaster_start_time()",//
        // a known function with an argument it cannot read
        "SELECT pg_catalog.pg_is_in_recovery(42)",//
        "SELECT pg_catalog.set_config('search_path', '')",//
        "/* app */ SELECT pg_catalog.no_such_function_10883()")) {
      final PostgresCatalog.Answer answer = probe(query);
      assertThat(answer).as(query).isSameAs(PostgresCatalog.DECLINED);
      assertThat(answer.rows).as("a refusal carries no rows to send: %s", query).isNull();
    }
  }

  @Test
  void aProbeTheCatalogCanEvaluateStillAnswersItsOneRow() {
    for (final String query : List.of("SELECT pg_catalog.current_schema()", "SELECT pg_catalog.set_config('search_path', '', false)",
        "SELECT pg_catalog.pg_is_in_recovery()", "SELECT pg_catalog.version()", "SELECT 1 AS pg_type")) {
      final PostgresCatalog.Answer answer = probe(query);
      assertThat(answer).as(query).isNotNull().isNotSameAs(PostgresCatalog.DECLINED);
      assertThat(answer.rows).as(query).hasSize(1);
    }
  }

  /** What is not a probe still reaches the SQL engine: the refusal is only for the statements resolveProbe owns. */
  @Test
  void aStatementThatIsNotAProbeIsStillLeftToTheEngine() {
    assertThat(probe("SELECT no_such_function_10883()")).isNull();
    assertThat(probe("SELECT 'pg_catalog.no_such_function_10883()'")).isNull();
    assertThat(probe("INSERT INTO Nope SET body = 'pg_catalog.no_such_function_10883()'")).isNull();
  }

  @Test
  void theRefusalIsFeatureNotSupported() {
    final PostgresCatalog.UnsupportedCatalogQuery refusal = PostgresCatalog.refusalOfProbe();
    assertThat(refusal.sqlState).isEqualTo("0A000");
    assertThat(refusal.getMessage()).contains("session probe");
  }

  private PostgresCatalog.Answer probe(final String query) {
    return PostgresCatalog.resolveProbe(query, database, USER, null);
  }
}
