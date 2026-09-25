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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8288: {@code $1}, {@code $2}... is how every Postgres-wire client (pgjdbc, libpq, psycopg 3,
 * node-postgres, ADBC) sends a bound value. {@link com.arcadedb.query.sql.parser.BaseExpression#execute} already
 * resolves it from the input parameters at run time, but {@code isEarlyCalculated()} did not agree, so the planner
 * treated it as a per-record value and scanned the whole type instead of serving an equality on an indexed
 * property from the index.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostgresPositionalParameterIndexTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE Item");
    database.command("sql", "CREATE PROPERTY Item.k LONG");
    database.command("sql", "CREATE INDEX ON Item (k) UNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < 100; i++)
        database.newDocument("Item").set("k", (long) i).save();
    });
  }

  @Test
  void dollarParameterIsServedFromTheIndex() {
    assertThat(plan("SELECT k FROM Item WHERE k = ?", 7L)).contains("FETCH FROM INDEX Item[k]");
    assertThat(plan("SELECT k FROM Item WHERE k = $1", 7L)).contains("FETCH FROM INDEX Item[k]");
    assertThat(plan("SELECT k FROM Item WHERE k = $2", 3L, 7L)).contains("FETCH FROM INDEX Item[k]");
  }

  @Test
  void dollarParameterStillBindsTheRightValue() {
    assertThat(single("SELECT k FROM Item WHERE k = $1", 7L)).isEqualTo(7L);
    assertThat(single("SELECT k FROM Item WHERE k = $2", 3L, 7L)).isEqualTo(7L);
    try (ResultSet rs = database.query("sql", "SELECT k FROM Item WHERE k = $1", Map.of("0", 9L))) {
      assertThat(rs.next().<Long>getProperty("k")).isEqualTo(9L);
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void scriptVariableNamedDollarOneIsUnaffected() {
    // `$1` is also a legal script variable name (LET $1 = ...): with no input parameter bound at position 0, the
    // fix must leave that identifier path alone.
    try (ResultSet rs = database.command("sqlscript", """
        LET $1 = SELECT count(*) AS c FROM Item WHERE k < 10;
        RETURN $1[0].c;""")) {
      assertThat(((Number) rs.next().getProperty("value")).longValue()).isEqualTo(10L);
    }
  }

  private String plan(final String sql, final Object... args) {
    try (ResultSet rs = database.query("sql", "EXPLAIN " + sql, args)) {
      return rs.next().getProperty("executionPlanAsString").toString();
    }
  }

  private Long single(final String sql, final Object... args) {
    try (ResultSet rs = database.query("sql", sql, args)) {
      final Long k = rs.next().getProperty("k");
      assertThat(rs.hasNext()).isFalse();
      return k;
    }
  }
}
