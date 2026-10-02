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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #8913: an unindexed scalar {@code p IN [?]} must not convert the operands to the property's type,
 * so it agrees with {@code p = ?} and with the same IN through an index.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class InScalarPropertyNotConvertedTest extends TestHelper {

  @Test
  void scalarInAgreesWithEqualsAndIndex() {
    final LocalDateTime micros = LocalDateTime.of(2026, 10, 1, 12, 34, 56, 789_123_000);
    final Object[][] cases = { { "STRING", "7", 7.0 }, { "DATE", LocalDate.of(2026, 10, 1), 1790812800000L },
        { "DATETIME_MICROS", micros, Date.from(micros.toInstant(ZoneOffset.UTC)) } };

    for (final Object[] c : cases) {
      final String t = "T_" + c[0];
      database.command("sql", "CREATE DOCUMENT TYPE " + t);
      database.command("sql", "CREATE PROPERTY " + t + ".a " + c[0]);
      database.command("sql", "CREATE PROPERTY " + t + ".b " + c[0]);
      database.command("sql", "CREATE INDEX ON " + t + " (a) NOTUNIQUE");
      database.transaction(() -> database.newDocument(t).set("a", c[1], "b", c[1]).save());

      final Object v = c[2];
      final long eq = count("SELECT FROM " + t + " WHERE b = ?", v);
      assertThat(eq).as(c[0] + " '=' premise").isEqualTo(0);
      assertThat(count("SELECT FROM " + t + " WHERE b IN [?]", v)).as(c[0] + " unindexed IN list").isEqualTo(eq);
      assertThat(count("SELECT FROM " + t + " WHERE b IN ?", List.of(v))).as(c[0] + " unindexed IN param").isEqualTo(eq);
      assertThat(count("SELECT FROM " + t + " WHERE b IN ?", new HashSet<>(List.of(v)))).as(c[0] + " unindexed IN set").isEqualTo(eq);
      assertThat(count("SELECT FROM " + t + " WHERE a IN [?]", v)).as(c[0] + " indexed IN").isEqualTo(eq);
      assertThat(count("SELECT FROM " + t + " WHERE b NOT IN [?, null]", v)).as(c[0] + " NOT IN with null is UNKNOWN").isEqualTo(0);
      assertThat(count("SELECT FROM " + t + " WHERE b NOT IN [?]", v)).as(c[0] + " NOT IN").isEqualTo(1);
    }
  }

  @Test
  void sameKindOperandStillMatches() {
    database.command("sql", "CREATE DOCUMENT TYPE K");
    database.command("sql", "CREATE PROPERTY K.s STRING");
    database.transaction(() -> database.newDocument("K").set("s", "7").save());
    assertThat(count("SELECT FROM K WHERE s IN [?]", "7")).isEqualTo(1);
    assertThat(count("SELECT FROM K WHERE s IN ?", new HashSet<>(List.of("7")))).isEqualTo(1);
  }

  @Test
  void nonConstantRightHandSideDoesNotConvertPropertyOnTheLeft() {
    database.command("sql", "CREATE DOCUMENT TYPE R");
    database.command("sql", "CREATE PROPERTY R.s STRING");
    database.command("sql", "CREATE PROPERTY R.l LIST OF DOUBLE");
    database.transaction(() -> database.newDocument("R").set("s", "7", "l", new ArrayList<>(List.of(7.0))).save());
    assertThat(count("SELECT FROM R WHERE s IN l")).isEqualTo(0);
    assertThat(count("SELECT FROM R WHERE ? IN l", "7")).isEqualTo(1);
  }

  @Test
  void subqueryAndReexecutedStatementDoNotLeakTheMemo() {
    database.command("sql", "CREATE DOCUMENT TYPE Q");
    database.command("sql", "CREATE PROPERTY Q.s STRING");
    database.command("sql", "CREATE PROPERTY Q.d DOUBLE");
    database.transaction(() -> database.newDocument("Q").set("s", "7", "d", 7.0).save());
    assertThat(count("SELECT FROM Q WHERE s IN (SELECT d FROM Q)")).isEqualTo(0);
    // same statement text (cached), property on the left never converts whatever the parameter type
    assertThat(count("SELECT FROM Q WHERE s IN [?]", 7.0)).isEqualTo(0);
    assertThat(count("SELECT FROM Q WHERE s IN [?]", "7")).isEqualTo(1);
    assertThat(count("SELECT FROM Q WHERE ? IN [d]", "7")).isEqualTo(0);
    assertThat(count("SELECT FROM Q WHERE s IN [?]", 7.0)).isEqualTo(0);
  }

  @Test
  void letVariableOnTheLeftKeepsConverting() {
    database.command("sql", "CREATE DOCUMENT TYPE V2");
    database.transaction(() -> database.newDocument("V2").save());
    assertThat(count("SELECT FROM V2 LET $x = '7' WHERE $x IN [7.0]")).isEqualTo(1);
  }

  @Test
  void literalOnTheLeftStillConverts() {
    database.command("sql", "CREATE DOCUMENT TYPE V");
    database.transaction(() -> database.newDocument("V").save());
    assertThat(count("SELECT FROM V WHERE '7' IN [7.0]")).isEqualTo(1);
  }

  private long count(final String query, final Object... params) {
    try (final ResultSet rs = database.query("sql", query, params)) {
      return rs.stream().count();
    }
  }
}
