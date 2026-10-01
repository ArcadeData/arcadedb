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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #8872: a suffix-less decimal literal was always parsed as a double, so a DECIMAL
 * property set or compared with a literal of more than ~16 significant digits lost the rest.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8872DecimalLiteralDigitsTest extends TestHelper {
  private static final String EXACT = "12345678901234567890.123456789012345678";

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.k STRING");
    database.command("sql", "CREATE PROPERTY T.dec DECIMAL");
  }

  @Test
  void literalKeepsAllDigitsOnInsertAndLookup() {
    final BigDecimal v = new BigDecimal(EXACT);
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET k = 'parameter', dec = ?", v);
      database.command("sql", "INSERT INTO T SET k = 'literal', dec = " + EXACT);
      database.command("sql", "INSERT INTO T SET k = 'string', dec = '" + EXACT + "'");
      database.newDocument("T").set("k", "api", "dec", v).save();
    });

    try (final ResultSet rs = database.query("sql", "SELECT k, dec FROM T")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        assertThat((BigDecimal) r.getProperty("dec")).as(r.<String>getProperty("k")).isEqualByComparingTo(v);
      }
    }
    assertThat(keys("SELECT k FROM T WHERE dec = " + EXACT)).containsExactlyInAnyOrder("parameter", "literal", "string", "api");
    assertThat(keys("SELECT k FROM T WHERE dec = -" + EXACT)).isEmpty();
  }

  @Test
  void shortLiteralsStayDoubles() {
    try (final ResultSet rs = database.query("sql", "SELECT 0.1 AS a, 1.5e3 AS b, -2.25 AS c")) {
      final Result r = rs.next();
      assertThat(r.<Object>getProperty("a")).isEqualTo(0.1d);
      assertThat(r.<Object>getProperty("b")).isEqualTo(1500d);
      assertThat(r.<Object>getProperty("c")).isEqualTo(-2.25d);
    }
  }

  @Test
  void negativeLongLiteral() {
    try (final ResultSet rs = database.query("sql", "SELECT -" + EXACT + " AS a")) {
      assertThat((BigDecimal) rs.next().getProperty("a")).isEqualByComparingTo(new BigDecimal("-" + EXACT));
    }
  }

  private List<String> keys(final String sql) {
    final List<String> ks = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        ks.add(rs.next().getProperty("k"));
    }
    return ks;
  }
}
