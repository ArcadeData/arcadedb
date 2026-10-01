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
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
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

  @Test
  void indexedDecimalAndDoubleLookups() {
    database.command("sql", "CREATE DOCUMENT TYPE I");
    database.command("sql", "CREATE PROPERTY I.k STRING");
    database.command("sql", "CREATE PROPERTY I.dec DECIMAL");
    database.command("sql", "CREATE PROPERTY I.dbl DOUBLE");
    database.command("sql", "CREATE INDEX ON I (dec) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON I (dbl) NOTUNIQUE");
    final BigDecimal v = new BigDecimal(EXACT);
    database.transaction(() -> {
      database.newDocument("I").set("k", "exact", "dec", v, "dbl", 1.5d).save();
      database.newDocument("I").set("k", "above", "dec", v.add(BigDecimal.ONE), "dbl", 2.5d).save();
    });
    assertThat(keys("SELECT k FROM I WHERE dec = " + EXACT)).containsExactly("exact");
    assertThat(keys("SELECT k FROM I WHERE dec > " + EXACT)).containsExactly("above");
    assertThat(keys("SELECT k FROM I WHERE dec >= " + EXACT)).containsExactlyInAnyOrder("exact", "above");
    assertThat(keys("SELECT k FROM I WHERE dbl = 1.5000000000000000")).containsExactly("exact");
    assertThat(keys("SELECT k FROM I WHERE dbl > 1.5000000000000000000001")).containsExactly("above");
  }

  @Test
  void arithmeticWithLongLiteral() {
    try (final ResultSet rs = database.query("sql", "SELECT " + EXACT + " + 1 AS a")) {
      assertThat((BigDecimal) rs.next().getProperty("a")).isEqualByComparingTo(new BigDecimal(EXACT).add(BigDecimal.ONE));
    }
  }

  @Test
  void longAndShortLiteralsPickTheirType() {
    try (final ResultSet rs = database.query("sql", "SELECT 0.1000000000000000 AS a, 1.2345678901234567890e5 AS b, 123456789012345.5 AS c")) {
      final Result r = rs.next();
      assertThat(r.<Object>getProperty("a")).isEqualTo(0.1d);
      assertThat(r.<Object>getProperty("b")).isInstanceOf(BigDecimal.class);
      assertThat(r.<Object>getProperty("c")).isEqualTo(123456789012345.5d);
    }
  }

  @Test
  void hexAndExtremeExponentsStayDoubles() {
    try (final ResultSet rs = database.query("sql", "SELECT 0x1.0000000000000p0 AS a, 1e999999999 AS b, 1e-999999999 AS c")) {
      final Result r = rs.next();
      assertThat(r.<Object>getProperty("a")).isEqualTo(1d);
      assertThat(r.<Object>getProperty("b")).isEqualTo(Double.POSITIVE_INFINITY);
      assertThat(r.<Object>getProperty("c")).isEqualTo(0d);
    }
  }

  @Test
  void underflowingExponentAndDSuffixStayDoubles() {
    try (final ResultSet rs = database.query("sql", "SELECT 1.0000000000000000e-9999999999 AS a, " + EXACT + "D AS b")) {
      final Result r = rs.next();
      assertThat(r.<Object>getProperty("a")).isEqualTo(0d);
      assertThat(r.<Object>getProperty("b")).isInstanceOf(Double.class);
    }
  }

  @Test
  void renderedLiteralReparsesToTheSameValue() {
    for (final String literal : new String[] { "12345678901234567890.", "12345678901234567890e0", "-12345678901234567890.", EXACT, "1.2345678901234567890e30" }) {
      final StringBuilder rendered = new StringBuilder();
      new SQLAntlrParser(null).parse("SELECT " + literal + " AS a").toString(null, rendered);
      try (final ResultSet rs = database.query("sql", rendered.toString())) {
        assertThat((BigDecimal) rs.next().getProperty("a")).as(literal).isEqualByComparingTo(new BigDecimal(literal.endsWith(".") ? literal + "0" : literal));
      }
    }
  }

  @Test
  void longLiteralAssignedToDoubleProperty() {
    database.command("sql", "CREATE DOCUMENT TYPE D");
    database.command("sql", "CREATE PROPERTY D.v DOUBLE");
    database.transaction(() -> database.command("sql", "INSERT INTO D SET v = 1.2345678901234567890"));
    try (final ResultSet rs = database.query("sql", "SELECT v FROM D")) {
      assertThat(rs.next().<Double>getProperty("v")).isEqualTo(1.2345678901234567890d);
    }
  }

  @Test
  void doublePropertyTimesLongLiteralIsExactDecimal() {
    database.command("sql", "CREATE DOCUMENT TYPE M");
    database.command("sql", "CREATE PROPERTY M.v DOUBLE");
    database.transaction(() -> database.command("sql", "INSERT INTO M SET v = 2.0"));
    try (final ResultSet rs = database.query("sql", "SELECT v * 3.14159265358979323846 AS a FROM M")) {
      assertThat(rs.next().<Object>getProperty("a")).isInstanceOf(BigDecimal.class);
    }
  }

  @Test
  void negativeLongLiteralAsDecimalIndexKey() {
    database.command("sql", "CREATE DOCUMENT TYPE N");
    database.command("sql", "CREATE PROPERTY N.k STRING");
    database.command("sql", "CREATE PROPERTY N.dec DECIMAL");
    database.command("sql", "CREATE INDEX ON N (dec) NOTUNIQUE");
    database.transaction(() -> database.newDocument("N").set("k", "neg", "dec", new BigDecimal("-" + EXACT)).save());
    try (final ResultSet rs = database.query("sql", "SELECT k FROM N WHERE dec = -" + EXACT)) {
      assertThat(rs.next().<String>getProperty("k")).isEqualTo("neg");
    }
  }

  @Test
  void seventeenDigitRoundTripLiteralMatchesStoredDoubleWithoutIndex() {
    database.command("sql", "CREATE DOCUMENT TYPE U");
    database.command("sql", "CREATE PROPERTY U.k STRING");
    database.command("sql", "CREATE PROPERTY U.d2 DOUBLE");
    database.transaction(() -> {
      database.newDocument("U").set("k", "tenth", "d2", 0.1d).save();
      database.newDocument("U").set("k", "third", "d2", 1d / 3).save();
    });
    assertThat(keys("SELECT k FROM U WHERE d2 = 0.10000000000000001")).containsExactly("tenth");
    assertThat(keys("SELECT k FROM U WHERE d2 = 0.33333333333333331")).containsExactly("third");
    try (final ResultSet rs = database.query("sql", "SELECT 0.10000000000000001 AS a")) {
      assertThat(rs.next().<Object>getProperty("a")).isEqualTo(0.1d);
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
