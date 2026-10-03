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
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression guard for #8974: sum() over LONG values wrapped silently past {@code Long.MAX_VALUE} and SQL avg() was computed from the
 * wrapped sum.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8974LongSumOverflowTest extends TestHelper {

  @BeforeEach
  void load() {
    database.command("sql", "CREATE VERTEX TYPE L");
    database.command("sql", "CREATE PROPERTY L.n LONG");
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        database.newVertex("L").set("n", 4_000_000_000_000_000_000L).save();
    });
  }

  @Test
  void sqlSumWidens() {
    try (final ResultSet rs = database.query("sql", "SELECT sum(n) AS s, avg(n) AS a FROM L")) {
      final Result row = rs.next();
      assertThat(new BigDecimal(row.<Number>getProperty("s").toString())).isEqualByComparingTo("12000000000000000000");
      assertThat(row.<Number>getProperty("a").doubleValue()).isEqualTo(4.0E18);
    }
  }

  @Test
  void cypherSumWidens() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (x:L) RETURN sum(x.n) AS s, avg(x.n) AS a")) {
      final Result row = rs.next();
      assertThat(new BigDecimal(row.<Number>getProperty("s").toString())).isEqualByComparingTo("12000000000000000000");
      assertThat(row.<Number>getProperty("a").doubleValue()).isEqualTo(4.0E18);
    }
  }

  @Test
  void negativeOverflowAndMixedWidths() {
    assertThat(Type.increment(Long.MIN_VALUE, -1L)).isEqualTo(new BigDecimal("-9223372036854775809"));
    assertThat(Type.increment(Long.MAX_VALUE, 1)).isEqualTo(new BigDecimal("9223372036854775808"));
    assertThat(Type.increment(1, Long.MAX_VALUE)).isEqualTo(new BigDecimal("9223372036854775808"));
    assertThat(Type.increment((short) 1, Long.MAX_VALUE)).isEqualTo(new BigDecimal("9223372036854775808"));
    assertThat(Type.increment(Long.MAX_VALUE - 1, 1L)).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void decimalAvgKeepsDecimalResult() {
    database.command("sql", "CREATE DOCUMENT TYPE D");
    database.command("sql", "CREATE PROPERTY D.x DECIMAL");
    database.transaction(() -> {
      database.newDocument("D").set("x", new BigDecimal("10")).save();
      database.newDocument("D").set("x", new BigDecimal("3")).save();
    });
    try (final ResultSet rs = database.query("sql", "SELECT avg(x) AS a FROM D")) {
      assertThat(rs.next().<Object>getProperty("a")).isInstanceOf(BigDecimal.class);
    }
  }
}
