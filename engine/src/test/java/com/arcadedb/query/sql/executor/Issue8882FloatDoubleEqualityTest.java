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

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8882: an equality lookup on a FLOAT or DOUBLE property must answer the same through an index, through a scan
 * and in Cypher, for every operand that {@code Type.convert} turns into exactly the stored value.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8882FloatDoubleEqualityTest extends TestHelper {
  private static final float[]  FLOATS  = { 0.1f, 1.0f / 3, 3.14159f, 0.001f, 2.5f };
  private static final double[] DOUBLES = { 0.1, 1.0 / 3, Math.PI, 0.001, 2.5 };

  @Test
  void floatLookedUpByWidenedDouble() {
    setup();
    for (int i = 0; i < FLOATS.length; i++) {
      final float f = FLOATS[i];
      assertAll(i, "f", "g", (double) f);
      assertAll(i, "f", "g", Float.valueOf(f));
      assertAll(i, "f", "g", Double.valueOf(Float.toString(f)));
      assertAll(i, "f", "g", new BigDecimal(f).round(new MathContext(9)));
    }
  }

  @Test
  void doubleLookedUpByBigDecimal() {
    setup();
    for (int i = 0; i < DOUBLES.length; i++) {
      final double d = DOUBLES[i];
      assertAll(i, "d", "e", new BigDecimal(d).round(new MathContext(17)));
      assertAll(i, "d", "e", new BigDecimal(d));
      assertAll(i, "d", "e", Double.valueOf(d));
    }
  }

  @Test
  void literalOperands() {
    setup();
    assertThat(count("sql", "SELECT FROM T WHERE g = 0.100000001 AND k = 0", null)).isEqualTo(1);
    assertThat(count("sql", "SELECT FROM T WHERE f = 0.100000001 AND k = 0", null)).isEqualTo(1);
    assertThat(count("opencypher", "MATCH (n:T) WHERE n.g = 0.100000001 AND n.k = 0 RETURN n", null)).isEqualTo(1);
    assertThat(count("sql", "SELECT FROM T WHERE g = 0.1 AND k = 0", null)).isEqualTo(1);
    assertThat(count("opencypher", "MATCH (n:T) WHERE n.g = 0.1 AND n.k = 0 RETURN n", null)).isEqualTo(1);
  }

  @Test
  void differentValuesStillDiffer() {
    setup();
    assertThat(count("sql", "SELECT FROM T WHERE g = ? AND k = 0", 0.2)).isEqualTo(0);
    assertThat(count("sql", "SELECT FROM T WHERE g = ? AND k = 0", 0.10000001)).isEqualTo(0);
    assertThat(count("sql", "SELECT FROM T WHERE e = ? AND k = 0", new BigDecimal("0.1000000000000001"))).isEqualTo(0);
    assertThat(count("opencypher", "MATCH (n:T) WHERE n.g = $p AND n.k = 0 RETURN n", 0.10000001)).isEqualTo(0);
  }

  private void setup() {
    database.command("sql", "CREATE VERTEX TYPE T");
    database.command("sql", "CREATE PROPERTY T.k INTEGER");
    database.command("sql", "CREATE PROPERTY T.f FLOAT");
    database.command("sql", "CREATE PROPERTY T.g FLOAT");
    database.command("sql", "CREATE PROPERTY T.d DOUBLE");
    database.command("sql", "CREATE PROPERTY T.e DOUBLE");
    database.command("sql", "CREATE INDEX ON T (f) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON T (d) NOTUNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < FLOATS.length; i++)
        database.newVertex("T").set("k", i, "f", FLOATS[i], "g", FLOATS[i], "d", DOUBLES[i], "e", DOUBLES[i]).save();
    });
  }

  private void assertAll(final int k, final String indexed, final String plain, final Object param) {
    final String label = param.getClass().getSimpleName() + " " + param + " k=" + k;
    for (final String p : new String[] { indexed, plain }) {
      assertThat(count("sql", "SELECT FROM T WHERE " + p + " = ? AND k = " + k, param)).as("sql " + p + " " + label).isEqualTo(1);
      assertThat(count("opencypher", "MATCH (n:T) WHERE n." + p + " = $p AND n.k = " + k + " RETURN n", param)).as("cypher " + p + " " + label)
          .isEqualTo(1);
    }
  }

  private int count(final String language, final String query, final Object param) {
    int n = 0;
    try (final ResultSet rs = param == null ?
        database.query(language, query) :
        language.equals("sql") ? database.query(language, query, param) : database.query(language, query, Map.of("p", param))) {
      while (rs.hasNext()) {
        rs.next();
        n++;
      }
    }
    return n;
  }
}
