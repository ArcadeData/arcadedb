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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8993: one number is one key over a STRING key type, whatever type it was written with. A declared STRING property keeps
 * answering a lookup by the text it holds.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8993CanonicalNumberKeyTest extends TestHelper {

  @Test
  void numbersOfEveryTypeShareOneSpelling() {
    assertThat(key(3.0d)).isEqualTo("3");
    assertThat(key(3.0f)).isEqualTo("3");
    assertThat(key(new BigDecimal("3.00"))).isEqualTo("3");
    assertThat(key(-0.0d)).isEqualTo("0");
    assertThat(key(0.1f)).isEqualTo("0.1");
    assertThat(key(2.5d)).isEqualTo("2.5");
    assertThat(key(1.0E10d)).isEqualTo("10000000000");
    assertThat(key(Double.NaN)).isEqualTo("NaN");
    assertThat(key(Double.POSITIVE_INFINITY)).isEqualTo("Infinity");
    assertThat(key(new BigDecimal("1E+999999"))).isEqualTo("1E+999999");
  }

  @Test
  void declaredStringPropertyIsStillFoundByItsText() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE S").close();
      database.command("sql", "CREATE PROPERTY S.s STRING").close();
      database.command("sql", "CREATE INDEX ON S (s) NOTUNIQUE").close();
      database.command("sql", "INSERT INTO S SET s = '3.0'").close();
    });
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM S WHERE s = '3.0'")) {
      assertThat(rs.next().<Number>getProperty("n").longValue()).isEqualTo(1L);
    }
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM S WHERE s = ?", 3.0d)) {
      assertThat(rs.next().<Number>getProperty("n").longValue()).isEqualTo(1L);
    }
  }

  private static Object key(final Object number) {
    return Type.canonicalNumberKey(number);
  }
}
