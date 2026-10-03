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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression guard for #9113 (#9030, #9031): without an index, {@code IN (?)} (a scalar right-hand side) and {@code IN (SELECT ...)}
 * compared with {@code Object.equals()}, so a Long and an Integer, two BigDecimals of different scale, a Date and a stored DATETIME,
 * or 0.0 and -0.0 were different. {@code =}, the list forms and the index all compare by value.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9113InScalarAndSubqueryScanEqualityTest extends TestHelper {
  private static final long T = 946684800000L;

  private void load() {
    for (final String t : new String[] { "I", "S" }) {
      database.command("sql", "CREATE DOCUMENT TYPE " + t);
      database.command("sql", "CREATE PROPERTY " + t + ".id INTEGER");
      database.command("sql", "CREATE PROPERTY " + t + ".k INTEGER");
      database.command("sql", "CREATE PROPERTY " + t + ".l LONG");
      database.command("sql", "CREATE PROPERTY " + t + ".m DECIMAL");
      database.command("sql", "CREATE PROPERTY " + t + ".t DATETIME");
      database.command("sql", "CREATE PROPERTY " + t + ".d DOUBLE");
    }
    for (final String p : new String[] { "k", "l", "m", "t", "d" })
      database.command("sql", "CREATE INDEX ON I (" + p + ") NOTUNIQUE");
    database.command("sql", "CREATE DOCUMENT TYPE Src");
    database.transaction(() -> {
      for (final String t : new String[] { "I", "S" })
        database.newDocument(t).set("id", 1, "k", 7, "l", 5L, "m", new BigDecimal("1.5"), "t", new Date(T), "d", -0.0).save();
      database.newDocument("Src").set("k", 7, "kd", 7.0, "kl", 7L, "l", 5, "m", new BigDecimal("1.50"), "d", 0.0).save();
    });
  }

  private List<Object> ids(final String type, final String where, final Object... params) {
    final List<Object> out = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT id FROM " + type + " WHERE " + where, params)) {
      rs.forEachRemaining(r -> out.add(r.getProperty("id")));
    }
    return out;
  }

  private void assertIndexAndScanFind(final String where, final Object... params) {
    assertThat(ids("I", where, params)).as("index: " + where).containsExactly(1);
    assertThat(ids("S", where, params)).as("scan: " + where).containsExactly(1);
  }

  @Test
  void singleParameterInParentheses() {
    load();
    assertIndexAndScanFind("k IN (?)", 7L);
    assertIndexAndScanFind("l IN (?)", 5);
    assertIndexAndScanFind("m IN (?)", new BigDecimal("1.50"));
    assertIndexAndScanFind("t IN (?)", new Date(T));
    assertThat(ids("S", "k IN (?)", 8L)).isEmpty();
  }

  @Test
  void subqueryCompareByValue() {
    load();
    assertIndexAndScanFind("k IN (SELECT k FROM Src)");
    assertIndexAndScanFind("k IN (SELECT kd FROM Src)");
    assertIndexAndScanFind("k IN (SELECT kl FROM Src)");
    assertIndexAndScanFind("l IN (SELECT l FROM Src)");
    assertIndexAndScanFind("m IN (SELECT m FROM Src)");
    assertIndexAndScanFind("d IN (SELECT d FROM Src)");
    assertThat(ids("S", "k IN (SELECT l FROM Src)")).isEmpty();
  }
}
