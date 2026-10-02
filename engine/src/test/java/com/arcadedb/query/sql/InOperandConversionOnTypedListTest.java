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
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for #8895: {@code ? IN list} on a typed list without an index must convert the operand to the list's
 * item type, as {@code list CONTAINS ?} and a BY ITEM index do.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class InOperandConversionOnTypedListTest extends TestHelper {

  @Test
  void inMatchesContainsAndIndexOnTypedLists() {
    final LocalDateTime at = LocalDateTime.of(2026, 10, 1, 12, 34, 56, 789_000_000);
    final Object[][] cases = { { "DOUBLE", 7.0, "7" }, { "DECIMAL", new BigDecimal("19.90"), "19.9" },
        { "DATETIME", at, at.toInstant(ZoneOffset.UTC) } };

    for (final Object[] c : cases) {
      final String t = "T_" + c[0];
      database.command("sql", "CREATE DOCUMENT TYPE " + t);
      database.command("sql", "CREATE PROPERTY " + t + ".a LIST OF " + c[0]);
      database.command("sql", "CREATE PROPERTY " + t + ".b LIST OF " + c[0]);
      database.command("sql", "CREATE INDEX ON " + t + " (a BY ITEM) NOTUNIQUE");
      database.transaction(() -> database.newDocument(t).set("a", new ArrayList<>(List.of(c[1])), "b", new ArrayList<>(List.of(c[1]))).save());

      final Object v = c[2];
      assertThat(count("SELECT FROM " + t + " WHERE ? IN a", v)).as(c[0] + " indexed IN").isEqualTo(1);
      assertThat(count("SELECT FROM " + t + " WHERE ? IN b", v)).as(c[0] + " unindexed IN").isEqualTo(1);
      assertThat(count("SELECT FROM " + t + " WHERE b CONTAINS ?", v)).as(c[0] + " unindexed CONTAINS").isEqualTo(1);
      assertThat(count("SELECT FROM " + t + " WHERE ? NOT IN b", v)).as(c[0] + " unindexed NOT IN").isEqualTo(0);
    }
  }

  @Test
  void inStillRejectsUnrelatedOperand() {
    database.command("sql", "CREATE DOCUMENT TYPE D");
    database.command("sql", "CREATE PROPERTY D.b LIST OF DOUBLE");
    database.transaction(() -> database.newDocument("D").set("b", new ArrayList<>(List.of(7.0))).save());
    assertThat(count("SELECT FROM D WHERE ? IN b", "8")).isEqualTo(0);
    assertThat(count("SELECT FROM D WHERE ? IN b", "abc")).isEqualTo(0);
  }

  private long count(final String query, final Object param) {
    return database.query("sql", query, param).stream().count();
  }
}
