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

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
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

  @Test
  void inOnOtherRightHandShapes() {
    database.command("sql", "CREATE DOCUMENT TYPE S");
    database.command("sql", "CREATE PROPERTY S.d DOUBLE");
    database.command("sql", "CREATE PROPERTY S.s STRING");
    database.command("sql", "CREATE PROPERTY S.l LIST OF DOUBLE");
    database.transaction(() -> database.newDocument("S").set("d", 7.0, "s", "7", "l", new ArrayList<>(List.of(7.0))).save());

    // parameter bound to a Set and to an array, with a String operand against a Double item
    assertThat(count("SELECT FROM S WHERE ? IN ?", "7", new HashSet<>(List.of(7.0)))).isEqualTo(1);
    assertThat(count("SELECT FROM S WHERE ? IN ?", "7", new Object[] { 7.0 })).isEqualTo(1);
    // a property on the left is never converted against the operands (#8913), like "="
    assertThat(count("SELECT FROM S WHERE s IN [7.0, 8.0]")).isEqualTo(0);
    assertThat(count("SELECT FROM S WHERE s = 7.0")).isEqualTo(0);
    assertThat(count("SELECT FROM S WHERE ? IN l", 7L)).isEqualTo(1);
    // a null element keeps the three-valued logic: no match is UNKNOWN, so NOT IN does not return the row
    assertThat(count("SELECT FROM S WHERE '8' NOT IN [7.0, null]")).isEqualTo(0);
    // same for a Set that holds a null: no match is UNKNOWN on the Set branch too
    assertThat(count("SELECT FROM S WHERE ? NOT IN ?", "8", new HashSet<>(Arrays.asList(7.0, null)))).isEqualTo(0);
    assertThat(count("SELECT FROM S WHERE ? IN ?", "7", new HashSet<>(Arrays.asList(7.0, null)))).isEqualTo(1);
    // a Number operand against a Set of Strings converts too, and a converted match is the only match for NOT IN
    assertThat(count("SELECT FROM S WHERE ? IN ?", 7, new HashSet<>(List.of("7")))).isEqualTo(1);
    assertThat(count("SELECT FROM S WHERE ? NOT IN ?", 7, new HashSet<>(List.of("7")))).isEqualTo(0);
    // numeric mixes keep their behaviour
    assertThat(count("SELECT FROM S WHERE ? IN ?", 7, new ArrayList<>(List.of(7L)))).isEqualTo(1);
    assertThat(count("SELECT FROM S WHERE ? IN ?", 8, new ArrayList<>(List.of(7L)))).isEqualTo(0);
    assertThat(count("SELECT FROM S WHERE '7' IN [7.0, null]")).isEqualTo(1);
  }

  @Test
  void inDatetimeRejectsUnparseableOperand() {
    database.command("sql", "CREATE DOCUMENT TYPE DT");
    database.command("sql", "CREATE PROPERTY DT.b LIST OF DATETIME");
    database.transaction(() -> database.newDocument("DT").set("b", new ArrayList<>(List.of(LocalDateTime.of(2026, 10, 1, 12, 0)))).save());
    assertThat(count("SELECT FROM DT WHERE ? IN b", "not a date")).isEqualTo(0);
  }

  private long count(final String query, final Object... params) {
    try (final ResultSet rs = database.query("sql", query, params)) {
      return rs.stream().count();
    }
  }

}
