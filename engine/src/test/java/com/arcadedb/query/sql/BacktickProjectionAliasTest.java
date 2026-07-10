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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #4691: backtick-quoted identifiers in SELECT projection were not
 * resolved as column references - the output key kept the literal backticks and GROUP BY values
 * resolved to null.
 */
class BacktickProjectionAliasTest extends TestHelper {

  @Test
  void backtickProjectionKeyIsStripped() {
    database.transaction(() -> {
      database.command("SQL", "CREATE DOCUMENT TYPE Tbl4691a");
      database.command("SQL", "CREATE PROPERTY Tbl4691a.col1 INTEGER");
      database.command("SQL", "INSERT INTO Tbl4691a SET col1 = 42");

      final ResultSet rs = database.query("SQL", "SELECT `col1` FROM Tbl4691a");
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();

      // The output key must be "col1" without surrounding backticks.
      assertThat(row.getPropertyNames()).containsExactly("col1");
      assertThat(row.<Integer>getProperty("col1")).isEqualTo(42);
    });
  }

  @Test
  void backtickProjectionGroupByValueIsNotNull() {
    database.transaction(() -> {
      database.command("SQL", "CREATE DOCUMENT TYPE Tbl4691b");
      database.command("SQL", "CREATE PROPERTY Tbl4691b.col1 INTEGER");
      database.command("SQL", "INSERT INTO Tbl4691b SET col1 = 1");
      database.command("SQL", "INSERT INTO Tbl4691b SET col1 = 1");
      database.command("SQL", "INSERT INTO Tbl4691b SET col1 = 2");

      final ResultSet rs = database.query("SQL",
          "SELECT `col1`, count(*) AS n FROM Tbl4691b GROUP BY `col1` ORDER BY `col1`");

      final List<Result> rows = new ArrayList<>();
      while (rs.hasNext())
        rows.add(rs.next());

      assertThat(rows).hasSize(2);

      // The grouped column must be keyed "col1" (no backticks) and must not be null.
      final Result first = rows.getFirst();
      assertThat(first.getPropertyNames()).contains("col1");
      assertThat(first.<Integer>getProperty("col1")).isNotNull().isEqualTo(1);
      assertThat(first.<Number>getProperty("n").longValue()).isEqualTo(2L);

      final Result second = rows.get(1);
      assertThat(second.<Integer>getProperty("col1")).isNotNull().isEqualTo(2);
      assertThat(second.<Number>getProperty("n").longValue()).isEqualTo(1L);
    });
  }

  @Test
  void quotedAliasOnQuotedColumnIsStripped() {
    database.transaction(() -> {
      database.command("SQL", "CREATE DOCUMENT TYPE Tbl4691c");
      database.command("SQL", "CREATE PROPERTY Tbl4691c.col1 INTEGER");
      database.command("SQL", "INSERT INTO Tbl4691c SET col1 = 7");
      database.command("SQL", "INSERT INTO Tbl4691c SET col1 = 7");

      // `col1` AS `alias1` → key should be "alias1" and value should be correct.
      final ResultSet rs = database.query("SQL",
          "SELECT `col1` AS `alias1`, count(*) AS n FROM Tbl4691c GROUP BY `col1`");
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.getPropertyNames()).contains("alias1");
      assertThat(row.<Integer>getProperty("alias1")).isNotNull().isEqualTo(7);
      assertThat(row.<Number>getProperty("n").longValue()).isEqualTo(2L);
    });
  }

  @Test
  void unquotedProjectionStillWorks() {
    database.transaction(() -> {
      database.command("SQL", "CREATE DOCUMENT TYPE Tbl4691d");
      database.command("SQL", "CREATE PROPERTY Tbl4691d.col1 INTEGER");
      database.command("SQL", "INSERT INTO Tbl4691d SET col1 = 3");
      database.command("SQL", "INSERT INTO Tbl4691d SET col1 = 3");
      database.command("SQL", "INSERT INTO Tbl4691d SET col1 = 5");

      final ResultSet rs = database.query("SQL",
          "SELECT col1, count(*) AS n FROM Tbl4691d GROUP BY col1 ORDER BY col1");
      final List<Result> rows = new ArrayList<>();
      while (rs.hasNext())
        rows.add(rs.next());

      assertThat(rows).hasSize(2);
      assertThat(rows.getFirst().<Integer>getProperty("col1")).isEqualTo(3);
      assertThat(rows.getFirst().<Number>getProperty("n").longValue()).isEqualTo(2L);
      assertThat(rows.get(1).<Integer>getProperty("col1")).isEqualTo(5);
      assertThat(rows.get(1).<Number>getProperty("n").longValue()).isEqualTo(1L);
    });
  }
}
