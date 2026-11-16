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
package com.arcadedb.query.sql.antlr;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #12082: an integer literal with a leading {@code 0} followed by more digits is octal, as sql-syntax.adoc documents and the
 * lexer's {@code OCTAL_LITERAL} rule spells it ({@code 010} is 8, {@code -065} is -53), and it has that value wherever it is
 * written: in an expression, in a value written to a record, in LIMIT/SKIP, in a range, in a bucket id or count, and when the
 * statement is served from the literal-parameterized statement cache. RID parts stay decimal, as the RID's own text form is.
 */
class Issue12082SqlOctalLiteralTest extends TestHelper {

  @Test
  void octalLiteralsInExpressions() {
    final Result row = single("SELECT 010 AS a, -065 AS b, 010L AS c, 0 AS z, 07 AS s, 01 AS one, 0x10 AS hex, 10 AS dec");
    assertThat(row.<Object>getProperty("a")).isEqualTo(8);
    assertThat(row.<Object>getProperty("b")).isEqualTo(-53);
    assertThat(row.<Object>getProperty("c")).isEqualTo(8L);
    assertThat(row.<Object>getProperty("z")).isEqualTo(0);
    assertThat(row.<Object>getProperty("s")).isEqualTo(7);
    assertThat(row.<Object>getProperty("one")).isEqualTo(1);
    assertThat(row.<Object>getProperty("hex")).isEqualTo(16);
    assertThat(row.<Object>getProperty("dec")).isEqualTo(10);
  }

  @Test
  void octalLiteralsAtTheLongBounds() {
    // 21 sevens are 2^63 - 1 in octal, while the same digits read in decimal overflow a long
    final Result row = single("SELECT 0777777777777777777777 AS max, -01000000000000000000000 AS min, -0x8000000000000000 AS hexMin,"
        + " 020000000000 AS widened");
    assertThat(row.<Object>getProperty("max")).isEqualTo(Long.MAX_VALUE);
    assertThat(row.<Object>getProperty("min")).isEqualTo(Long.MIN_VALUE);
    assertThat(row.<Object>getProperty("hexMin")).isEqualTo(Long.MIN_VALUE);
    assertThat(row.<Object>getProperty("widened")).isEqualTo(2147483648L);
  }

  @Test
  void octalLiteralWrittenIntoRecords() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET n = 010, m = -065");
      database.command("sql", "UPDATE T SET k = 011 WHERE n = 010");
    });
    final Result row = single("SELECT n, m, k FROM T");
    assertThat(row.<Object>getProperty("n")).isEqualTo(8);
    assertThat(row.<Object>getProperty("m")).isEqualTo(-53);
    assertThat(row.<Object>getProperty("k")).isEqualTo(9);
  }

  @Test
  void octalLiteralInLimitSkipAndRanges() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE DOCUMENT TYPE L");
    database.transaction(() -> {
      for (int i = 0; i < 20; i++)
        database.command("sql", "INSERT INTO T SET n = ?", i);
      database.command("sql", "INSERT INTO L SET l = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]");
    });
    assertThat(count("SELECT FROM T LIMIT 010")).isEqualTo(8);
    assertThat(count("SELECT FROM T SKIP 010")).isEqualTo(12);

    final Result row = single("SELECT l[01..010] AS r, l[01...010] AS e FROM L");
    assertThat(row.<List<Integer>>getProperty("r")).containsExactly(1, 2, 3, 4, 5, 6, 7);
    assertThat(row.<List<Integer>>getProperty("e")).containsExactly(1, 2, 3, 4, 5, 6, 7, 8);
  }

  @Test
  void octalBucketCountAndBucketIds() {
    database.command("sql", "CREATE DOCUMENT TYPE T BUCKETS 010");
    final DocumentType type = database.getSchema().getType("T");
    assertThat(type.getBuckets(false)).hasSize(8);

    // a bucket id whose octal spelling differs from its decimal one
    final int bucketId = type.getBuckets(false).getLast().getFileId();
    assertThat(bucketId).isGreaterThanOrEqualTo(8);
    final String octal = "0" + Integer.toOctalString(bucketId);

    database.transaction(() -> {
      for (int i = 0; i < 11; i++)
        database.command("sql", "INSERT INTO bucket:" + octal + " SET n = ?", i);
    });
    final List<Result> rows = database.query("sql", "SELECT @rid AS rid FROM bucket:" + octal).stream().toList();
    assertThat(rows).hasSize(11);
    for (final Result row : rows)
      assertThat(row.<RID>getProperty("rid").getBucketId()).isEqualTo(bucketId);

    // a RID is an address: its parts are decimal, as RID's own text form is
    final RID tenth = single("SELECT @rid AS rid FROM #" + bucketId + ":010").getProperty("rid");
    assertThat(tenth).isEqualTo(new RID("#" + bucketId + ":10"));
  }

  @Test
  void literalsDifferingOnlyByLeadingZeroDoNotShareACachedValue() {
    assertThat(single("SELECT 10 AS v").<Object>getProperty("v")).isEqualTo(10);
    assertThat(single("SELECT 010 AS v").<Object>getProperty("v")).isEqualTo(8);
    assertThat(single("SELECT 10 AS v").<Object>getProperty("v")).isEqualTo(10);

    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET n = 8");
      database.command("sql", "INSERT INTO T SET n = 10");
    });
    assertThat(single("SELECT n FROM T WHERE n = 10").<Object>getProperty("n")).isEqualTo(10);
    assertThat(single("SELECT n FROM T WHERE n = 010").<Object>getProperty("n")).isEqualTo(8);
  }

  private Result single(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      final List<Result> rows = rs.stream().toList();
      assertThat(rows).as(sql).hasSize(1);
      return rows.getFirst();
    }
  }

  private int count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return (int) rs.stream().count();
    }
  }
}
