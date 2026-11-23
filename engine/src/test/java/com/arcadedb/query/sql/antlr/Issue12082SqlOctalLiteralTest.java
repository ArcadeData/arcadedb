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
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * #12082: a numeric literal with a leading {@code 0} followed by more digits is octal, as sql-syntax.adoc documents and the
 * lexer's {@code OCTAL_LITERAL} rule spells it ({@code 010} is 8, {@code -065} is -53), and it has that value wherever a number
 * is read: in an expression, in a value written to a record, in LIMIT/SKIP, in a range, and when the statement is served from
 * the literal-parameterized statement cache. The positions that name something with a number (a bucket count, a bucket id, the
 * parts of a RID) are not numeric literals: they keep reading decimal digits, and keep refusing hexadecimal and the L suffix.
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
    final Result row = single(
        "SELECT 0777777777777777777777 AS max, -01000000000000000000000 AS min, -01000000000000000000000L AS minL, 020000000000 AS widened");
    assertThat(row.<Object>getProperty("max")).isEqualTo(Long.MAX_VALUE);
    assertThat(row.<Object>getProperty("min")).isEqualTo(Long.MIN_VALUE);
    assertThat(row.<Object>getProperty("minL")).isEqualTo(Long.MIN_VALUE);
    assertThat(row.<Object>getProperty("widened")).isEqualTo(2147483648L);

    // one more than the magnitude of Long.MIN_VALUE is still an overflow
    assertThatThrownBy(() -> database.query("sql", "SELECT -01000000000000000000001 AS v").close()).hasMessageContaining(
        "Invalid integer");
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

    // the range written without spaces is one token, with spaces it goes through the expression parser: they agree
    final Result row = single("SELECT l[01..010] AS r, l[01...010] AS e, l[01 .. 010] AS rs, l[01 ... 010] AS es, l[010] AS one FROM L");
    assertThat(row.<List<Integer>>getProperty("r")).containsExactly(1, 2, 3, 4, 5, 6, 7);
    assertThat(row.<List<Integer>>getProperty("e")).containsExactly(1, 2, 3, 4, 5, 6, 7, 8);
    assertThat(row.<List<Integer>>getProperty("rs")).containsExactly(1, 2, 3, 4, 5, 6, 7);
    assertThat(row.<List<Integer>>getProperty("es")).containsExactly(1, 2, 3, 4, 5, 6, 7, 8);
    assertThat(row.<Object>getProperty("one")).isEqualTo(8);

    // hexadecimal and the L suffix were never accepted in the range token, and still are not
    assertThatThrownBy(() -> database.query("sql", "SELECT l[0x1..0x3] AS r FROM L").close()).hasMessageContaining("range token");
    assertThatThrownBy(() -> database.query("sql", "SELECT l[1L..3L] AS r FROM L").close()).hasMessageContaining("range token");

    // a leading 0 followed by an 8 or a 9 never lexed as one number, so it never reached the range token: still a syntax error
    assertThatThrownBy(() -> database.query("sql", "SELECT l[08..10] AS r FROM L").close()).isInstanceOf(CommandSQLParsingException.class);
    assertThatThrownBy(() -> database.query("sql", "SELECT l[1..09] AS r FROM L").close()).isInstanceOf(CommandSQLParsingException.class);
  }

  @Test
  void bucketCountsBucketIdsAndRidPartsStayDecimal() {
    // a count is not a numeric literal: BUCKETS 010 is ten buckets, as it always was
    database.command("sql", "CREATE DOCUMENT TYPE T BUCKETS 010");
    final DocumentType type = database.getSchema().getType("T");
    assertThat(type.getBuckets(false)).hasSize(10);

    // hexadecimal and the L suffix were never accepted for a count, and still are not
    assertThatThrownBy(() -> database.command("sql", "CREATE DOCUMENT TYPE H BUCKETS 0x10")).isInstanceOf(RuntimeException.class);
    assertThatThrownBy(() -> database.command("sql", "CREATE DOCUMENT TYPE H BUCKETS 10L")).isInstanceOf(RuntimeException.class);
    assertThat(database.getSchema().existsType("H")).isFalse();

    // a bucket id whose decimal digits are all octal digits and whose octal reading would be another bucket: bucket:0NN is the
    // bucket NN, as RID's own text form says (#NN:0 is the first record of the bucket NN)
    final int bucketId = bucketIdWhoseOctalReadingDiffers();
    final String padded = "0" + bucketId;

    database.transaction(() -> {
      for (int i = 0; i < 11; i++)
        database.command("sql", "INSERT INTO bucket:" + padded + " SET n = ?", i);
    });
    final List<Result> rows = database.query("sql", "SELECT @rid AS rid FROM bucket:" + padded).stream().toList();
    assertThat(rows).hasSize(11);
    for (final Result row : rows)
      assertThat(row.<RID>getProperty("rid").getBucketId()).isEqualTo(bucketId);

    // the parts of a RID are decimal: #NN:010 is the record 10 of the bucket NN
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
      database.command("sql", "INSERT INTO T SET n = -8");
      database.command("sql", "INSERT INTO T SET n = 0");
      database.command("sql", "INSERT INTO T SET big = -9223372036854775808");
    });
    assertThat(single("SELECT n FROM T WHERE n = 10").<Object>getProperty("n")).isEqualTo(10);
    assertThat(single("SELECT n FROM T WHERE n = 010").<Object>getProperty("n")).isEqualTo(8);

    // the extracted value of a negated literal: the sign moves into the value, with or without the L suffix, and so does the
    // Long.MIN_VALUE fold of a magnitude that overflows a long (the same value the parser gives it)
    assertThat(single("SELECT n FROM T WHERE n = -010").<Object>getProperty("n")).isEqualTo(-8);
    assertThat(single("SELECT n FROM T WHERE n = -010L").<Object>getProperty("n")).isEqualTo(-8);
    assertThat(single("SELECT n FROM T WHERE n = -0").<Object>getProperty("n")).isEqualTo(0);
    assertThat(single("SELECT big FROM T WHERE big = -01000000000000000000000").<Object>getProperty("big")).isEqualTo(Long.MIN_VALUE);
    assertThat(single("SELECT big FROM T WHERE big = -01000000000000000000000L").<Object>getProperty("big")).isEqualTo(Long.MIN_VALUE);
  }

  /**
   * The id of a bucket whose decimal digits are all octal digits, so that {@code bucket:0NN} lexes as one token, and that is at
   * least 8, so that reading those digits in octal would address another bucket. File ids depend on what the database holds, so
   * single-bucket types are created until one has such an id.
   */
  private int bucketIdWhoseOctalReadingDiffers() {
    for (int i = 0; i < 200; i++) {
      final int id = database.getSchema().createDocumentType("B" + i, 1).getBuckets(false).getFirst().getFileId();
      if (id >= 8 && isOctalSpelled(id))
        return id;
    }
    throw new AssertionError("no bucket id spelled with octal digits only");
  }

  private static boolean isOctalSpelled(final int id) {
    for (final char c : Integer.toString(id).toCharArray())
      if (c > '7')
        return false;
    return true;
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
