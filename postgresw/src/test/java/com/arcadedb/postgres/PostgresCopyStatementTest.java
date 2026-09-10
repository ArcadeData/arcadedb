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
package com.arcadedb.postgres;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7188: how {@code COPY ... TO STDOUT} is read in both of PostgreSQL's spellings, which parts of it this
 * server declines, and the text/CSV encodings the rows are written in.
 */
class PostgresCopyStatementTest {

  @Test
  void onlyACopyIsACopy() {
    assertThat(PostgresCopyStatement.parse("SELECT FROM t")).isNull();
    assertThat(PostgresCopyStatement.parse("COPYX TO STDOUT")).isNull();
    assertThat(PostgresCopyStatement.parse("copy")).isNull();
    assertThat(PostgresCopyStatement.isCopy("copy (select 1) to stdout")).isTrue();
    assertThat(PostgresCopyStatement.isCopy("COPY\tt TO STDOUT")).isTrue();
  }

  @Test
  void theQueryFormKeepsTheQueryVerbatimWhateverItHolds() {
    // Nested parentheses, a ')' and a 'TO STDOUT' inside a string literal, a quoted identifier with ')', and a
    // comment: none of them may end the query early.
    final String query = "SELECT name, upper(name) AS \"n)\" FROM t WHERE name = 'a) TO STDOUT' -- ) \n AND (x > (1))";
    final PostgresCopyStatement copy = PostgresCopyStatement.parse("COPY ( " + query + " ) TO STDOUT;");

    assertThat(copy).isNotNull();
    assertThat(copy.getQuery()).isEqualTo(query);
    assertThat(copy.getFormat()).isEqualTo(PostgresCopyStatement.Format.TEXT);
    assertThat(copy.getDelimiter()).isEqualTo('\t');
    assertThat(copy.getNullString()).isEqualTo("\\N");
    assertThat(copy.isHeader()).isFalse();
  }

  @Test
  void theOptionListSpelling() {
    final PostgresCopyStatement copy = PostgresCopyStatement.parse(
        "COPY (SELECT 1) TO STDOUT WITH (FORMAT csv, HEADER true, DELIMITER ';', NULL 'NULL', QUOTE '''', ESCAPE '\\', FORCE_QUOTE (name, \"Other\"), ENCODING 'UTF-8')");

    assertThat(copy.getFormat()).isEqualTo(PostgresCopyStatement.Format.CSV);
    assertThat(copy.isHeader()).isTrue();
    assertThat(copy.getDelimiter()).isEqualTo(';');
    assertThat(copy.getNullString()).isEqualTo("NULL");
    assertThat(copy.getQuote()).isEqualTo('\'');
    assertThat(copy.getEscape()).isEqualTo('\\');

    // The header spelled as a flag, the format as a string, the boolean in its other forms.
    assertThat(PostgresCopyStatement.parse("COPY t TO STDOUT (FORMAT 'binary')").getFormat())
        .isEqualTo(PostgresCopyStatement.Format.BINARY);
    assertThat(PostgresCopyStatement.parse("COPY t TO STDOUT (HEADER)").isHeader()).isTrue();
    assertThat(PostgresCopyStatement.parse("COPY t TO STDOUT (HEADER off)").isHeader()).isFalse();
    assertThat(PostgresCopyStatement.parse("COPY t TO STDOUT (HEADER 1)").isHeader()).isTrue();
  }

  @Test
  void theLegacyKeywordSpelling() {
    final PostgresCopyStatement copy = PostgresCopyStatement.parse(
        "COPY t (a, \"B\") TO STDOUT WITH CSV HEADER DELIMITER AS ';' NULL AS '' QUOTE AS '\"' FORCE QUOTE *");

    assertThat(copy.getQuery()).isEqualTo("SELECT `a`, `B` FROM `t`");
    assertThat(copy.getFormat()).isEqualTo(PostgresCopyStatement.Format.CSV);
    assertThat(copy.isHeader()).isTrue();
    assertThat(copy.getDelimiter()).isEqualTo(';');
    assertThat(copy.getNullString()).isEmpty();

    assertThat(PostgresCopyStatement.parse("COPY t TO STDOUT BINARY").getFormat()).isEqualTo(PostgresCopyStatement.Format.BINARY);
    assertThat(PostgresCopyStatement.parse("COPY t TO STDOUT WITH DELIMITER '|'").getDelimiter()).isEqualTo('|');
  }

  @Test
  void theTableFormIsAnImplicitSelect() {
    // Schema-qualified, as pg_dump and psql spell it: ArcadeDB has no schemas, the last segment is the type.
    final PostgresCopyStatement copy = PostgresCopyStatement.parse("COPY public.\"Person\" TO STDOUT");
    assertThat(copy.getQuery()).isEqualTo("SELECT FROM `Person`");

    // The quoted-identifier rewriter hands the executor back-ticks, which read as the same thing.
    assertThat(PostgresCopyStatement.parse("COPY `Person` (`name`) TO STDOUT").getQuery())
        .isEqualTo("SELECT `name` FROM `Person`");
  }

  @Test
  void whatThisServerDeclinesIsDeclinedAsUnsupportedNotAsASyntaxError() {
    assertCopyException("COPY t FROM STDIN", PostgresCopyStatement.SQLSTATE_FEATURE_NOT_SUPPORTED, "FROM STDIN");
    assertCopyException("COPY t FROM '/tmp/x'", PostgresCopyStatement.SQLSTATE_FEATURE_NOT_SUPPORTED, "FROM STDIN");
    assertCopyException("COPY t TO '/tmp/x'", PostgresCopyStatement.SQLSTATE_FEATURE_NOT_SUPPORTED, "server-side file");
    assertCopyException("COPY t TO PROGRAM 'gzip'", PostgresCopyStatement.SQLSTATE_FEATURE_NOT_SUPPORTED, "PROGRAM");
    assertCopyException("COPY t TO STDOUT (ENCODING 'LATIN1')", PostgresCopyStatement.SQLSTATE_FEATURE_NOT_SUPPORTED, "UTF-8");
  }

  @Test
  void whatPostgresRefusesIsRefusedTheSameWay() {
    assertCopyException("COPY t TO STDOUT (FOO 1)", PostgresCopyStatement.SQLSTATE_SYNTAX_ERROR, "option \"foo\" not recognized");
    assertCopyException("COPY t TO STDOUT (FORMAT binary, HEADER)", PostgresCopyStatement.SQLSTATE_SYNTAX_ERROR, "HEADER in BINARY");
    assertCopyException("COPY t TO STDOUT (FORMAT binary, DELIMITER ';')", PostgresCopyStatement.SQLSTATE_SYNTAX_ERROR, "DELIMITER in BINARY");
    assertCopyException("COPY t TO STDOUT (QUOTE '\"')", PostgresCopyStatement.SQLSTATE_SYNTAX_ERROR, "QUOTE requires CSV");
    assertCopyException("COPY t TO STDOUT (FORMAT csv, DELIMITER ',,')", PostgresCopyStatement.SQLSTATE_SYNTAX_ERROR, "single");
    assertCopyException("COPY t TO STDOUT (HEADER match)", PostgresCopyStatement.SQLSTATE_SYNTAX_ERROR, "match");
    assertCopyException("COPY t TO STDOUT (FORCE_NULL (a))", PostgresCopyStatement.SQLSTATE_SYNTAX_ERROR, "COPY FROM");
    assertCopyException("COPY (SELECT 1 TO STDOUT", PostgresCopyStatement.SQLSTATE_SYNTAX_ERROR, "never closed");
    assertCopyException("COPY (SELECT 1) STDOUT", PostgresCopyStatement.SQLSTATE_SYNTAX_ERROR, "expected TO");
    assertCopyException("COPY (SELECT 1) TO STDOUT garbage", PostgresCopyStatement.SQLSTATE_SYNTAX_ERROR, "'garbage'");
    assertCopyException("COPY t (a, b TO STDOUT", PostgresCopyStatement.SQLSTATE_SYNTAX_ERROR, "column list");
    // The table form splices its names between back-ticks in a SELECT: a name holding one is refused, not spliced.
    assertCopyException("COPY \"t` WHERE 1=1 --\" TO STDOUT", PostgresCopyStatement.SQLSTATE_SYNTAX_ERROR, "back-tick");
    assertCopyException("COPY t (\"a`, b\") TO STDOUT", PostgresCopyStatement.SQLSTATE_SYNTAX_ERROR, "back-tick");
  }

  private static void assertCopyException(final String statement, final String sqlState, final String messagePart) {
    assertThatThrownBy(() -> PostgresCopyStatement.parse(statement))
        .as(statement)
        .isInstanceOf(PostgresCopyStatement.CopyException.class)
        .hasMessageContaining(messagePart)
        .extracting(e -> ((PostgresCopyStatement.CopyException) e).sqlState)
        .isEqualTo(sqlState);
  }

  // ---- encodings ----

  @Test
  void textFormatEscapesWhatTheReaderUnescapes() {
    final PostgresCopyStatement copy = PostgresCopyStatement.parse("COPY t TO STDOUT");
    final StringBuilder out = new StringBuilder();
    copy.appendRow(out, new String[] { "a\tb\nc\rd\\e\u000Bf\bg\fh", null, "" }, new String[] { "x", "y", "z" });
    assertThat(out.toString()).isEqualTo("a\\tb\\nc\\rd\\\\e\\vf\\bg\\fh\t\\N\t\n");

    // A custom delimiter is escaped inside a value, so the reader does not split on it.
    final PostgresCopyStatement semicolon = PostgresCopyStatement.parse("COPY t TO STDOUT (DELIMITER ';', NULL 'nil')");
    out.setLength(0);
    semicolon.appendRow(out, new String[] { "a;b", null }, new String[] { "x", "y" });
    assertThat(out.toString()).isEqualTo("a\\;b;nil\n");

    out.setLength(0);
    PostgresCopyStatement.parse("COPY t TO STDOUT (HEADER)").appendHeader(out, List.of("id", "na\tme"));
    assertThat(out.toString()).isEqualTo("id\tna\\tme\n");
  }

  @Test
  void csvFormatQuotesExactlyWhenPostgresDoes() {
    final PostgresCopyStatement copy = PostgresCopyStatement.parse("COPY t TO STDOUT (FORMAT csv)");
    final String[] names = { "a", "b", "c", "d", "e", "f" };
    final StringBuilder out = new StringBuilder();
    // plain | holds the delimiter | holds a quote (doubled) | holds a line break | NULL | empty string
    copy.appendRow(out, new String[] { "plain", "x,y", "say \"hi\"", "two\nlines", null, "" }, names);
    assertThat(out.toString())
        .as("an empty string is quoted so it can be told from NULL, which is the empty null string")
        .isEqualTo("plain,\"x,y\",\"say \"\"hi\"\"\",\"two\nlines\",,\"\"\n");

    // A value equal to the NULL string is quoted, or the reader would read it back as NULL.
    final PostgresCopyStatement nullWord = PostgresCopyStatement.parse("COPY t TO STDOUT (FORMAT csv, NULL 'NULL')");
    out.setLength(0);
    nullWord.appendRow(out, new String[] { "NULL", null, "" }, new String[] { "a", "b", "c" });
    assertThat(out.toString()).isEqualTo("\"NULL\",NULL,\n");

    // FORCE_QUOTE, by name and for every column; and an escape character other than the quote.
    final PostgresCopyStatement forced = PostgresCopyStatement.parse("COPY t TO STDOUT (FORMAT csv, FORCE_QUOTE (b), ESCAPE '\\')");
    out.setLength(0);
    forced.appendRow(out, new String[] { "a\"b\\c", "plain" }, new String[] { "a", "b" });
    assertThat(out.toString()).isEqualTo("\"a\\\"b\\\\c\",\"plain\"\n");

    final PostgresCopyStatement all = PostgresCopyStatement.parse("COPY t TO STDOUT CSV FORCE QUOTE *");
    out.setLength(0);
    all.appendRow(out, new String[] { "1", null }, new String[] { "a", "b" });
    assertThat(out.toString()).as("NULL is never quoted, even under FORCE_QUOTE").isEqualTo("\"1\",\n");

    // The end-of-data marker alone on a line is quoted, in a single-column result only.
    out.setLength(0);
    copy.appendRow(out, new String[] { "\\." }, new String[] { "a" });
    assertThat(out.toString()).isEqualTo("\"\\.\"\n");

    out.setLength(0);
    PostgresCopyStatement.parse("COPY t TO STDOUT (FORMAT csv, HEADER)").appendHeader(out, List.of("id", "first,last"));
    assertThat(out.toString()).isEqualTo("id,\"first,last\"\n");
  }
}
