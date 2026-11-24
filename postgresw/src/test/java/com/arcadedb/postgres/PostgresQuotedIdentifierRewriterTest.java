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

import com.arcadedb.exception.CommandSQLParsingException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #5369: the Postgres wire protocol has to treat double-quoted tokens as identifiers.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostgresQuotedIdentifierRewriterTest {

  @Test
  void noDoubleQuoteReturnsTheSameInstance() {
    final String sql = "SELECT name FROM Character";
    assertThat(PostgresQuotedIdentifierRewriter.rewrite(sql)).isSameAs(sql);
    assertThat(PostgresQuotedIdentifierRewriter.rewrite(null)).isNull();
  }

  @Test
  void quotedProjectionAndTypeBecomeIdentifiers() {
    assertThat(PostgresQuotedIdentifierRewriter.rewrite("SELECT \"name\" FROM \"Character\"")).isEqualTo(
        "SELECT `name` FROM `Character`");
  }

  @Test
  void backslashInAQuotedNameIsEscapedOnTheWayOut() {
    // inside a back-tick identifier a backslash escapes the character that follows it, so a backslash carried over from a
    // Postgres double-quoted name has to be doubled or the engine reads `My\\Type` as MyType
    assertThat(PostgresQuotedIdentifierRewriter.rewrite("SELECT FROM \"My\\Type\"")).isEqualTo("SELECT FROM `My\\\\Type`");
  }

  @Test
  void backTickInAQuotedNameIsEscapedOnTheWayOut() {
    assertThat(PostgresQuotedIdentifierRewriter.rewrite("SELECT FROM \"Back`Tick\"")).isEqualTo("SELECT FROM `Back\\`Tick`");
  }

  @Test
  void qualifiedIdentifiersAreTranslated() {
    assertThat(PostgresQuotedIdentifierRewriter.rewrite("SELECT \"c\".\"name\" FROM Character AS \"c\"")).isEqualTo(
        "SELECT `c`.`name` FROM Character AS `c`");
  }

  @Test
  void singleQuotedLiteralsAreLeftUntouched() {
    assertThat(PostgresQuotedIdentifierRewriter.rewrite("SELECT \"name\" FROM Character WHERE \"name\" = 'Napoleon'")).isEqualTo(
        "SELECT `name` FROM Character WHERE `name` = 'Napoleon'");
  }

  @Test
  void doubleQuotesInsideStringLiteralsAreNotTouched() {
    assertThat(PostgresQuotedIdentifierRewriter.rewrite("SELECT * FROM V WHERE name = 'say \"hello\"'")).isEqualTo(
        "SELECT * FROM V WHERE name = 'say \"hello\"'");
  }

  @Test
  void escapedQuotesInsideStringLiteralsDoNotBreakScanning() {
    assertThat(PostgresQuotedIdentifierRewriter.rewrite("SELECT \"a\" FROM V WHERE x = 'it''s \"ok\"' AND \"b\" = 1")).isEqualTo(
        "SELECT `a` FROM V WHERE x = 'it''s \"ok\"' AND `b` = 1");
    // A backslash escapes the quote only where PostgreSQL reads escapes: an E'...' literal, or any literal with
    // standard_conforming_strings off (issue #10871)
    assertThat(PostgresQuotedIdentifierRewriter.rewrite("SELECT \"a\" FROM V WHERE x = E'it\\'s \"ok\"' AND \"b\" = 1")).isEqualTo(
        "SELECT `a` FROM V WHERE x = E'it\\'s \"ok\"' AND `b` = 1");
    assertThat(PostgresQuotedIdentifierRewriter.rewrite("SELECT \"a\" FROM V WHERE x = 'it\\'s \"ok\"' AND \"b\" = 1", false)).isEqualTo(
        "SELECT `a` FROM V WHERE x = 'it\\'s \"ok\"' AND `b` = 1");
  }

  @Test
  void aBackslashEndsNoLiteralWithStandardConformingStrings() {
    // 'C:\' is a complete literal in PostgreSQL: the identifier after it is still an identifier (issue #10871)
    assertThat(PostgresQuotedIdentifierRewriter.rewrite("SELECT \"a\" FROM V WHERE p = 'C:\\' AND \"b\" = 1")).isEqualTo(
        "SELECT `a` FROM V WHERE p = 'C:\\' AND `b` = 1");
  }

  @Test
  void doubledDoubleQuoteIsAnEscapedQuoteInsideTheIdentifier() {
    assertThat(PostgresQuotedIdentifierRewriter.rewrite("SELECT \"we\"\"ird\" FROM V")).isEqualTo("SELECT `we\"ird` FROM V");
  }

  @Test
  void backTickInsideTheIdentifierIsEscaped() {
    assertThat(PostgresQuotedIdentifierRewriter.rewrite("SELECT \"we`ird\" FROM V")).isEqualTo("SELECT `we\\`ird` FROM V");
  }

  @Test
  void recordAttributesAreWrittenBare() {
    // issue #11392: double-quoted, ArcadeDB SQL read "@rid" as a string literal
    assertThat(PostgresQuotedIdentifierRewriter.rewrite("SELECT \"@rid\", \"name\" FROM V")).isEqualTo("SELECT @rid, `name` FROM V");
    assertThat(PostgresQuotedIdentifierRewriter.rewrite("SELECT \"@TYPE\", \"@this\" FROM V")).isEqualTo("SELECT @TYPE, @this FROM V");
    // an @name the SQL lexer has no token for is left as written: bare, "@foo" would not parse and "@ridfoo" would lex as @rid
    // aliased foo
    for (final String other : new String[] { "\"@a b\"", "\"@foo\"", "\"@ridfoo\"" })
      assertThat(PostgresQuotedIdentifierRewriter.rewrite("SELECT " + other + " FROM V")).isEqualTo("SELECT " + other + " FROM V");
  }

  @Test
  void emptyIdentifierIsLeftUntouched() {
    assertThat(PostgresQuotedIdentifierRewriter.rewrite("SELECT \"\" FROM V")).isEqualTo("SELECT \"\" FROM V");
  }

  @Test
  void unterminatedIdentifierIsLeftUntouched() {
    assertThat(PostgresQuotedIdentifierRewriter.rewrite("SELECT \"name FROM V")).isEqualTo("SELECT \"name FROM V");
  }

  @Test
  void commentsAreCopiedVerbatim() {
    assertThat(PostgresQuotedIdentifierRewriter.rewrite("SELECT \"a\" -- keep \"this\"\nFROM V")).isEqualTo(
        "SELECT `a` -- keep \"this\"\nFROM V");
    assertThat(PostgresQuotedIdentifierRewriter.rewrite("SELECT /* \"nope\" /* nested \"x\" */ */ \"a\" FROM V")).isEqualTo(
        "SELECT /* \"nope\" /* nested \"x\" */ */ `a` FROM V");
  }

  @Test
  void existingBackTickIdentifiersSurvive() {
    assertThat(PostgresQuotedIdentifierRewriter.rewrite("SELECT `a b`, \"c\" FROM V")).isEqualTo("SELECT `a b`, `c` FROM V");
  }

  @Test
  void contextVariablesAreNotMistakenForDollarQuoting() {
    assertThat(PostgresQuotedIdentifierRewriter.rewrite("SELECT $current.\"name\" FROM V LET $current = 1")).isEqualTo(
        "SELECT $current.`name` FROM V LET $current = 1");
  }

  @Test
  void dollarQuotedLiteralsAreCopiedVerbatim() {
    assertThat(PostgresQuotedIdentifierRewriter.rewrite("SELECT \"a\" FROM V WHERE x = $$keep \"this\"$$")).isEqualTo(
        "SELECT `a` FROM V WHERE x = $$keep \"this\"$$");
    assertThat(PostgresQuotedIdentifierRewriter.rewrite("SELECT \"a\" FROM V WHERE x = $tag$keep \"this\"$tag$")).isEqualTo(
        "SELECT `a` FROM V WHERE x = $tag$keep \"this\"$tag$");
  }

  @Test
  void jsonObjectLiteralsAreCopiedVerbatim() {
    assertThat(PostgresQuotedIdentifierRewriter.rewrite("INSERT INTO \"article\" CONTENT {\"id\": 1, \"title\": \"first\"}")).isEqualTo(
        "INSERT INTO `article` CONTENT {\"id\": 1, \"title\": \"first\"}");
  }

  @Test
  void nestedJsonObjectLiteralsAreCopiedVerbatim() {
    assertThat(PostgresQuotedIdentifierRewriter.rewrite(
        "INSERT INTO T CONTENT {\"a\": {\"b\": [{\"c\": \"}\"}]}, \"d\": \"e\"} RETURN \"a\"")).isEqualTo(
        "INSERT INTO T CONTENT {\"a\": {\"b\": [{\"c\": \"}\"}]}, \"d\": \"e\"} RETURN `a`");
  }

  @Test
  void backSlashEscapesInsideJsonStringsAreHonoured() {
    assertThat(PostgresQuotedIdentifierRewriter.rewrite("INSERT INTO T CONTENT {\"a\": \"say \\\"hi\\\"\"} RETURN \"a\"")).isEqualTo(
        "INSERT INTO T CONTENT {\"a\": \"say \\\"hi\\\"\"} RETURN `a`");
  }

  @Test
  void collectionLiteralsAreCopiedVerbatim() {
    assertThat(PostgresQuotedIdentifierRewriter.rewrite("UPDATE \"T\" SET \"tags\" = [\"a\", \"b\"] WHERE \"id\" = 1")).isEqualTo(
        "UPDATE `T` SET `tags` = [\"a\", \"b\"] WHERE `id` = 1");
  }

  @Test
  void insertAndUpdateStatementsAreTranslated() {
    assertThat(PostgresQuotedIdentifierRewriter.rewrite("INSERT INTO \"Character\" (\"name\") VALUES ('Cosette')")).isEqualTo(
        "INSERT INTO `Character` (`name`) VALUES ('Cosette')");
    assertThat(PostgresQuotedIdentifierRewriter.rewrite("UPDATE \"Character\" SET \"name\" = 'Cosette' WHERE \"id\" = 1")).isEqualTo(
        "UPDATE `Character` SET `name` = 'Cosette' WHERE `id` = 1");
  }

  /*
   * Issue #11483: the split of a simple-query ('Q') text into its statements, on {@code ;} outside every quoted form and
   * comment, as PostgreSQL's lexer reads them.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static List<String> split(final String text) {
    return PostgresQuotedIdentifierRewriter.splitStatements(text, true, true);
  }

  // Regression: #11483
  @Test
  @DisplayName("[#11483] the psqlodbc 16 connect-time string is three statements")
  void psqlodbcConnectString() {
    assertThat(split("SET DateStyle = 'ISO';SET extra_float_digits = 2;show transaction_isolation"))
        .containsExactly("SET DateStyle = 'ISO'", "SET extra_float_digits = 2", "show transaction_isolation");
  }

  // Regression: #11483
  @Test
  @DisplayName("[#11483] a text without a separator is one statement, the same instance")
  void singleStatement() {
    final String text = "SELECT * FROM V WHERE name = 'a'";
    assertThat(split(text)).containsExactly(text);
    assertThat(split(text).get(0)).isSameAs(text);
  }

  // Regression: #11483
  @Test
  @DisplayName("[#11483] a trailing ';' and empty statements add no statement")
  void emptyStatementsAreDropped() {
    assertThat(split("SELECT 1;")).containsExactly("SELECT 1");
    assertThat(split("SELECT 1;;  ;SELECT 2 ; ")).containsExactly("SELECT 1", "SELECT 2 ");
    assertThat(split(";")).isEmpty();
    assertThat(split(" ; ;; ")).isEmpty();
    assertThat(split("")).isEmpty();
  }

  // Regression: #11483
  @Test
  @DisplayName("[#11483] a statement made only of comments is no statement")
  void commentOnlyStatementsAreDropped() {
    assertThat(split("-- nothing here")).isEmpty();
    assertThat(split("/* nothing */")).isEmpty();
    assertThat(split("SELECT 1; -- trailing; comment")).containsExactly("SELECT 1");
    assertThat(split("SELECT 1; /* a; b */ ; SELECT 2")).containsExactly("SELECT 1", " SELECT 2");
  }

  // Regression: #11483
  @Test
  @DisplayName("[#11483] a ';' inside a single-quoted literal does not split, doubling and E'' escapes honoured")
  void singleQuotedLiterals() {
    assertThat(split("SELECT 'a;b';SELECT 'it''s; fine'")).containsExactly("SELECT 'a;b'", "SELECT 'it''s; fine'");
    assertThat(split("SELECT E'x\\';y';SELECT 2")).containsExactly("SELECT E'x\\';y'", "SELECT 2");
    // with standard_conforming_strings on, a backslash in a plain literal is an ordinary character: the quote after it closes
    assertThat(split("SELECT 'C:\\';SELECT 2")).containsExactly("SELECT 'C:\\'", "SELECT 2");
  }

  // Regression: #11483
  @Test
  @DisplayName("[#11483] with standard_conforming_strings off a backslash escapes the quote of a plain literal")
  void nonStandardConformingStrings() {
    assertThat(PostgresQuotedIdentifierRewriter.splitStatements("SELECT 'x\\';y';SELECT 2", false, true))
        .containsExactly("SELECT 'x\\';y'", "SELECT 2");
  }

  // Regression: #11483
  @Test
  @DisplayName("[#11483] a ';' inside a quoted identifier, a back-tick name, a dollar-quoted literal or a comment does not split")
  void otherQuotedFormsAndComments() {
    assertThat(split("SELECT \"a;\"\"b\" FROM t;SELECT 2")).containsExactly("SELECT \"a;\"\"b\" FROM t", "SELECT 2");
    assertThat(split("SELECT `a;b` FROM t;SELECT 2")).containsExactly("SELECT `a;b` FROM t", "SELECT 2");
    assertThat(split("SELECT $$a;b$$;SELECT $tag$c;$$;d$tag$;SELECT 3"))
        .containsExactly("SELECT $$a;b$$", "SELECT $tag$c;$$;d$tag$", "SELECT 3");
    assertThat(split("SELECT 1 -- a;b\n;SELECT 2")).containsExactly("SELECT 1 -- a;b\n", "SELECT 2");
    assertThat(split("SELECT 1 /* a; /* nested; */ b; */;SELECT 2")).containsExactly("SELECT 1 /* a; /* nested; */ b; */", "SELECT 2");
  }

  // Regression: #11483
  @Test
  @DisplayName("[#11483] a ';' inside an ArcadeDB JSON object or collection literal does not split")
  void arcadeDbJsonLiterals() {
    assertThat(split("INSERT INTO T CONTENT {\"k\": \"v;\\\"w\"};SELECT 2"))
        .containsExactly("INSERT INTO T CONTENT {\"k\": \"v;\\\"w\"}", "SELECT 2");
    assertThat(split("UPDATE T SET tags = [\"a;b\"];SELECT 2")).containsExactly("UPDATE T SET tags = [\"a;b\"]", "SELECT 2");
  }

  // Regression: #11483
  @Test
  @DisplayName("[#11483] a bracket or brace inside a quoted form is not counted, a stray closing one is harmless")
  void bracketsAndBracesOutsideALiteral() {
    assertThat(split("SELECT '[';SELECT '{';SELECT \"}\" FROM t;SELECT 4"))
        .containsExactly("SELECT '['", "SELECT '{'", "SELECT \"}\" FROM t", "SELECT 4");
    assertThat(split("SELECT /* [ { */ 1;SELECT 2 -- ]\n;SELECT 3")).containsExactly("SELECT /* [ { */ 1", "SELECT 2 -- ]\n", "SELECT 3");
    // a closing bracket or brace that opens nothing leaves the depth at zero, so the separators after it still split
    assertThat(split("SELECT ];SELECT };SELECT 3")).containsExactly("SELECT ]", "SELECT }", "SELECT 3");
  }

  // Regression: #11483
  @Test
  @DisplayName("[#11483] an unclosed bracket or brace keeps the rest of the text in one statement, which the parser refuses whole")
  void unclosedBracketOrBrace() {
    // PostgreSQL refuses such a message whole, at its raw parse, before running anything; one statement that fails to parse
    // runs nothing either
    assertThat(split("SELECT [1;SELECT 2")).containsExactly("SELECT [1;SELECT 2");
    assertThat(split("SELECT {1;SELECT 2")).containsExactly("SELECT {1;SELECT 2");
  }

  // Regression: #11483
  @Test
  @DisplayName("[#11483] an ArcadeDB context variable is no dollar-quoted literal")
  void contextVariable() {
    assertThat(split("SELECT $current;SELECT 2")).containsExactly("SELECT $current", "SELECT 2");
  }

  // Regression: #11483
  @Test
  @DisplayName("[#11483] an unterminated literal swallows the rest of the text, for the parser to report")
  void unterminatedLiteral() {
    assertThat(split("SELECT 'abc;SELECT 2")).containsExactly("SELECT 'abc;SELECT 2");
    assertThat(split("SELECT \"abc;SELECT 2")).containsExactly("SELECT \"abc;SELECT 2");
    assertThat(split("SELECT `abc;SELECT 2")).containsExactly("SELECT `abc;SELECT 2");
    assertThat(split("SELECT 1 /* abc; /* nested */ ;SELECT 2")).containsExactly("SELECT 1 /* abc; /* nested */ ;SELECT 2");
  }

  // Regression: #11483
  @Test
  @DisplayName("[#11483] a dollar quote that never closes is an ordinary character, as an ArcadeDB context variable is")
  void unterminatedDollarQuote() {
    // the first statement then fails to parse and stops the message, which is where PostgreSQL's raw parse stops it too
    assertThat(split("SELECT $$abc;SELECT 2")).containsExactly("SELECT $$abc", "SELECT 2");
    assertThat(split("SELECT $tag$abc;SELECT 2")).containsExactly("SELECT $tag$abc", "SELECT 2");
  }

  /*
   * Issue #10871: a PostgreSQL string literal is read with PostgreSQL's rules - a backslash is an ordinary character in a
   * plain literal while {@code standard_conforming_strings} is on, an escape in an {@code E'...'} literal and in every
   * literal with the setting off - and handed to the engine in the form its lexer decodes to that same value.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private static String on(final String sql) {
    return PostgresQuotedIdentifierRewriter.toEngineLiterals(sql, true);
  }

  private static String off(final String sql) {
    return PostgresQuotedIdentifierRewriter.toEngineLiterals(sql, false);
  }

  // Regression: #10871
  @Test
  void aBackslashInAPlainLiteralIsAnOrdinaryCharacterWithStandardConformingStrings() {
    assertThat(on("SELECT 'a\\b' AS v")).isEqualTo("SELECT 'a\\\\b' AS v");
    assertThat(on("SELECT 'x\\ty' AS v")).isEqualTo("SELECT 'x\\\\ty' AS v");
    // a literal ending in a backslash is complete, and the one after it is a literal of its own
    assertThat(on("SELECT 'a\\' AS v, 'b' AS w")).isEqualTo("SELECT 'a\\\\' AS v, 'b' AS w");
    assertThat(on("INSERT INTO Bs (v) VALUES ('C:\\')")).isEqualTo("INSERT INTO Bs (v) VALUES ('C:\\\\')");
  }

  // Regression: #10871
  @Test
  void theDoubledQuoteIsWrittenTheWayTheEngineEscapesAQuote() {
    assertThat(on("SELECT 'it''s' AS v")).isEqualTo("SELECT 'it\\'s' AS v");
    assertThat(on("SELECT '''' AS v")).isEqualTo("SELECT '\\'' AS v");
  }

  // Regression: #10871
  @Test
  void anEscapeStringDecodesPostgresEscapes() {
    assertThat(on("SELECT E'a\\tb' AS v")).isEqualTo("SELECT 'a\tb' AS v");
    assertThat(on("SELECT e'a\\\\b' AS v")).isEqualTo("SELECT 'a\\\\b' AS v");
    assertThat(on("SELECT E'it\\'s' AS v")).isEqualTo("SELECT 'it\\'s' AS v");
    // \b is a backspace, not a dropped backslash
    assertThat(on("SELECT E'a\\bc' AS v")).isEqualTo("SELECT 'a\bc' AS v");
    assertThat(on("SELECT E'\\101\\x42\\u0043\\U00000044' AS v")).isEqualTo("SELECT 'ABCD' AS v");
    assertThat(on("SELECT E'\\U0001F600' AS v")).isEqualTo("SELECT '\uD83D\uDE00' AS v");
    // an escaped character with no meaning of its own stands for itself, and so does an \x with no digit
    assertThat(on("SELECT E'\\q\\xz' AS v")).isEqualTo("SELECT 'qxz' AS v");
    // line breaks, which the engine lexer does not accept inside a literal, are escaped
    assertThat(on("SELECT E'a\\nb\\rc' AS v")).isEqualTo("SELECT 'a\\nb\\rc' AS v");
    assertThat(on("SELECT 'a\nb' AS v")).isEqualTo("SELECT 'a\\nb' AS v");
  }

  // Regression: #10871
  @Test
  void aMalformedUnicodeEscapeIsRefusedAsPostgresRefusesIt() {
    assertThatThrownBy(() -> on("SELECT E'abc \\u30 def' AS v")).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("invalid Unicode escape");
    assertThatThrownBy(() -> on("SELECT E'\\U00110000' AS v")).isInstanceOf(CommandSQLParsingException.class);
    assertThatThrownBy(() -> on("SELECT E'\\UFFFFFFFF' AS v")).isInstanceOf(CommandSQLParsingException.class);
    // a pair of 4-digit surrogate escapes spells one character
    assertThat(on("SELECT E'\\uD83D\\uDE00' AS v")).isEqualTo("SELECT '😀' AS v");
    // in a plain literal the backslash is an ordinary character: nothing to refuse
    assertThat(on("SELECT 'abc \\u30 def' AS v")).isEqualTo("SELECT 'abc \\\\u30 def' AS v");
  }

  // Regression: #10871
  @Test
  void anEOnlyPrefixesAnEscapeStringWhenItIsNotTheEndOfAWord() {
    assertThat(on("SELECT name FROM V WHERE'a\\b' = name")).isEqualTo("SELECT name FROM V WHERE'a\\\\b' = name");
    assertThat(on("SELECT (E'a\\tb') AS v")).isEqualTo("SELECT ('a\tb') AS v");
  }

  // Regression: #10871
  @Test
  void everyLiteralProcessesEscapesWithStandardConformingStringsOff() {
    assertThat(off("SELECT 'a\\\\b' AS v")).isEqualTo("SELECT 'a\\\\b' AS v");
    assertThat(off("SELECT 'a\\tb' AS v")).isEqualTo("SELECT 'a\tb' AS v");
    assertThat(off("SELECT 'it\\'s', 'b' AS v")).isEqualTo("SELECT 'it\\'s', 'b' AS v");
  }

  // Regression: #10871
  @Test
  void whatIsNotAPostgresLiteralIsLeftAsItIs() {
    // nothing to translate: the same instance
    final String plain = "SELECT 'abc' AS v FROM V WHERE x = 1";
    assertThat(on(plain)).isSameAs(plain);
    final String noLiteral = "SELECT `a\\b` FROM V";
    assertThat(on(noLiteral)).isSameAs(noLiteral);

    // comments, back-tick identifiers and ArcadeDB double-quoted strings are copied verbatim
    assertThat(on("SELECT `it's` FROM V -- 'a\\b'\n WHERE \"@rid\" = '#1:0' /* 'c\\' */"))
        .isEqualTo("SELECT `it's` FROM V -- 'a\\b'\n WHERE \"@rid\" = '#1:0' /* 'c\\' */");
    // inside an ArcadeDB JSON object a single-quoted literal is a PostgreSQL literal too
    assertThat(on("INSERT INTO T CONTENT {\"k\": 'a\\tb'}")).isEqualTo("INSERT INTO T CONTENT {\"k\": 'a\\\\tb'}");
    // square brackets are also PostgreSQL's: a literal between them is a PostgreSQL literal
    assertThat(on("SELECT ARRAY['C:\\'] AS v")).isEqualTo("SELECT ARRAY['C:\\\\'] AS v");
    // an unterminated literal is left for the parser to report
    assertThat(on("SELECT 'abc")).isEqualTo("SELECT 'abc");
    assertThat(on("SELECT E'abc\\'")).isEqualTo("SELECT E'abc\\'");
  }

  // A single-quoted literal is read with PostgreSQL's rules wherever it stands, inside an ArcadeDB JSON object included: a
  // client that composes the statement text itself (psycopg2, pgjdbc with preferQueryMode=simple) writes a value as a
  // PostgreSQL literal, so the three passes - identifiers, literals and statement splitting - must agree with it on where
  // the literal ends.

  /** The value written as a client composing the text writes it while standard_conforming_strings is on. */
  private static String clientLiteral(final String value) {
    return "'" + value.replace("'", "''") + "'";
  }

  /** The value written in the form the ArcadeDB lexer decodes back to it. */
  private static String engineLiteral(final String value) {
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'";
  }

  private static final String[] COMPOSED_VALUES = { "plain", "a\\b", "C:\\", "\\'", "it's", "\\''", "x\\'}", "\\', \"w\": 1, \"v\": '",
      "};z;--", "\\'};z;--", "\\'}];z /* ", "{[;'\\", "a\\';b" };

  @Test
  void aLiteralInsideAJsonObjectIsReadWithPostgresRules() {
    // a backslash is an ordinary character
    assertThat(on("INSERT INTO T CONTENT {\"k\": 'a\\tb'}")).isEqualTo("INSERT INTO T CONTENT {\"k\": 'a\\\\tb'}");
    // a literal ending in a backslash is complete, and the one after it is a literal of its own
    assertThat(on("INSERT INTO T CONTENT {\"k\": 'C:\\', \"j\": 'x'}")).isEqualTo("INSERT INTO T CONTENT {\"k\": 'C:\\\\', \"j\": 'x'}");
    // a doubled quote is one quote
    assertThat(on("INSERT INTO T CONTENT {\"k\": 'it''s'}")).isEqualTo("INSERT INTO T CONTENT {\"k\": 'it\\'s'}");
    // in a collection nested in the object, and in an object nested in the collection
    assertThat(on("INSERT INTO T CONTENT {\"k\": ['a\\', 'b']}")).isEqualTo("INSERT INTO T CONTENT {\"k\": ['a\\\\', 'b']}");
    assertThat(on("UPDATE T SET v = [{\"k\": 'a\\'}]")).isEqualTo("UPDATE T SET v = [{\"k\": 'a\\\\'}]");
    // an escape string decodes the escapes
    assertThat(on("INSERT INTO T CONTENT {\"k\": E'a\\tb\\'c'}")).isEqualTo("INSERT INTO T CONTENT {\"k\": 'a\tb\\'c'}");
    // with standard_conforming_strings off every literal does
    assertThat(off("INSERT INTO T CONTENT {\"k\": 'it\\'s'}")).isEqualTo("INSERT INTO T CONTENT {\"k\": 'it\\'s'}");
    assertThat(off("INSERT INTO T CONTENT {\"k\": 'a\\\\b'}")).isEqualTo("INSERT INTO T CONTENT {\"k\": 'a\\\\b'}");
    // a double-quoted string of the object keeps the ArcadeDB meaning
    assertThat(on("INSERT INTO T CONTENT {\"k\": \"it\\\"s 'x'\"}")).isEqualTo("INSERT INTO T CONTENT {\"k\": \"it\\\"s 'x'\"}");
  }

  @Test
  void aValueComposedByTheClientIsOneLiteralInsideAJsonObject() {
    for (final String value : COMPOSED_VALUES) {
      assertThat(on("INSERT INTO T CONTENT {\"k\": " + clientLiteral(value) + "}"))
          .as("value %s", value)
          .isEqualTo("INSERT INTO T CONTENT {\"k\": " + engineLiteral(value) + "}");
      assertThat(on("INSERT INTO T CONTENT {\"k\": [" + clientLiteral(value) + ", " + clientLiteral(value) + "], \"j\": 1}"))
          .as("value %s in a collection", value)
          .isEqualTo("INSERT INTO T CONTENT {\"k\": [" + engineLiteral(value) + ", " + engineLiteral(value) + "], \"j\": 1}");
    }
  }

  @Test
  void aValueComposedByTheClientIsOneStatementInsideAJsonObject() {
    for (final String value : COMPOSED_VALUES) {
      final String text = "INSERT INTO T CONTENT {\"k\": " + clientLiteral(value) + "}";
      assertThat(split(text)).as("value %s", value).containsExactly(text);
      assertThat(split(text + ";SELECT 2")).as("value %s followed by a statement", value).containsExactly(text, "SELECT 2");
    }
  }

  @Test
  void theSplitterAndTheTranslatorAgreeOnWhereALiteralInsideAJsonObjectEnds() {
    // 'a\' is a whole literal while standard_conforming_strings is on: the object closes after it and the ';' separates
    assertThat(split("INSERT INTO T CONTENT {\"k\": 'a\\'};SELECT 2")).containsExactly("INSERT INTO T CONTENT {\"k\": 'a\\'}", "SELECT 2");
    // with the setting off the backslash escapes the quote, the literal runs on and so does the object
    assertThat(PostgresQuotedIdentifierRewriter.splitStatements("INSERT INTO T CONTENT {\"k\": 'a\\';SELECT 2'}", false, true))
        .containsExactly("INSERT INTO T CONTENT {\"k\": 'a\\';SELECT 2'}");
    // an escape string always does
    assertThat(split("INSERT INTO T CONTENT {\"k\": E'a\\';SELECT 2'}")).containsExactly("INSERT INTO T CONTENT {\"k\": E'a\\';SELECT 2'}");
    // the quoted-identifier pass reads the same end: the identifier after the object is rewritten
    assertThat(PostgresQuotedIdentifierRewriter.rewrite("SELECT \"a\" FROM T WHERE x = {\"k\": 'a\\'} AND \"b\" = 1"))
        .isEqualTo("SELECT `a` FROM T WHERE x = {\"k\": 'a\\'} AND `b` = 1");
  }
}
