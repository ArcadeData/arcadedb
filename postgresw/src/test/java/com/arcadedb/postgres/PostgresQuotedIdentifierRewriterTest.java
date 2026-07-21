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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #5369: the Postgres wire protocol has to treat double-quoted tokens as identifiers.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class PostgresQuotedIdentifierRewriterTest {

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
    assertThat(PostgresQuotedIdentifierRewriter.rewrite("SELECT \"a\" FROM V WHERE x = 'it\\'s \"ok\"' AND \"b\" = 1")).isEqualTo(
        "SELECT `a` FROM V WHERE x = 'it\\'s \"ok\"' AND `b` = 1");
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
  void recordAttributesAreLeftUntouched() {
    assertThat(PostgresQuotedIdentifierRewriter.rewrite("SELECT \"@rid\", \"name\" FROM V")).isEqualTo("SELECT \"@rid\", `name` FROM V");
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
}
