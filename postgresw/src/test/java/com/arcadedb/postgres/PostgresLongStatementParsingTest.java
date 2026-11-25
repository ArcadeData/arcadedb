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

import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The statement text the protocol parses before the engine sees it is read once, front to back: a statement made mostly of
 * blanks, the longest one the wire takes being 32 KB, costs time proportional to its length wherever the blanks sit.
 * <p>
 * Every statement here is one the parsers decline or refuse, which is where a parser that tries each way of splitting a run of
 * blanks between its parts pays for all of them. Two run lengths are used: the shorter already separates a linear parse from a
 * cubic one by seconds, the longer is the size of the wire's buffer.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class PostgresLongStatementParsingTest {
  /** What ends a statement the parsers decline: {@code String.trim()} keeps it, and neither {@code \s} nor {@code .} reads it. */
  private static final String LINE_SEPARATOR = "\u2028";
  private static final int[]  RUNS           = { 2_500, 30_000 };
  private static final long   BOUND_MS       = 500;

  @BeforeAll
  static void loadTheParsers() {
    // Class loading and the static tables are not what is measured
    PostgresTypeCatalog.resolve("SELECT oid FROM pg_type");
    PostgresSystemQuery.parse("SELECT user");
    PostgresNetworkExecutor.parseSetCommand("SET a = b");
    PostgresNetworkExecutor.resolveSetCommand("SET CONSTRAINTS ALL DEFERRED");
  }

  @Test
  void aTypeLookupWithALongRunOfBlanksIsDeclinedInOnePass() {
    final List<IntFunction<String>> shapes = List.of(//
        run -> "SELECT" + blanks(run) + LINE_SEPARATOR,//
        run -> "SELECT oid" + blanks(run) + LINE_SEPARATOR,//
        run -> "SELECT oid" + blanks(run) + "FROM" + blanks(run) + LINE_SEPARATOR,//
        run -> "SELECT oid FROM" + blanks(run) + LINE_SEPARATOR,//
        run -> "SELECT oid FROM pg_catalog" + blanks(run) + LINE_SEPARATOR,//
        run -> "SELECT oid FROM pg_type" + blanks(run) + LINE_SEPARATOR,//
        run -> "SELECT oid FROM pg_type t" + blanks(run) + LINE_SEPARATOR,//
        run -> "SELECT oid FROM pg_type AS" + blanks(run) + LINE_SEPARATOR,//
        run -> "SELECT oid FROM pg_type WHERE" + blanks(run) + LINE_SEPARATOR,//
        run -> "SELECT oid FROM pg_type t WHERE" + blanks(run) + LINE_SEPARATOR,//
        run -> "SELECT oid FROM pg_type WHERE oid" + blanks(run) + "=" + blanks(run) + LINE_SEPARATOR,//
        run -> "SELECT oid FROM pg_type WHERE" + blanks(run) + "oid = 23" + blanks(run) + "AND" + blanks(run) + LINE_SEPARATOR);

    for (final int run : RUNS)
      for (final IntFunction<String> shape : shapes) {
        final String statement = shape.apply(run);
        final StallAwareStopwatch watch = StallAwareStopwatch.start();
        final List<Map<String, Object>> answer = PostgresTypeCatalog.resolve(statement);
        watch.assertStayedUnder(BOUND_MS, "one pass over the text, not one per way of splitting its blanks");
        assertThat(answer).as("a statement of %d blanks is not a pg_type lookup", run).isNull();
      }
  }

  @Test
  void aTypeLookupAnswersWhateverTheBlanksBetweenItsPartsAre() {
    for (final int run : RUNS) {
      final String blanks = blanks(run);
      final StallAwareStopwatch watch = StallAwareStopwatch.start();

      final List<Map<String, Object>> filtered = PostgresTypeCatalog.resolve(
          "SELECT" + blanks + "oid," + blanks + "typname" + blanks + "FROM" + blanks + "pg_catalog" + blanks + "." + blanks + "pg_type"
              + blanks + "AS" + blanks + "t" + blanks + "WHERE" + blanks + "t.oid" + blanks + "=" + blanks + "25" + blanks);
      assertThat(filtered).hasSize(1);
      assertThat(filtered.get(0)).containsEntry("oid", PostgresType.TEXT.code).containsEntry("typname", "text");

      final List<Map<String, Object>> all = PostgresTypeCatalog.resolve("SELECT oid" + blanks + "FROM" + blanks + "pg_type");
      assertThat(all).hasSize(PostgresType.values().length);

      watch.assertStayedUnder(BOUND_MS, "one pass over the text, not one per way of splitting its blanks");
    }
  }

  @Test
  void aTypeLookupIsReadTheSameWhateverBlankSeparatesItsParts() {
    final List<String> spellings = List.of(//
        "SELECT oid FROM pg_type",//
        "select oid from PG_TYPE",//
        "SELECT\toid\tFROM\tpg_type",//
        "SELECT oid\nFROM\npg_type",//
        "SELECT oid FROM pg_type t",//
        "SELECT oid FROM pg_type AS t",//
        "SELECT t.oid FROM pg_catalog.pg_type AS t",//
        "SELECT t.oid FROM pg_catalog . pg_type t",//
        "SELECT t.oid FROM pg_type t WHERE t.oid = 23",//
        "SELECT oid FROM pg_type AS t WHERE oid = 23",//
        "SELECT oid FROM pg_type\nWHERE\noid = 23");
    for (final String spelling : spellings)
      assertThat(PostgresTypeCatalog.resolve(spelling)).as(spelling).isNotNull().isNotEmpty();

    // a FROM that the table does not follow is part of the projection: here the name of the column
    assertThat(PostgresTypeCatalog.resolve("SELECT oid AS from FROM pg_type").get(0).keySet()).containsExactly("from");

    // an alias is not a filter, and a filter is not an alias
    assertThat(PostgresTypeCatalog.resolve("SELECT oid FROM pg_type t")).hasSize(PostgresType.values().length);
    assertThat(PostgresTypeCatalog.resolve("SELECT oid FROM pg_type t WHERE oid = 23")).hasSize(1);
    assertThat(PostgresTypeCatalog.resolve("SELECT oid FROM pg_type WHERE typname = 'int4'")).hasSize(1);

    // not a statement of this shape
    assertThat(PostgresTypeCatalog.resolve("SELECToid FROM pg_type")).isNull();
    assertThat(PostgresTypeCatalog.resolve("SELECT oid FROMpg_type")).isNull();
    assertThat(PostgresTypeCatalog.resolve("SELECT oid FROM pg_types")).isNull();
    assertThat(PostgresTypeCatalog.resolve("SELECT oid FROM pg_type t u")).isNull();
    assertThat(PostgresTypeCatalog.resolve("SELECT FROM pg_type")).isNull();
    assertThat(PostgresTypeCatalog.resolve("SELECT oid FROM pg_catalog pg_type")).isNull();
  }

  @Test
  void aSystemQueryWithALongRunOfBlanksIsDeclinedInOnePass() {
    final List<IntFunction<String>> shapes = List.of(//
        run -> "SELECT user" + blanks(run) + "!",//
        run -> "SELECT user" + blanks(run) + "(" + blanks(run) + "!",//
        run -> "SELECT user" + blanks(run) + "AS" + blanks(run) + "!",//
        run -> "SELECT pg_catalog" + blanks(run) + "." + blanks(run) + "version" + blanks(run) + "alias" + blanks(run) + "!",//
        run -> "SELECT" + blanks(run) + "!");

    for (final int run : RUNS)
      for (final IntFunction<String> shape : shapes) {
        final String statement = shape.apply(run);
        final StallAwareStopwatch watch = StallAwareStopwatch.start();
        final PostgresSystemQuery parsed = PostgresSystemQuery.parse(statement);
        watch.assertStayedUnder(BOUND_MS, "one pass over the text, not one per way of splitting its blanks");
        assertThat(parsed).as("a statement of %d blanks is not a system query", run).isNull();
      }
  }

  @Test
  void aSystemQueryAnswersWhateverTheBlanksBetweenItsPartsAre() {
    for (final int run : RUNS) {
      final String blanks = blanks(run);
      final StallAwareStopwatch watch = StallAwareStopwatch.start();

      final PostgresSystemQuery aliased = PostgresSystemQuery.parse(
          "SELECT" + blanks + "pg_catalog" + blanks + "." + blanks + "current_user" + blanks + "(" + blanks + ")" + blanks + "AS" + blanks
              + "who" + blanks + ";");
      assertThat(aliased).isNotNull();
      assertThat(aliased.function).isEqualTo(PostgresSystemQuery.Function.CURRENT_USER);
      assertThat(aliased.columnName).isEqualTo("who");

      final PostgresSystemQuery bare = PostgresSystemQuery.parse("SELECT" + blanks + "user" + blanks + "me");
      assertThat(bare).isNotNull();
      assertThat(bare.function).isEqualTo(PostgresSystemQuery.Function.USER);
      assertThat(bare.columnName).isEqualTo("me");

      watch.assertStayedUnder(BOUND_MS, "one pass over the text, not one per way of splitting its blanks");
    }
  }

  @Test
  void aSystemQueryIsReadTheSameWayItAlwaysWas() {
    assertThat(PostgresSystemQuery.parse("SELECT user x").columnName).isEqualTo("x");
    assertThat(PostgresSystemQuery.parse("SELECT user  AS  x ").columnName).isEqualTo("x");
    assertThat(PostgresSystemQuery.parse("SELECT user ( ) x").columnName).isEqualTo("x");
    assertThat(PostgresSystemQuery.parse("SELECT user () \"X y\"").columnName).isEqualTo("X y");
    assertThat(PostgresSystemQuery.parse("SELECT user AS AS").columnName).isEqualTo("as");
    assertThat(PostgresSystemQuery.parse("SELECT user_id")).isNull();
    assertThat(PostgresSystemQuery.parse("SELECT user (")).isNull();
    assertThat(PostgresSystemQuery.parse("SELECT user x y")).isNull();
    assertThat(PostgresSystemQuery.parse("SELECT user AS x y")).isNull();
  }

  @Test
  void aSetCommandWithALongRunOfBlanksIsParsedInOnePass() {
    for (final int run : RUNS) {
      final String blanks = blanks(run);

      final StallAwareStopwatch declined = StallAwareStopwatch.start();
      assertThat(PostgresNetworkExecutor.parseSetCommand("SET a" + blanks + "b")).isNull();
      assertThat(PostgresNetworkExecutor.parseSetCommand("SET a" + blanks + "T" + blanks + "b")).isNull();
      assertThat(PostgresNetworkExecutor.parseSetCommand("SET a" + blanks + "TOO" + blanks + "b")).isNull();
      declined.assertStayedUnder(BOUND_MS, "one pass over the text, not one per way of splitting its blanks");

      final StallAwareStopwatch parsed = StallAwareStopwatch.start();
      final PostgresSessionSettings.Assignment assignment = PostgresNetworkExecutor.parseSetCommand(
          "SET" + blanks + "a" + blanks + "to" + blanks + "'b c'");
      assertThat(assignment).isNotNull();
      assertThat(assignment.name()).isEqualTo("a");
      assertThat(assignment.value()).isEqualTo("b c");
      parsed.assertStayedUnder(BOUND_MS, "one pass over the text, not one per way of splitting its blanks");
    }
  }

  @Test
  void aSetConstraintsCommandWithALongRunOfBlanksIsParsedInOnePass() {
    for (final int run : RUNS) {
      final String blanks = blanks(run);

      final StallAwareStopwatch watch = StallAwareStopwatch.start();
      assertThatThrownBy(() -> PostgresNetworkExecutor.resolveSetCommand("SET CONSTRAINTS a" + blanks + "b"))
          .isInstanceOf(PostgresSessionSettings.SettingException.class)
          .satisfies(e -> assertThat(((PostgresSessionSettings.SettingException) e).sqlState).isEqualTo("42601"));
      assertThatThrownBy(() -> PostgresNetworkExecutor.resolveSetCommand("SET CONSTRAINTS a" + blanks + "DEFERRED" + blanks + "b"))
          .isInstanceOf(PostgresSessionSettings.SettingException.class)
          .satisfies(e -> assertThat(((PostgresSessionSettings.SettingException) e).sqlState).isEqualTo("42601"));

      // the first name is the one that is reported, and a run of blanks before the separator is not part of it
      assertThatThrownBy(() -> PostgresNetworkExecutor.resolveSetCommand("SET CONSTRAINTS fk_a" + blanks + "," + blanks + "fk_b" + blanks
          + "IMMEDIATE")).isInstanceOf(PostgresSessionSettings.SettingException.class)
          .hasMessage("constraint \"fk_a\" does not exist")
          .satisfies(e -> assertThat(((PostgresSessionSettings.SettingException) e).sqlState).isEqualTo("42704"));
      assertThat(PostgresNetworkExecutor.resolveSetCommand("SET CONSTRAINTS" + blanks + "ALL" + blanks + "deferred"))
          .isSameAs(PostgresSessionSettings.Assignment.NO_OP);
      watch.assertStayedUnder(BOUND_MS, "one pass over the text, not one per way of splitting its blanks");
    }
  }

  @Test
  void aSetConstraintsCommandIsReadTheSameWayItAlwaysWas() {
    assertThat(PostgresNetworkExecutor.resolveSetCommand("SET CONSTRAINTS ALL DEFERRED")).isSameAs(PostgresSessionSettings.Assignment.NO_OP);
    assertThat(PostgresNetworkExecutor.resolveSetCommand("set constraints all immediate")).isSameAs(PostgresSessionSettings.Assignment.NO_OP);
    assertThat(PostgresNetworkExecutor.resolveSetCommand("SET CONSTRAINTS\tALL\nIMMEDIATE")).isSameAs(PostgresSessionSettings.Assignment.NO_OP);
    assertThatThrownBy(() -> PostgresNetworkExecutor.resolveSetCommand("SET CONSTRAINTS fk_a, fk_b DEFERRED"))
        .isInstanceOf(PostgresSessionSettings.SettingException.class).hasMessage("constraint \"fk_a\" does not exist");
    assertThatThrownBy(() -> PostgresNetworkExecutor.resolveSetCommand("SET CONSTRAINTS fk_a ,fk_b DEFERRED"))
        .isInstanceOf(PostgresSessionSettings.SettingException.class).hasMessage("constraint \"fk_a\" does not exist");
    assertThatThrownBy(() -> PostgresNetworkExecutor.resolveSetCommand("SET CONSTRAINTS ALL"))
        .isInstanceOf(PostgresSessionSettings.SettingException.class).satisfies(e -> assertThat(e.getMessage()).contains("syntax error"));
    assertThatThrownBy(() -> PostgresNetworkExecutor.resolveSetCommand("SET CONSTRAINTS DEFERRED"))
        .isInstanceOf(PostgresSessionSettings.SettingException.class).satisfies(e -> assertThat(e.getMessage()).contains("syntax error"));
    assertThatThrownBy(() -> PostgresNetworkExecutor.resolveSetCommand("SET CONSTRAINTS ALLDEFERRED"))
        .isInstanceOf(PostgresSessionSettings.SettingException.class).satisfies(e -> assertThat(e.getMessage()).contains("syntax error"));
  }

  @Test
  void aSessionCommandWithALongQuotedNameIsParsed() {
    for (final int length : new int[] { 5_000, 10_000, 20_000, 30_000 }) {
      final String name = "a".repeat(length);

      final PostgresSessionCommand quoted = PostgresSessionCommand.parse("DEALLOCATE \"" + name + "\"");
      assertThat(quoted.kind()).isEqualTo(PostgresSessionCommand.Kind.DEALLOCATE);
      assertThat(quoted.name()).isEqualTo(name);

      final PostgresSessionCommand doubled = PostgresSessionCommand.parse("CLOSE \"" + "\"\"".repeat(length / 2) + "\"");
      assertThat(doubled.kind()).isEqualTo(PostgresSessionCommand.Kind.CLOSE);
      assertThat(doubled.name()).isEqualTo("\"".repeat(length / 2));

      // a quote that never closes is not a name, whatever follows it
      assertThatThrownBy(() -> PostgresSessionCommand.parse("DISCARD \"" + name))
          .isInstanceOf(PostgresSessionSettings.SettingException.class);
      assertThatThrownBy(() -> PostgresSessionCommand.parse("DEALLOCATE \"" + name + " x"))
          .isInstanceOf(PostgresSessionSettings.SettingException.class);
    }
  }

  @Test
  void aSessionCommandIsReadTheSameWayItAlwaysWas() {
    assertThat(PostgresSessionCommand.parse("DISCARD ALL").kind()).isEqualTo(PostgresSessionCommand.Kind.DISCARD_ALL);
    assertThat(PostgresSessionCommand.parse("deallocate prepare Foo").name()).isEqualTo("foo");
    assertThat(PostgresSessionCommand.parse("DEALLOCATE \"Foo Bar\"").name()).isEqualTo("Foo Bar");
    assertThat(PostgresSessionCommand.parse("DEALLOCATE \"a\"\"b\"").name()).isEqualTo("a\"b");
    assertThat(PostgresSessionCommand.parse("CLOSE `Foo Bar`").name()).isEqualTo("Foo Bar");
    assertThat(PostgresSessionCommand.parse("CLOSE ALL").kind()).isEqualTo(PostgresSessionCommand.Kind.CLOSE_ALL);
    assertThat(PostgresSessionCommand.parse("DEALLOCATE ALL").kind()).isEqualTo(PostgresSessionCommand.Kind.DEALLOCATE_ALL);
    assertThatThrownBy(() -> PostgresSessionCommand.parse("DISCARD \"ALL")).isInstanceOf(PostgresSessionSettings.SettingException.class);
    assertThatThrownBy(() -> PostgresSessionCommand.parse("CLOSE a b")).isInstanceOf(PostgresSessionSettings.SettingException.class);
  }

  private static String blanks(final int count) {
    return " ".repeat(count);
  }
}
