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
package com.arcadedb.query.sql.parser;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #11442: the AST builder kept an UNWIND operand only when it was a bare top-level field and
 * never kept the alias, so {@code UNWIND a.b} unwound {@code a}, {@code UNWIND :p} became a no-op clause rendering as an
 * unparseable {@code UNWIND }, and {@code UNWIND tags AS x} lost {@code x}.
 */
class Issue11442UnwindOperandTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("T");
      database.newDocument("T").set("name", "n1").set("tags", List.of("t1", "t2")).set("a", Map.of("b", List.of(1, 2, 3)))
          .save();
    });
  }

  private static SelectStatement parse(final String sql) {
    return (SelectStatement) new SQLAntlrParser(null).parse(sql);
  }

  /** Renders the statement, re-parses the rendering and checks both the text and the UNWIND node survive. */
  private static void assertRoundTrips(final String sql, final String expectedUnwind) {
    final SelectStatement first = parse(sql);
    assertThat(first.getUnwind().toString()).isEqualTo(expectedUnwind);
    final SelectStatement second = parse(first.toString());
    assertThat(second.getUnwind()).isEqualTo(first.getUnwind());
    assertThat(second.getUnwind().toString()).isEqualTo(expectedUnwind);
    assertThat(first.copy().getUnwind()).isEqualTo(first.getUnwind());
  }

  private List<Result> rows(final String sql) {
    return rows(sql, Map.of());
  }

  private List<Result> rows(final String sql, final Map<String, Object> params) {
    final List<Result> list = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql, params)) {
      rs.forEachRemaining(list::add);
    }
    return list;
  }

  @Test
  void aBareFieldStillRoundTripsAndUnwindsInPlace() {
    assertRoundTrips("SELECT FROM T UNWIND tags", "UNWIND tags");
    assertRoundTrips("SELECT FROM T UNWIND tags, name", "UNWIND tags, name");
    assertThat(rows("SELECT FROM T UNWIND tags").stream().map(r -> r.<String>getProperty("tags")).toList())
        .containsExactly("t1", "t2");
  }

  @Test
  void aNestedPathWithoutAnAliasIsRejectedInsteadOfUnwindingItsHead() {
    assertThatThrownBy(() -> parse("SELECT FROM T UNWIND a.b")).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("needs an alias");
  }

  @Test
  void aParameterWithoutAnAliasIsRejectedInsteadOfBecomingANoOp() {
    assertThatThrownBy(() -> parse("SELECT FROM T UNWIND :p")).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("needs an alias");
    assertThatThrownBy(() -> parse("SELECT FROM T UNWIND tags, :p")).isInstanceOf(CommandSQLParsingException.class);
  }

  @Test
  void anAliasIsKeptAndRendered() {
    assertRoundTrips("SELECT FROM T UNWIND tags AS x", "UNWIND tags AS x");
    assertRoundTrips("SELECT FROM T UNWIND tags x", "UNWIND tags AS x");
    assertRoundTrips("SELECT FROM T UNWIND a.b AS x", "UNWIND a.b AS x");
    assertRoundTrips("SELECT FROM T UNWIND :p AS x", "UNWIND :p AS x");
    assertRoundTrips("SELECT FROM T UNWIND tags, a.b AS x", "UNWIND tags, a.b AS x");
  }

  @Test
  void anAliasedFieldWritesItsElementsIntoTheAlias() {
    final List<Result> result = rows("SELECT FROM T UNWIND tags AS x");
    assertThat(result.stream().map(r -> r.<String>getProperty("x")).toList()).containsExactly("t1", "t2");
    // the source field is left as it was
    assertThat(result.getFirst().<List<String>>getProperty("tags")).containsExactly("t1", "t2");
  }

  @Test
  void aNestedPathUnwindsTheNestedListNotItsHead() {
    assertThat(rows("SELECT FROM T UNWIND a.b AS x").stream().map(r -> r.<Integer>getProperty("x")).toList())
        .containsExactly(1, 2, 3);
  }

  @Test
  void aParameterIsUnwound() {
    final List<Result> result = rows("SELECT FROM T UNWIND :p AS x", Map.of("p", List.<Object>of("p1", "p2")));
    assertThat(result.stream().map(r -> r.<String>getProperty("x")).toList()).containsExactly("p1", "p2");
  }

  @Test
  void aNarrowingProjectionStillFeedsTheOperandAndKeepsTheAlias() {
    final List<Result> result = rows("SELECT name FROM T UNWIND a.b AS x");
    assertThat(result).hasSize(3);
    assertThat(result.stream().map(r -> r.<Integer>getProperty("x")).toList()).containsExactly(1, 2, 3);
    assertThat(result.getFirst().<String>getProperty("name")).isEqualTo("n1");
    assertThat(result.getFirst().getPropertyNames()).containsExactlyInAnyOrder("name", "x");

    assertThat(rows("SELECT name FROM T UNWIND tags AS x").stream().map(r -> r.<String>getProperty("x")).toList())
        .containsExactly("t1", "t2");

    // the operand reads the projected row, so it sees a projection alias, as a bare field does
    assertThat(rows("SELECT a AS c FROM T UNWIND c.b AS x").stream().map(r -> r.<Integer>getProperty("x")).toList())
        .containsExactly(1, 2, 3);

    // every field the operand reads is kept for the UNWIND, not only the root of a bare path
    assertThat(rows("SELECT name FROM T UNWIND (a.b) AS x").stream().map(r -> r.<Integer>getProperty("x")).toList())
        .containsExactly(1, 2, 3);
    final List<Result> literal = rows("SELECT tags FROM T UNWIND [name, 'z'] AS x");
    assertThat(literal.stream().map(r -> r.<String>getProperty("x")).toList()).containsExactly("n1", "z");
    assertThat(literal.getFirst().getPropertyNames()).containsExactlyInAnyOrder("tags", "x");

    final List<Result> parameter = rows("SELECT name FROM T UNWIND :p AS x", Map.of("p", List.<Object>of("p1", "p2")));
    assertThat(parameter.stream().map(r -> r.<String>getProperty("x")).toList()).containsExactly("p1", "p2");
    assertThat(parameter.getFirst().<String>getProperty("name")).isEqualTo("n1");
  }

  @Test
  void anAliasedItemCombinesWithABareOne() {
    final List<Result> result = rows("SELECT FROM T UNWIND tags, a.b AS x");
    assertThat(result).hasSize(6);
    assertThat(result.stream().map(r -> r.getProperty("tags") + ":" + r.getProperty("x")).toList())
        .containsExactly("t1:1", "t1:2", "t1:3", "t2:1", "t2:2", "t2:3");
  }

  @Test
  void aScalarOperandIsForwardedUnderTheAlias() {
    final List<Result> result = rows("SELECT FROM T UNWIND name AS x");
    assertThat(result).hasSize(1);
    assertThat(result.getFirst().<String>getProperty("x")).isEqualTo("n1");
  }

  @Test
  void aMatchUnwindsAnAliasedOperandOfItsReturnRow() {
    final MatchStatement match = (MatchStatement) new SQLAntlrParser(null).parse(
        "MATCH {type: T, as: t} RETURN t.a AS a UNWIND a.b AS x");
    assertThat(match.getUnwind().toString()).isEqualTo("UNWIND a.b AS x");
    assertThat(((MatchStatement) new SQLAntlrParser(null).parse(match.toString())).getUnwind()).isEqualTo(match.getUnwind());

    assertThat(rows("MATCH {type: T, as: t} RETURN t.a AS a UNWIND a.b AS x").stream().map(r -> r.<Integer>getProperty("x"))
        .toList()).containsExactly(1, 2, 3);
    assertThat(rows("MATCH {type: T, as: t} RETURN t.tags AS tags UNWIND tags AS x").stream()
        .map(r -> r.<String>getProperty("x")).toList()).containsExactly("t1", "t2");
    // the root of the operand is a pattern alias the RETURN does not project: it is brought in for the UNWIND only
    final List<Result> fromPatternAlias = rows("MATCH {type: T, as: t} RETURN t.name AS n UNWIND t.tags AS x");
    assertThat(fromPatternAlias.stream().map(r -> r.<String>getProperty("x")).toList()).containsExactly("t1", "t2");
    assertThat(fromPatternAlias.getFirst().getPropertyNames()).containsExactlyInAnyOrder("n", "x");

    assertThatThrownBy(() -> new SQLAntlrParser(null).parse("MATCH {type: T, as: t} RETURN t.a AS a UNWIND a.b"))
        .isInstanceOf(CommandSQLParsingException.class);
  }

  @Test
  void theFacetsAmbiguityStillReadsAsAnAliasedUnwind() {
    final SelectStatement s = parse("SELECT FROM T FACETS unwind: p TERMS");
    assertThat(s.getUnwind().toString()).isEqualTo("UNWIND :p AS TERMS");
    assertThat(parse(s.toString()).getUnwind()).isEqualTo(s.getUnwind());
  }
}
