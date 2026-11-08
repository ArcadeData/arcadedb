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
import com.arcadedb.query.sql.parser.FromItem;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.Statement;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #11407 (#11391, #11383, #11380): the rendering of a parsed statement, which CREATE MATERIALIZED VIEW
 * and INSERT ... FROM store and parse again, has to parse back into the statement it was rendered from.
 * <ul>
 *   <li>#11391: a long literal written with an {@code L} suffix rendered without it, and parsed back as an int</li>
 *   <li>#11383: SKIP was always rendered before LIMIT, so the positional parameters of {@code LIMIT ? SKIP ?} swapped</li>
 *   <li>#11380: a FROM subquery lost its alias</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11407LossyRenderingTest extends TestHelper {

  @Test
  void aLongLiteralKeepsItsTypeThroughTheRendering() {
    assertThat(render("SELECT 5L AS x")).isEqualTo("SELECT 5L AS x");
    assertThat(render("SELECT 0xFFL AS x")).isEqualTo("SELECT 255L AS x");
    assertThat(render("SELECT -5L AS x")).isEqualTo("SELECT -5L AS x");
    // controls: an int stays an int, and a long the int range cannot hold needs no suffix to parse back as a long
    assertThat(render("SELECT 5 AS x")).isEqualTo("SELECT 5 AS x");
    assertThat(render("SELECT 2147483648 AS x")).isEqualTo("SELECT 2147483648 AS x");

    for (final String query : List.of("SELECT 5L AS x", "SELECT 0xFFL AS x", "SELECT -5L AS x", "SELECT 5 AS x", "SELECT 2147483648 AS x")) {
      final Object before = value(query);
      final Object after = value(render(query));
      assertThat(after).as("'%s' rendered as '%s'", query, render(query)).isEqualTo(before);
      assertThat(after.getClass()).as("'%s' rendered as '%s'", query, render(query)).isEqualTo(before.getClass());
    }
    assertThat(value("SELECT 5L AS x")).isInstanceOf(Long.class);
    assertThat(render(render("SELECT 5L AS x"))).isEqualTo("SELECT 5L AS x");
  }

  @Test
  void limitAndSkipKeepTheOrderTheirParametersAreNumberedIn() {
    database.getSchema().createDocumentType("T");
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.newDocument("T").set("id", i).save();
    });

    final String query = "SELECT id FROM T ORDER BY id LIMIT ? SKIP ?";
    final String rendered = render(query);
    assertThat(rendered).isEqualTo("SELECT id FROM T ORDER BY id ASC LIMIT ? SKIP ?");
    assertThat(ids(query, 2, 3)).containsExactly(3, 4);
    assertThat(ids(rendered, 2, 3)).containsExactly(3, 4);

    // named parameters bind by name and positional literal values carry no binding, but the written order is kept for them too
    assertThat(render("SELECT id FROM T LIMIT :l SKIP :s")).isEqualTo("SELECT id FROM T LIMIT :l SKIP :s");
    assertThat(render("SELECT id FROM T LIMIT 2 SKIP 3")).isEqualTo("SELECT id FROM T LIMIT 2 SKIP 3");
    // controls: the other order, and one of the two alone
    assertThat(render("SELECT id FROM T SKIP ? LIMIT ?")).isEqualTo("SELECT id FROM T SKIP ? LIMIT ?");
    assertThat(render("SELECT id FROM T LIMIT ?")).isEqualTo("SELECT id FROM T LIMIT ?");
    assertThat(render("SELECT id FROM T SKIP ?")).isEqualTo("SELECT id FROM T SKIP ?");

    // a copy, which the planner and the profiled-limit path render and execute, keeps the order as well
    final SelectStatement copy = ((SelectStatement) parse(query)).copy();
    final StringBuilder builder = new StringBuilder();
    copy.toString(null, builder);
    assertThat(builder.toString()).isEqualTo(rendered);
  }

  @Test
  void aSubqueryKeepsItsAlias() {
    database.getSchema().createDocumentType("Character");
    database.transaction(() -> database.newDocument("Character").set("name", "Luke").save());

    for (final String query : List.of("SELECT * FROM (SELECT name FROM Character) spark_q0 WHERE 1 = 0",
        "SELECT * FROM (SELECT name FROM Character) AS sub WHERE name = 'Luke'")) {
      final String rendered = render(query);
      final FromItem before = fromItem(query);
      final FromItem after = fromItem(rendered);
      assertThat(before.getAlias()).isNotNull();
      assertThat(after.getAlias()).as("'%s' rendered as '%s'", query, rendered).isEqualTo(before.getAlias());
      assertThat(render(rendered)).isEqualTo(rendered);
    }
    assertThat(render("SELECT * FROM (SELECT name FROM Character) spark_q0 WHERE 1 = 0")).isEqualTo(
        "SELECT * FROM (SELECT name FROM Character) AS spark_q0 WHERE 1 = 0");

    // a materialized view stores the rendering and parses it again
    database.command("sql", "CREATE MATERIALIZED VIEW mv AS SELECT name FROM (SELECT name FROM Character) AS sub");
    assertThat(database.getSchema().getMaterializedView("mv").getQuery()).contains("AS sub");
    assertThat(database.query("sql", "SELECT count(*) AS n FROM mv").next().<Number>getProperty("n").longValue()).isEqualTo(1L);
  }

  private Object value(final String query) {
    return database.query("sql", query).next().getProperty("x");
  }

  private List<Integer> ids(final String query, final Object... args) {
    return database.query("sql", query, args).stream().map(r -> r.<Integer>getProperty("id")).toList();
  }

  private static FromItem fromItem(final String query) {
    return ((SelectStatement) parse(query)).getTarget().getItem();
  }

  private static Statement parse(final String query) {
    return new SQLAntlrParser(null).parse(query);
  }

  private static String render(final String query) {
    final StringBuilder builder = new StringBuilder();
    parse(query).toString((Map<String, Object>) null, builder);
    return builder.toString();
  }
}
