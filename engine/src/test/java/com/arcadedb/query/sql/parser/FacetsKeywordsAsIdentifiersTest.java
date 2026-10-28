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
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The 13 keywords the FACETS clause adds are still valid property, alias, type and map-key names: a database that already uses
 * one of them as a name keeps working.
 */
class FacetsKeywordsAsIdentifiersTest extends TestHelper {
  private static final String[] KEYWORDS = { "facets", "terms", "top", "filter", "include", "min_count", "missing", "ranges",
      "histogram", "calendar", "fixed", "timezone", "bounds" };

  private static Statement parse(final String sql) {
    return new SQLAntlrParser(null).parse(sql);
  }

  @ParameterizedTest
  @ValueSource(strings = { "facets", "terms", "top", "filter", "include", "min_count", "missing", "ranges", "histogram", "calendar",
      "fixed", "timezone", "bounds" })
  void projectionWhereOrderBy(final String k) {
    final SelectStatement s = (SelectStatement) parse("SELECT %1$s FROM T WHERE %1$s = 1 ORDER BY %1$s".formatted(k));
    assertThat(s.getProjection().getItems().get(0).getProjectionAliasAsString()).isEqualTo(k);
    assertThat(s.getWhereClause()).isNotNull();
    assertThat(s.getOrderBy()).isNotNull();
    assertThat(s.getFacets()).isNull();
  }

  @ParameterizedTest
  @ValueSource(strings = { "facets", "terms", "top", "filter", "include", "min_count", "missing", "ranges", "histogram", "calendar",
      "fixed", "timezone", "bounds" })
  void groupByAndAlias(final String k) {
    final SelectStatement s = (SelectStatement) parse("SELECT %1$s AS %1$s, count(*) FROM T GROUP BY %1$s".formatted(k));
    assertThat(s.getProjection().getItems()).hasSize(2);
    assertThat(s.getGroupBy()).isNotNull();
  }

  @ParameterizedTest
  @ValueSource(strings = { "facets", "terms", "top", "filter", "include", "min_count", "missing", "ranges", "histogram", "calendar",
      "fixed", "timezone", "bounds" })
  void createProperty(final String k) {
    assertThat(parse("CREATE PROPERTY T.%s STRING".formatted(k))).isInstanceOf(CreatePropertyStatement.class);
  }

  @ParameterizedTest
  @ValueSource(strings = { "facets", "terms", "top", "filter", "include", "min_count", "missing", "ranges", "histogram", "calendar",
      "fixed", "timezone", "bounds" })
  void insertSet(final String k) {
    assertThat(parse("INSERT INTO T SET %s = 'v'".formatted(k))).isInstanceOf(InsertStatement.class);
  }

  @ParameterizedTest
  @ValueSource(strings = { "facets", "terms", "top", "filter", "include", "min_count", "missing", "ranges", "histogram", "calendar",
      "fixed", "timezone", "bounds" })
  void pathSegment(final String k) {
    assertThat(((SelectStatement) parse("SELECT FROM T WHERE a.%s = 1".formatted(k))).getWhereClause()).isNotNull();
  }

  @ParameterizedTest
  @ValueSource(strings = { "facets", "terms", "top", "filter", "include", "min_count", "missing", "ranges", "histogram", "calendar",
      "fixed", "timezone", "bounds" })
  void mapKey(final String k) {
    assertThat(((SelectStatement) parse("SELECT {%s: 1} AS m FROM T".formatted(k))).getProjection().getItems()).hasSize(1);
  }

  @ParameterizedTest
  @ValueSource(strings = { "facets", "terms", "top", "filter", "include", "min_count", "missing", "ranges", "histogram", "calendar",
      "fixed", "timezone", "bounds" })
  void functionAndMethodName(final String k) {
    // a user-defined function or a method may carry the name too
    assertThat(((SelectStatement) parse("SELECT %1$s(1) AS x, a.%1$s() AS y FROM T".formatted(k))).getProjection().getItems())
        .hasSize(2);
  }

  @ParameterizedTest
  @ValueSource(strings = { "facets", "terms", "top", "filter", "include", "min_count", "missing", "ranges", "histogram", "calendar",
      "fixed", "timezone", "bounds" })
  void typeName(final String k) {
    assertThat(parse("CREATE DOCUMENT TYPE %s".formatted(k))).isInstanceOf(CreateDocumentTypeStatement.class);
    final SelectStatement s = (SelectStatement) parse("SELECT FROM %1$s WHERE x = 1".formatted(k));
    assertThat(s.getTarget().toString()).isEqualTo(k);
    assertThat(s.getFacets()).isNull();
  }

  @ParameterizedTest
  @ValueSource(strings = { "facets", "terms", "top", "filter", "include", "min_count", "missing", "ranges", "histogram", "calendar",
      "fixed", "timezone", "bounds" })
  void fromAlias(final String k) {
    final SelectStatement s = (SelectStatement) parse("SELECT FROM T %1$s WHERE %1$s.x = :p LIMIT :n".formatted(k));
    assertThat(s.getTarget().toString()).isEqualTo("T AS " + k);
    assertThat(s.getLimit()).isNotNull();
    assertThat(s.getFacets()).isNull();
  }

  @ParameterizedTest
  @ValueSource(strings = { "facets", "terms", "top", "filter", "include", "min_count", "missing", "ranges", "histogram", "calendar",
      "fixed", "timezone", "bounds" })
  void unwindAlias(final String k) {
    final SelectStatement s = (SelectStatement) parse("SELECT FROM T UNWIND tags %1$s LIMIT :n".formatted(k));
    assertThat(s.getUnwind()).isNotNull();
    assertThat(s.getLimit()).isNotNull();
    assertThat(s.getFacets()).isNull();
  }

  @ParameterizedTest
  @ValueSource(strings = { "facets", "terms", "top", "filter", "include", "min_count", "missing", "ranges", "histogram", "calendar",
      "fixed", "timezone", "bounds" })
  void facetOnPropertyNamedK(final String k) {
    final FacetsClause facets = ((SelectStatement) parse("SELECT FROM T FACETS %s TERMS".formatted(k))).getFacets();
    assertThat(facets.items).hasSize(1);
    assertThat(facets.items.get(0).expression.toString()).isEqualTo(k);
  }

  @ParameterizedTest
  @ValueSource(strings = { "facets", "terms", "top", "filter", "include", "min_count", "missing", "ranges", "histogram", "calendar",
      "fixed", "timezone", "bounds" })
  void facetAliasNamedK(final String k) {
    final FacetsClause facets = ((SelectStatement) parse("SELECT FROM T FACETS %s: brand TERMS".formatted(k))).getFacets();
    assertThat(facets.items.get(0).alias.getStringValue()).isEqualTo(k);
  }

  @Test
  void keywordsAsPropertyNamesExecute() {
    database.command("sql", "CREATE DOCUMENT TYPE KW");
    for (final String k : KEYWORDS)
      database.command("sql", "CREATE PROPERTY KW.%s STRING".formatted(k));

    database.transaction(() -> {
      for (final String k : KEYWORDS)
        database.command("sql", "INSERT INTO KW SET %s = 'v'".formatted(k));
    });

    for (final String k : KEYWORDS)
      try (final ResultSet rs = database.query("sql", "SELECT %1$s FROM KW WHERE %1$s = 'v'".formatted(k))) {
        assertThat(rs.hasNext()).as(k).isTrue();
        final Result row = rs.next();
        assertThat(row.<String>getProperty(k)).as(k).isEqualTo("v");
        assertThat(rs.hasNext()).as(k).isFalse();
      }
  }
}
