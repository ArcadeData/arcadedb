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

import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Grammar and AST of the FACETS clause of SELECT (columnar faceting).
 */
class SQLFacetsParserTest {
  private static final String SQL = """
      SELECT @rid, title FROM Asset WHERE search_fields(['title', 'body'], 'running shoes')
      FACETS brand TERMS TOP 10 FILTER brand IN ['Nike', 'Adidas'] INCLUDE ['Nike', 'Adidas'],
        tags TERMS TOP 20 MIN_COUNT 2 ORDER BY COUNT DESC,
        size: fileSize RANGES ([0, 1048576) AS 'small', [1048576, 10485760) AS 'medium', [10485760, *) AS 'large') DEFAULT 'other',
        pub: publishDate HISTOGRAM CALENDAR '1M' TIMEZONE 'Europe/Rome' OFFSET '+6h' MIN_COUNT 0 BOUNDS ['2026-01-01', '2026-12-31']
      LIMIT 20""";

  private static Statement parseAny(final String sql) {
    return new SQLAntlrParser(null).parse(sql);
  }

  private static SelectStatement parse(final String sql) {
    return (SelectStatement) parseAny(sql);
  }

  @Test
  void fullSpecExampleParses() {
    final SelectStatement s = parse(SQL);
    assertThat(s.getFacets().items).hasSize(4);
    assertThat(s.getLimit().getValue(null)).isEqualTo(20);

    final FacetItem brand = s.getFacets().items.get(0);
    assertThat(brand.kind).isEqualTo(FacetItem.Kind.TERMS);
    assertThat(brand.getName()).isEqualTo("brand");
    assertThat(brand.top.getValue()).isEqualTo(10);
    assertThat(brand.filter).isNotNull();
    assertThat(brand.include).hasSize(2);
    assertThat(brand.include.get(0).toString()).isEqualTo("'Nike'");

    final FacetItem tags = s.getFacets().items.get(1);
    assertThat(tags.top.getValue()).isEqualTo(20);
    assertThat(tags.minCount.getValue()).isEqualTo(2);
    assertThat(tags.orderByCount).isTrue();
    assertThat(tags.ascending).isFalse();
    assertThat(tags.missing).isFalse();
    assertThat(tags.filter).isNull();

    final FacetItem size = s.getFacets().items.get(2);
    assertThat(size.alias.getStringValue()).isEqualTo("size");
    assertThat(size.kind).isEqualTo(FacetItem.Kind.RANGES);
    assertThat(size.ranges).hasSize(3);
    assertThat(size.ranges.get(0).from).isEqualTo(0);
    assertThat(size.ranges.get(0).to).isEqualTo(1048576);
    assertThat(size.ranges.get(0).label).isEqualTo("small");
    assertThat(size.ranges.get(2).from).isEqualTo(10485760);
    assertThat(size.ranges.get(2).to).isNull();
    assertThat(size.defaultLabel).isEqualTo("other");

    final FacetItem pub = s.getFacets().items.get(3);
    assertThat(pub.kind).isEqualTo(FacetItem.Kind.HISTOGRAM);
    assertThat(pub.calendar).isTrue();
    assertThat(pub.interval).isEqualTo("1M");
    assertThat(pub.timezone).isEqualTo("Europe/Rome");
    assertThat(pub.offset).isEqualTo("+6h");
    assertThat(pub.minCount.getValue()).isEqualTo(0);
    assertThat(pub.boundsFrom).isEqualTo("2026-01-01");
    assertThat(pub.boundsTo).isEqualTo("2026-12-31");
  }

  @Test
  void boundsKeepTheirTypes() {
    final FacetItem item = parse("""
        SELECT FROM A FACETS s: size RANGES ([-5, 2.5) AS 'x', [:lo, ?), [*, 3000000000)), \
        h: n HISTOGRAM FIXED 100 BOUNDS [-1, *]""").getFacets().items.get(0);
    assertThat(item.ranges.get(0).from).isEqualTo(-5);
    assertThat(item.ranges.get(0).to).isEqualTo(2.5d);
    assertThat(item.ranges.get(1).from).isInstanceOf(InputParameter.class);
    assertThat(item.ranges.get(1).to).isInstanceOf(InputParameter.class);
    assertThat(item.ranges.get(1).label).isNull();
    assertThat(item.ranges.get(2).from).isNull();
    assertThat(item.ranges.get(2).to).isEqualTo(3000000000L);

    final FacetItem histogram = parse("SELECT FROM A FACETS h: n HISTOGRAM FIXED 100 BOUNDS [-1, *]").getFacets().items.get(0);
    assertThat(histogram.calendar).isFalse();
    assertThat(histogram.interval).isEqualTo(100);
    assertThat(histogram.boundsFrom).isEqualTo(-1);
    assertThat(histogram.boundsTo).isNull();
  }

  @Test
  void toStringRoundTrips() {
    final String once = parse(SQL).toString();
    assertThat(parse(once).toString()).isEqualTo(once);
    assertThat(once).contains(" FACETS ").contains(" LIMIT 20");
    assertThat(once.indexOf(" FACETS ")).isLessThan(once.indexOf(" LIMIT "));

    // FILTER is printed last, so its condition never swallows an option that follows it on the re-parse
    final String other = parse("""
        SELECT FROM A FACETS b TERMS FILTER b = 'x' MISSING ORDER BY VALUE ASC INCLUDE [:p] MIN_COUNT 3 TOP 2, \
        r: n RANGES ([*, -1.5) AS 'neg\\'s', [-1.5, ?)) DEFAULT 'rest' FILTER n > 0, \
        h: d HISTOGRAM FIXED '90m' FILTER d IS NOT NULL OFFSET '+1h' TIMEZONE 'UTC' BOUNDS [:a, :b] MIN_COUNT 0 \
        SKIP 3 LIMIT 4""").toString();
    final SelectStatement reparsed = parse(other);
    assertThat(reparsed.toString()).isEqualTo(other);
    assertThat(reparsed).isEqualTo(parse(other));
    assertThat(other).contains("b TERMS TOP 2 MIN_COUNT 3 ORDER BY VALUE ASC INCLUDE [:p] MISSING FILTER b = 'x'");
    assertThat(reparsed.getFacets().items.get(1).ranges.get(0).label).isEqualTo("neg's");
  }

  @Test
  void copyIsEqual() {
    final SelectStatement s = parse(SQL);
    assertThat(s.copy()).isEqualTo(s);
    assertThat(s.copy().hashCode()).isEqualTo(s.hashCode());
    assertThat(s.copy().getFacets()).isNotSameAs(s.getFacets());

    // the facets take part in the identity of the statement
    assertThat(parse("SELECT FROM A FACETS b TERMS TOP 1")).isNotEqualTo(parse("SELECT FROM A FACETS b TERMS TOP 2"));
    assertThat(parse("SELECT FROM A FACETS b TERMS")).isNotEqualTo(parse("SELECT FROM A"));
  }

  @Test
  void toJSONCarriesTheItems() {
    final FacetsClause facets = parse(SQL).getFacets();
    assertThat(facets.toJSON().get("items")).isInstanceOf(List.class);
    assertThat((List<?>) facets.toJSON().get("items")).hasSize(4);
  }

  @Test
  void facetsIsNotTakenAsTargetAlias() {
    assertThat(parse("SELECT FROM Asset FACETS brand TERMS").getFacets()).isNotNull();
  }

  @Test
  void aliasNamedFacetsStillParses() {
    final SelectStatement s = parse("SELECT FROM Asset facets WHERE facets.x = 1");
    assertThat(s.getFacets()).isNull();
    assertThat(s.getWhereClause()).isNotNull();
  }

  @Test
  void unwindThenFacets() {
    final SelectStatement s = parse("SELECT FROM Asset UNWIND tags FACETS brand TERMS");
    assertThat(s.getFacets()).isNotNull();
    assertThat(s.getUnwind()).isNotNull();
  }

  @Test
  void appendedLimitAfterFacetsParses() {
    final SelectStatement s = parse("SELECT FROM Asset FACETS brand TERMS TOP 5 limit 21");
    assertThat(s.getLimit()).isNotNull();
    assertThat(s.getFacets().items.get(0).top.getValue()).isEqualTo(5);

    // the condition of a FILTER written last does not take the appended LIMIT either
    final SelectStatement filtered = parse("SELECT FROM Asset FACETS brand TERMS FILTER brand = 'x' limit 21");
    assertThat(filtered.getLimit().getValue(null)).isEqualTo(21);
  }

  @Test
  void facetsAfterLimitIsRefused() {
    assertThatThrownBy(() -> parse("SELECT FROM Asset LIMIT 20 FACETS brand TERMS"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("FACETS must come before SKIP and LIMIT");
    assertThatThrownBy(() -> parse("SELECT FROM Asset SKIP 2 FACETS brand TERMS"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("FACETS must come before SKIP and LIMIT");
    assertThatThrownBy(() -> parse("SELECT FROM Asset SKIP 2 LIMIT 3 FACETS brand TERMS"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("FACETS must come before SKIP and LIMIT");
    assertThatThrownBy(() -> parse("SELECT FROM Asset LIMIT 3 SKIP 2 FACETS brand TERMS"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("FACETS must come before SKIP and LIMIT");
  }

  @Test
  void facetsInSubqueryIsRefused() {
    assertThatThrownBy(() -> parse("SELECT FROM (SELECT FROM Asset FACETS brand TERMS)"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("FACETS is only allowed on the outermost SELECT");
    assertThatThrownBy(() -> parse("SELECT FROM Asset WHERE x IN (SELECT y FROM B FACETS y TERMS)"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("FACETS is only allowed on the outermost SELECT");
  }

  @Test
  void facetsInLetAndInsertAreRefused() {
    assertThatThrownBy(() -> parse("SELECT FROM Asset LET $a = (SELECT FROM B FACETS b TERMS)"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("FACETS is only allowed on the outermost SELECT");
    assertThatThrownBy(() -> parseAny("INSERT INTO X SELECT FROM B FACETS b TERMS"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("FACETS is only allowed on the outermost SELECT");
    assertThatThrownBy(() -> new SQLAntlrParser(null).parseScript("LET $a = SELECT FROM B FACETS b TERMS; RETURN $a"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("FACETS is only allowed on the outermost SELECT");
  }

  @Test
  void facetsInViewDefinitionsAreRefused() {
    assertThatThrownBy(() -> parseAny("CREATE MATERIALIZED VIEW v AS SELECT FROM A FACETS b TERMS"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("FACETS is only allowed on the outermost SELECT");
    assertThatThrownBy(() -> parseAny("CREATE CONTINUOUS AGGREGATE c AS SELECT count(*) AS n FROM A FACETS b TERMS"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("FACETS is only allowed on the outermost SELECT");
  }

  @Test
  void explainAndProfileAndScriptKeepFacets() {
    final Statement explain = parseAny("EXPLAIN SELECT FROM A FACETS b TERMS");
    assertThat(((SelectStatement) ((ExplainStatement) explain).statement).getFacets()).isNotNull();

    final Statement profile = parseAny("PROFILE SELECT FROM A FACETS b TERMS");
    assertThat(((SelectStatement) ((ProfileStatement) profile).statement).getFacets()).isNotNull();

    final List<Statement> script = new SQLAntlrParser(null).parseScript("BEGIN; SELECT FROM A FACETS b TERMS; COMMIT;");
    assertThat(script).hasSize(3);
    assertThat(((SelectStatement) script.get(1)).getFacets()).isNotNull();

    // a statement in an IF, WHILE or FOREACH block is still a statement of the script
    final List<Statement> blocks = new SQLAntlrParser(null).parseScript("""
        IF (1 = 1) { SELECT FROM A FACETS b TERMS; }
        WHILE (1 = 2) { SELECT FROM A FACETS b TERMS; }
        FOREACH ($i IN [1, 2]) { SELECT FROM A FACETS b TERMS; }""");
    assertThat(blocks).hasSize(3);
  }

  @Test
  void facetsWithoutFromIsRefused() {
    assertThatThrownBy(() -> parse("SELECT 1 AS x FACETS x TERMS").validate())
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("FACETS requires a FROM clause");
  }

  @Test
  void duplicateOptionIsRefused() {
    assertThatThrownBy(() -> parse("SELECT FROM A FACETS b TERMS TOP 1 TOP 2"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("TOP given twice in facet 'b'");
    assertThatThrownBy(() -> parse("SELECT FROM A FACETS x: b TERMS MISSING MISSING"))
        .hasMessageContaining("MISSING given twice in facet 'x'");
    assertThatThrownBy(() -> parse("SELECT FROM A FACETS b TERMS ORDER BY COUNT ORDER BY VALUE"))
        .hasMessageContaining("ORDER BY given twice in facet 'b'");
    assertThatThrownBy(() -> parse("SELECT FROM A FACETS b TERMS FILTER b = 1 FILTER b = 2"))
        .hasMessageContaining("FILTER given twice in facet 'b'");
    assertThatThrownBy(() -> parse("SELECT FROM A FACETS h: d HISTOGRAM CALENDAR '1d' TIMEZONE 'UTC' TIMEZONE 'UTC'"))
        .hasMessageContaining("TIMEZONE given twice in facet 'h'");
    assertThatThrownBy(() -> parse("SELECT FROM A FACETS h: d HISTOGRAM CALENDAR '1d' MIN_COUNT 0 MIN_COUNT 1"))
        .hasMessageContaining("MIN_COUNT given twice in facet 'h'");
    assertThatThrownBy(() -> parse("SELECT FROM A FACETS h: d HISTOGRAM CALENDAR '1d' BOUNDS [1, 2] BOUNDS [1, 2]"))
        .hasMessageContaining("BOUNDS given twice in facet 'h'");
    // an unbounded BOUNDS is still a BOUNDS given
    assertThatThrownBy(() -> parse("SELECT FROM A FACETS h: d HISTOGRAM CALENDAR '1d' BOUNDS [*, *] BOUNDS [1, 2]"))
        .hasMessageContaining("BOUNDS given twice in facet 'h'");
  }

  @Test
  void plan5KindsAreNotParsedYet() {
    assertThatThrownBy(() -> parse("SELECT FROM A FACETS t: tags HIERARCHY")).isInstanceOf(CommandSQLParsingException.class);
    assertThatThrownBy(() -> parse("SELECT FROM A FACETS t: tags STATS")).isInstanceOf(CommandSQLParsingException.class);
  }

  @Test
  void distinctProjectionUnaffected() {
    final SelectStatement s = parse("SELECT DISTINCT brand FROM A FACETS brand TERMS");
    assertThat(s.getFacets()).isNotNull();
    assertThat(s.getProjection().isDistinct()).isTrue();
  }

  @Test
  void sameFieldTwiceNeedsAliases() {
    assertThatThrownBy(() -> parse("SELECT FROM A FACETS brand TERMS, brand TERMS TOP 3"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("duplicate facet alias 'brand'");
    assertThatThrownBy(() -> parse("SELECT FROM A FACETS x: brand TERMS, x: tags TERMS"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("duplicate facet alias 'x'");
    assertThat(parse("SELECT FROM A FACETS b1: brand TERMS, b2: brand TERMS TOP 3").getFacets().items).hasSize(2);

    // the default name is the one a projection gives the expression: without back-ticks
    assertThatThrownBy(() -> parse("SELECT FROM A FACETS `b` TERMS, b TERMS"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("duplicate facet alias 'b'");
    assertThat(parse("SELECT FROM A FACETS `my field` TERMS").getFacets().items.get(0).getName()).isEqualTo("my field");
  }

  @Test
  void metaAliasIsReserved() {
    assertThatThrownBy(() -> parse("SELECT FROM A FACETS meta: brand TERMS"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("facet alias 'meta' is reserved");
    assertThatThrownBy(() -> parse("SELECT FROM A FACETS meta TERMS"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("facet alias 'meta' is reserved");
    assertThat(parse("SELECT FROM A FACETS m: meta TERMS").getFacets().items.get(0).getName()).isEqualTo("m");
  }

  @Test
  void moreThan31FiltersIsRefused() {
    final StringBuilder sql = new StringBuilder("SELECT FROM A FACETS ");
    for (int i = 0; i < 32; i++) {
      if (i > 0)
        sql.append(", ");
      sql.append("f").append(i).append(": p").append(i).append(" TERMS FILTER p").append(i).append(" = 1");
    }
    assertThatThrownBy(() -> parse(sql.toString()))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("at most 31 facets may carry a FILTER");

    // 31 filters and one more facet without a FILTER are accepted
    final String accepted = sql.toString().replace("f31: p31 TERMS FILTER p31 = 1", "f31: p31 TERMS");
    assertThat(parse(accepted).getFacets().items).hasSize(32);
  }

  @Test
  void subqueryAliasThenFacets() {
    final SelectStatement facets = parse("SELECT FROM (SELECT FROM A) FACETS b TERMS");
    assertThat(facets.getFacets()).isNotNull();

    final SelectStatement alias = parse("SELECT FROM (SELECT FROM A) facets WHERE facets.x = 1");
    assertThat(alias.getFacets()).isNull();
    assertThat(alias.getWhereClause()).isNotNull();
  }

  @Test
  void facetAliasNamedFacetsIsNotTakenAsTheAliasOfWhatPrecedes() {
    // "T FACETS facets: brand" also reads as the alias FACETS and a facet on the parameter :brand: the clause wins
    for (final String sql : new String[] { "SELECT FROM T FACETS facets: brand TERMS", "SELECT FROM (SELECT FROM T) FACETS facets: brand TERMS",
        "SELECT FROM T UNWIND tags FACETS facets: brand TERMS", "SELECT FROM T UNWIND a, tags FACETS facets: brand TERMS" }) {
      final SelectStatement s = parse(sql);
      assertThat(s.getFacets().items.get(0).alias.getStringValue()).as(sql).isEqualTo("facets");
      assertThat(s.getFacets().items.get(0).expression.toString()).as(sql).isEqualTo("brand");
    }
    assertThat(parse("SELECT FROM T FACETS facets: brand TERMS").getTarget().toString()).isEqualTo("T");

    // an alias written with AS, or followed by another clause, is still the alias
    assertThat(parse("SELECT FROM T AS FACETS FACETS :brand TERMS").getTarget().toString()).isEqualTo("T AS FACETS");
    assertThat(parse("SELECT FROM T facets FACETS b: brand TERMS").getTarget().toString()).isEqualTo("T AS facets");
  }

  @Test
  void bareAliasFacetsBeforeAClauseAndANamedParameterIsStillTheAlias() {
    for (final String sql : new String[] { "SELECT FROM T facets WHERE :p = facets.x", "SELECT FROM T facets LIMIT :n",
        "SELECT FROM T facets SKIP :s LIMIT 3" }) {
      final SelectStatement s = parse(sql);
      assertThat(s.getTarget().toString()).as(sql).isEqualTo("T AS facets");
      assertThat(s.getFacets()).as(sql).isNull();
    }
    final SelectStatement unwound = parse("SELECT FROM T UNWIND tags facets LIMIT :n");
    assertThat(unwound.getFacets()).isNull();
    assertThat(unwound.getLimit()).isNotNull();
    assertThat(parseAny("DELETE FROM T facets WHERE :p = 1")).isInstanceOf(DeleteStatement.class);

    // UNWIND is no identifier, so this text has one reading only: the alias facets and UNWIND :p AS terms
    final SelectStatement unwind = parse("SELECT FROM T facets UNWIND :p terms");
    assertThat(unwind.getTarget().toString()).isEqualTo("T AS facets");
    assertThat(unwind.getUnwind()).isNotNull();
    assertThat(unwind.getFacets()).isNull();
  }

  @Test
  void facetAliasNamedLikeAClauseKeyword() {
    // the clause keywords that are identifiers: the alias reading (FACETS as the target alias) dies on the facet body
    for (final String alias : new String[] { "where", "limit", "skip", "`unwind`", "`timeout`" }) {
      final SelectStatement s = parse("SELECT FROM T FACETS " + alias + ": brand TERMS");
      assertThat(s.getTarget().toString()).as(alias).isEqualTo("T");
      assertThat(s.getFacets().items.get(0).alias.getStringValue()).as(alias).isEqualTo(alias.replace("`", ""));
    }
  }

  @Test
  void bareUnwindAfterFacetsIsAnUnwind() {
    // UNWIND is no identifier: "FACETS unwind: p TERMS" has the one reading alias FACETS + UNWIND :p AS TERMS, so a facet alias
    // spelled unwind needs its back-ticks (facetAliasNamedLikeAClauseKeyword)
    final SelectStatement s = parse("SELECT FROM T FACETS unwind: p TERMS");
    assertThat(s.getTarget().toString()).isEqualTo("T AS FACETS");
    assertThat(s.getUnwind()).isNotNull();
    assertThat(s.getFacets()).isNull();
  }

  @Test
  void refersToParentLooksIntoFacets() {
    assertThat(parse("SELECT FROM A FACETS b TERMS FILTER b = $parent.x").refersToParent()).isTrue();
    assertThat(parse("SELECT FROM A FACETS b TERMS FILTER b = 1").refersToParent()).isFalse();
  }
}
