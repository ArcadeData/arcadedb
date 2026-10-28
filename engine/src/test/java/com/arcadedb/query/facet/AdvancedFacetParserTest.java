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
package com.arcadedb.query.facet;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * HIERARCHY, DISTINCT, STATS and the AFTER cursor of the FACETS clause: parsing, compilation and the checks that need the schema.
 */
class AdvancedFacetParserTest extends TestHelper {

  @BeforeEach
  void schema() {
    database.command("sql", "CREATE VERTEX TYPE Tag");
    database.command("sql", "CREATE PROPERTY Tag.path STRING");
    database.command("sql", "CREATE EDGE TYPE ParentOf");
    database.command("sql", "CREATE DOCUMENT TYPE Asset");
    database.command("sql", "CREATE PROPERTY Asset.tags LIST OF STRING");
    database.command("sql", "CREATE PROPERTY Asset.brand STRING");
    database.command("sql", "CREATE PROPERTY Asset.fileSize LONG");
    database.command("sql", "CREATE PROPERTY Asset.price DECIMAL");
    database.command("sql", "CREATE PROPERTY Asset.published DATETIME");
    database.command("sql", "CREATE PROPERTY Asset.day DATE");
    database.command("sql", "CREATE PROPERTY Asset.ratio FLOAT");
    database.command("sql", "CREATE PROPERTY Asset.small BYTE");
    database.command("sql", "CREATE PROPERTY Asset.medium SHORT");
    database.command("sql", "CREATE PROPERTY Asset.count INTEGER");
    database.command("sql", "CREATE PROPERTY Asset.score DOUBLE");
    database.command("sql", "CREATE PROPERTY Asset.sec DATETIME_SECOND");
    database.command("sql", "CREATE PROPERTY Asset.micros DATETIME_MICROS");
    database.command("sql", "CREATE PROPERTY Asset.nanos DATETIME_NANOS");
    database.command("sql", "CREATE PROPERTY Asset.path STRING");
    database.command("sql", "CREATE INDEX ON Asset (path) COLUMNAR METADATA "
        + "{\"hierarchy\":{\"vertexType\":\"Tag\",\"key\":\"path\",\"parentEdge\":\"ParentOf\"}}");
    database.command("sql", "CREATE DOCUMENT TYPE Super");
    database.command("sql", "CREATE DOCUMENT TYPE Sub EXTENDS Super");
    database.command("sql", "CREATE PROPERTY Sub.x LONG");
    database.command("sql", "CREATE PROPERTY Sub.h STRING");
    database.command("sql", "CREATE INDEX ON Sub (x) COLUMNAR");
    database.command("sql", "CREATE INDEX ON Sub (h) COLUMNAR METADATA "
        + "{\"hierarchy\":{\"vertexType\":\"Tag\",\"key\":\"path\",\"parentEdge\":\"ParentOf\"}}");
    database.command("sql", "CREATE INDEX ON Asset (tags BY ITEM) COLUMNAR");
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    database.command("sql", "CREATE INDEX ON Asset (fileSize) COLUMNAR");
  }

  private static List<FacetSpec> facetsOf(final String sql, final Map<String, Object> params) {
    final CommandContext ctx = new BasicCommandContext();
    ctx.setInputParameters(params);
    return FacetSpecCompiler.compile(((SelectStatement) new SQLAntlrParser(null).parse(sql)).getFacets(), ctx);
  }

  private static List<FacetSpec> facetsOf(final String sql) {
    return facetsOf(sql, Map.of());
  }

  @Test
  void parsesAdvancedKinds() {
    final List<FacetSpec> specs = facetsOf("SELECT FROM Asset FACETS t: tags HIERARCHY TOP 20 MIN_COUNT 2, a: brand DISTINCT, "
        + "n: fileSize DISTINCT PRECISION 3000, s: fileSize STATS, p: brand TERMS TOP 5 AFTER 'Nike'");
    assertThat(specs.get(0)).isEqualTo(new HierarchyFacetSpec("t", "tags", null, 20, 2));
    assertThat(((DistinctFacetSpec) specs.get(1)).precisionThreshold()).isEqualTo(Long.MAX_VALUE);
    assertThat(((DistinctFacetSpec) specs.get(2)).precisionThreshold()).isEqualTo(3000L);
    assertThat(specs.get(3)).isInstanceOf(StatsFacetSpec.class);
    assertThat(((TermsFacetSpec) specs.get(4)).after()).isEqualTo("Nike");
  }

  @Test
  void defaultsAndFilters() {
    final List<FacetSpec> specs = facetsOf("SELECT FROM Asset FACETS t: tags HIERARCHY, "
        + "a: brand DISTINCT FILTER fileSize > 5, s: fileSize STATS FILTER brand = 'x'");
    final HierarchyFacetSpec hierarchy = (HierarchyFacetSpec) specs.get(0);
    assertThat(hierarchy.top()).isEqualTo(10);
    assertThat(hierarchy.minCount()).isEqualTo(1);
    assertThat(specs.get(1).filter()).isNotNull();
    assertThat(specs.get(2).filter()).isNotNull();
  }

  @Test
  void afterTakesAParameter() {
    assertThat(((TermsFacetSpec) facetsOf("SELECT FROM Asset FACETS brand TERMS AFTER :c", Map.of("c", "Nike")).getFirst()).after())
        .isEqualTo("Nike");
    assertThat(((TermsFacetSpec) facetsOf("SELECT FROM Asset FACETS fileSize TERMS AFTER 7").getFirst()).after()).isEqualTo(7);
    assertThat(((TermsFacetSpec) facetsOf("SELECT FROM Asset FACETS fileSize TERMS AFTER -7 ORDER BY VALUE ASC").getFirst()).after())
        .isEqualTo(-7);
  }

  @Test
  void afterIsAConstant() {
    assertThatThrownBy(() -> facetsOf("SELECT FROM Asset FACETS brand TERMS AFTER brand")).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("AFTER takes a literal or a parameter");
    assertThatThrownBy(() -> facetsOf("SELECT FROM Asset FACETS brand TERMS AFTER 'a' AFTER 'b'")).isInstanceOf(
        CommandSQLParsingException.class).hasMessageContaining("AFTER given twice in facet 'brand'");
  }

  @Test
  void textRoundTrips() {
    final String sql = "SELECT FROM Asset FACETS t: tags HIERARCHY TOP 20 MIN_COUNT 2 FILTER brand = 'x', a: brand DISTINCT PRECISION 3000 "
        + "FILTER fileSize > 1, s: fileSize STATS, p: brand TERMS TOP 5 ORDER BY VALUE ASC AFTER 'Nike' FILTER fileSize > 2";
    final SelectStatement parsed = (SelectStatement) new SQLAntlrParser(null).parse(sql);
    final SelectStatement reparsed = (SelectStatement) new SQLAntlrParser(null).parse(parsed.toString());
    assertThat(reparsed.toString()).isEqualTo(parsed.toString());
    assertThat(reparsed.getFacets()).isEqualTo(parsed.getFacets());
    assertThat(parsed.copy().getFacets()).isEqualTo(parsed.getFacets());
    assertThat(parsed.toString()).contains("DISTINCT PRECISION 3000").contains("AFTER 'Nike'");
  }

  @Test
  void afterWithOrderByCountIsRefused() {
    assertThatThrownBy(() -> database.query("sql", "SELECT FROM Asset FACETS brand TERMS ORDER BY COUNT AFTER 'Nike'"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessage("AFTER pages in value order: remove ORDER BY COUNT");
  }

  @Test
  void afterWithOrderByValueDescIsRefused() {
    assertThatThrownBy(() -> database.query("sql", "SELECT FROM Asset FACETS brand TERMS ORDER BY VALUE DESC AFTER 'Nike'"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessage("AFTER pages in ascending value order: remove ORDER BY VALUE DESC");
  }

  @Test
  void afterWithIncludeIsRefused() {
    assertThatThrownBy(() -> database.query("sql", "SELECT FROM Asset FACETS brand TERMS INCLUDE ['a'] AFTER 'Nike'"))
        .isInstanceOf(CommandSQLParsingException.class).hasMessage("AFTER and INCLUDE cannot be combined");
  }

  @Test
  void afterWithOrderByValueAscIsAccepted() {
    assertThat(database.query("sql", "SELECT FROM Asset FACETS brand TERMS ORDER BY VALUE ASC AFTER 'Nike'").hasNext()).isFalse();
  }

  @Test
  void statsOnStringIsRefused() {
    assertThatThrownBy(() -> database.query("sql", "SELECT FROM Asset FACETS brand STATS")).isInstanceOf(CommandSQLParsingException.class)
        .hasMessage("STATS requires a property of type BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, DATE or DATETIME: brand");
  }

  @Test
  void statsOnDecimalIsRefused() {
    assertThatThrownBy(() -> database.query("sql", "SELECT FROM Asset FACETS price STATS")).isInstanceOf(CommandSQLParsingException.class)
        .hasMessage("STATS requires a property of type BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, DATE or DATETIME: price");
  }

  @Test
  void statsOnUndeclaredPropertyIsRefused() {
    assertThatThrownBy(() -> database.query("sql", "SELECT FROM Asset FACETS nope STATS")).isInstanceOf(CommandSQLParsingException.class)
        .hasMessage("STATS requires a property of type BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, DATE or DATETIME: nope");
  }

  @Test
  void statsOnNumericAndDateTypesIsAccepted() {
    // validation only: the STATS collector arrives with a later task
    for (final String property : new String[] { "fileSize", "ratio", "published", "day", "small", "medium", "count", "score", "sec" })
      FacetSpecCompiler.validateAgainstSchema(facetsOf("SELECT FROM Asset FACETS " + property + " STATS"), database.getSchema().getType("Asset"));
  }

  @Test
  void statsOnMicrosAndNanosIsRefused() {
    for (final String property : new String[] { "micros", "nanos" })
      assertThatThrownBy(() -> database.query("sql", "SELECT FROM Asset FACETS " + property + " STATS")).as(property)
          .isInstanceOf(CommandSQLParsingException.class).hasMessageStartingWith("STATS requires a property of type");
  }

  @Test
  void hierarchyWithMetadataIsAccepted() {
    FacetSpecCompiler.validateAgainstSchema(facetsOf("SELECT FROM Asset FACETS path HIERARCHY"), database.getSchema().getType("Asset"));
    assertThat(facetsOf("SELECT FROM Asset FACETS path HIERARCHY")).hasSize(1);
  }

  @Test
  void facetsOnAPropertyOfASubtypeArePlannedFromTheSupertype() {
    final DocumentType superType = database.getSchema().getType("Super");
    for (final String sql : new String[] { "x STATS", "x DISTINCT", "h HIERARCHY" })
      FacetSpecCompiler.validateAgainstSchema(facetsOf("SELECT FROM Super FACETS " + sql), superType);
  }

  @Test
  void aPropertyDeclaredNowhereStillFailsFromTheSupertype() {
    final DocumentType superType = database.getSchema().getType("Super");
    assertThatThrownBy(() -> FacetSpecCompiler.validateAgainstSchema(facetsOf("SELECT FROM Super FACETS nope STATS"), superType))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageStartingWith("STATS requires a property of type");
    assertThatThrownBy(() -> FacetSpecCompiler.validateAgainstSchema(facetsOf("SELECT FROM Super FACETS nope HIERARCHY"), superType))
        .isInstanceOf(CommandSQLParsingException.class).hasMessage("HIERARCHY requires a COLUMNAR index with METADATA hierarchy on Super.nope");
  }

  @Test
  void hierarchyWithoutMetadataIsRefused() {
    assertThatThrownBy(() -> database.query("sql", "SELECT FROM Asset FACETS brand HIERARCHY"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessage("HIERARCHY requires a COLUMNAR index with METADATA hierarchy on Asset.brand");
  }

  @Test
  void hierarchyWithoutIndexIsRefused() {
    assertThatThrownBy(() -> database.query("sql", "SELECT FROM Asset FACETS price HIERARCHY"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessage("HIERARCHY requires a COLUMNAR index with METADATA hierarchy on Asset.price");
  }

  @Test
  void bucketTargetSkipsTheSchemaChecks() {
    // no declaring type is known at planning (a bucket or subquery target): only the type-free checks run, the executor does the rest
    final List<FacetSpec> specs = facetsOf("SELECT FROM Asset FACETS brand STATS, t: price HIERARCHY");
    FacetSpecCompiler.validateAgainstSchema(specs, null);
    assertThatThrownBy(() -> FacetSpecCompiler.validateAgainstSchema(specs, database.getSchema().getType("Asset")))
        .isInstanceOf(CommandSQLParsingException.class);
  }

  @Test
  void bucketAndSubqueryTargetsPlanWithoutTheSchemaChecks() {
    final String bucket = database.getSchema().getType("Asset").getBuckets(false).getFirst().getName();
    // brand is a STRING: against the Asset type STATS is refused, a bucket or subquery has no type to refuse it with at planning
    assertThat(database.query("sql", "EXPLAIN SELECT FROM bucket:" + bucket + " FACETS brand STATS").hasNext()).isTrue();
    assertThat(database.query("sql", "EXPLAIN SELECT FROM (SELECT FROM Asset) FACETS price HIERARCHY").hasNext()).isTrue();
  }

  @Test
  void afterParameterIsBoundPerExecution() {
    assertThat(database.query("sql", "SELECT FROM Asset FACETS brand TERMS AFTER :c", Map.of("c", "Nike")).hasNext()).isFalse();
    final Map<String, Object> bound = new HashMap<>();
    bound.put("c", null);
    assertThatThrownBy(() -> database.query("sql", "SELECT FROM Asset FACETS brand TERMS AFTER :c", bound))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("missing or bound to null");
    assertThatThrownBy(() -> database.query("sql", "SELECT FROM Asset FACETS brand TERMS AFTER :c", Map.of("d", "x")))
        .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("missing or bound to null");
  }
}
