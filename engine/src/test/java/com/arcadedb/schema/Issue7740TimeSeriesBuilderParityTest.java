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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7740: three divergences between the embedded and the remote TimeSeries schema paths, which are meant to
 * be the same builder body producing the same type.
 * <ol>
 * <li>{@code quote()} refused a back-quote and passed a BACKSLASH through, which is the defect #5849 fixed once
 * already in {@code MCPToolUtils}: the grammar's {@code QUOTED_IDENTIFIER : BACKTICK ( ~[`\\] | '\\' . )+ BACKTICK}
 * gives the backslash its escaping meaning, so {@code a\b} rendered as a name the server stored as {@code ab} -
 * and {@code create()} then failed with "Type with name 'a\b' was not found" over a fully built type under a name
 * nobody asked for. A trailing backslash was a parse error instead. The embedded path accepted both names.</li>
 * <li>The column ORDER: the SQL rendering has one slot for the timestamp, one for TAGS and one for FIELDS, so the
 * remote path regroups; the embedded one replayed insertion order. One builder body, two types.</li>
 * <li>The single-TIMESTAMP rule was enforced only when rendering SQL, so a two-TIMESTAMP builder - which
 * {@code JsonlImporterFormat} can produce by looping {@code withColumn} over an export - built an embedded type
 * whose {@code getTimestampColumn()} and {@code findTimestampColumnIndex()} named different columns.</li>
 * </ol>
 * The fourth item of the issue, {@code RemoteSchema.reload()} keeping a cached type's Java class, is pinned in
 * {@code Issue7740RemoteSchemaReloadKindIT} - it needs a server.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7740">issue #7740</a>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7740TimeSeriesBuilderParityTest extends TestHelper {

  private TimeSeriesTypeBuilder builder(final String name) {
    return database.getSchema().buildTimeSeriesType().withName(name).withTimestamp("ts");
  }

  private static List<String> columnNames(final TimeSeriesType type) {
    final List<String> names = new ArrayList<>();
    for (final ColumnDefinition col : type.getTsColumns())
      names.add(col.getName());
    return names;
  }

  // ---- 1. the identifier escaping ----

  /**
   * A name with a backslash renders as DDL that parses AND that names the column the caller asked for. Rendering
   * alone is not enough to see the bug: {@code `a\b`} parses fine, it simply means a different name.
   */
  @Test
  void aBackslashInANameIsEscapedRatherThanPassedThrough() {
    final String odd = "a\\b";
    final String sql = builder("Backslash").withField(odd, Type.DOUBLE).toSQL().getFirst();

    assertThatNoException().as(sql).isThrownBy(() -> new SQLAntlrParser(null).parse(sql));
    assertThat(sql).as("the backslash is escaped, so the name survives the round trip").contains("`a\\\\b`");

    database.command("sql", sql);
    assertThat(columnNames((TimeSeriesType) database.getSchema().getType("Backslash")))
        .as("the server built the columns the builder declared, not a name with the backslash eaten")
        .containsExactly("ts", odd);
  }

  /** A name ENDING with a backslash used to be a parse error: unescaped, it swallowed the closing back-quote. */
  @Test
  void aTrailingBackslashDoesNotSwallowTheClosingQuote() {
    final String sql = builder("TrailingBackslash").withField("weird\\", Type.DOUBLE).toSQL().getFirst();

    assertThatNoException().as(sql).isThrownBy(() -> new SQLAntlrParser(null).parse(sql));
    database.command("sql", sql);
    assertThat(columnNames((TimeSeriesType) database.getSchema().getType("TrailingBackslash")))
        .containsExactly("ts", "weird\\");
  }

  /** And the back-quote, which used to be refused outright, is escaped like any other special character. */
  @Test
  void aBackQuoteInANameIsEscapedInsteadOfRefused() {
    final String sql = builder("BackQuote").withField("we`ird", Type.DOUBLE).toSQL().getFirst();

    assertThatNoException().as(sql).isThrownBy(() -> new SQLAntlrParser(null).parse(sql));
    database.command("sql", sql);
    assertThat(columnNames((TimeSeriesType) database.getSchema().getType("BackQuote")))
        .containsExactly("ts", "we`ird");
  }

  /** An ordinary name renders exactly as it always did: the escaping is not a new quoting style. */
  @Test
  void anOrdinaryNameIsUnchanged() {
    assertThat(builder("Ordinary").withTag("host", Type.STRING).withField("value", Type.DOUBLE).toSQL().getFirst())
        .isEqualTo("CREATE TIMESERIES TYPE `Ordinary` TIMESTAMP `ts` TAGS (`host` STRING) FIELDS (`value` DOUBLE)");
  }

  // ---- 2. the column order ----

  /**
   * A recipe the grammar CAN spell produces the same type either way, which is the parity #7740 asks for.
   */
  @Test
  void theEmbeddedAndTheRenderedPathOrderTheColumnsAlike() {
    final TimeSeriesType embedded = database.getSchema().buildTimeSeriesType().withName("Embedded")
        .withTimestamp("ts")
        .withTag("host", Type.STRING)
        .withField("cpu", Type.DOUBLE)
        .create();

    database.command("sql", database.getSchema().buildTimeSeriesType().withName("Rendered")
        .withTimestamp("ts")
        .withTag("host", Type.STRING)
        .withField("cpu", Type.DOUBLE)
        .toSQL().getFirst());
    final TimeSeriesType rendered = (TimeSeriesType) database.getSchema().getType("Rendered");

    assertThat(columnNames(embedded)).containsExactly("ts", "host", "cpu");
    assertThat(columnNames(rendered)).as("one builder body, one column order").isEqualTo(columnNames(embedded));
  }

  /**
   * And a recipe it cannot spell is NOT silently turned into a different type on the embedded side to match.
   * <p>
   * Regrouping the embedded declaration was the first answer to #7740 and it was wrong in a way the engine can
   * feel: the stored order is the type's identity - column indices are positions in it, and a sample is a
   * positional array - so a logical restore, which maps an export's sample arrays onto the type it has just
   * rebuilt, would have put every value in the wrong column ({@code Issue7371PromQLDiscoveryProjectionIT} builds
   * exactly that layout on purpose, and caught it). What the rendering cannot carry is documented on
   * {@link TimeSeriesTypeBuilder#toSQL()}, and the one path where the difference would corrupt rather than
   * surprise refuses it: see {@code JsonlImporterFormat}.
   */
  @Test
  void theRenderedPathRegroupsWhatTheGrammarCannotSpell() {
    final String sql = database.getSchema().buildTimeSeriesType().withName("FieldFirst")
        .withField("cpu", Type.DOUBLE)
        .withTimestamp("ts")
        .withTag("host", Type.STRING)
        .toSQL().getFirst();

    assertThat(sql).isEqualTo("CREATE TIMESERIES TYPE `FieldFirst` TIMESTAMP `ts` TAGS (`host` STRING) "
        + "FIELDS (`cpu` DOUBLE)");
  }

  /** The embedded path still accepts it, and stores it as given: the engine supports the layout. */
  @Test
  void theEmbeddedPathKeepsTheOrderItWasGiven() {
    final TimeSeriesType type = database.getSchema().buildTimeSeriesType().withName("AsDeclared")
        .withField("cpu", Type.DOUBLE)
        .withTimestamp("ts")
        .withTag("host", Type.STRING)
        .create();

    assertThat(columnNames(type)).containsExactly("cpu", "ts", "host");
  }

  /** Within a role, declaration order is kept on both paths. */
  @Test
  void declarationOrderSurvivesWithinEachRole() {
    final TimeSeriesType type = database.getSchema().buildTimeSeriesType().withName("WithinRole")
        .withTimestamp("ts")
        .withTag("rack", Type.STRING)
        .withTag("host", Type.STRING)
        .withField("zeta", Type.DOUBLE)
        .withField("alpha", Type.DOUBLE)
        .create();

    assertThat(columnNames(type)).containsExactly("ts", "rack", "host", "zeta", "alpha");
  }

  // ---- 3. the single-TIMESTAMP rule ----

  /** {@code create()} refuses a second TIMESTAMP column, as rendering SQL always did. */
  @Test
  void aSecondTimestampColumnIsRefusedByCreateToo() {
    final TimeSeriesTypeBuilder twoTimestamps = database.getSchema().buildTimeSeriesType().withName("TwoTimestamps")
        .withColumn(new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP))
        .withColumn(new ColumnDefinition("ts2", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP))
        .withField("value", Type.DOUBLE);

    assertThatThrownBy(twoTimestamps::create)
        .as("the rule is the type's, not the grammar's")
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("exactly one TIMESTAMP column");

    assertThatThrownBy(twoTimestamps::toSQL)
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("exactly one TIMESTAMP column");

    assertThat(database.getSchema().existsType("TwoTimestamps"))
        .as("and nothing half-built is left behind")
        .isFalse();
  }

  /** One TIMESTAMP column is still perfectly ordinary. */
  @Test
  void oneTimestampColumnIsAccepted() {
    assertThatNoException().isThrownBy(() -> database.getSchema().buildTimeSeriesType().withName("OneTimestamp")
        .withColumn(new ColumnDefinition("ts", Type.LONG, ColumnDefinition.ColumnRole.TIMESTAMP))
        .withField("value", Type.DOUBLE)
        .create());
  }
}
