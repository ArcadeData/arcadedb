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
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.parser.Statement;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8726: {@code RemoteSchema.alterMaterializedView()} renders the call as {@code ALTER MATERIALIZED VIEW}, so the
 * refresh clause rendering moved out of the builder into a shared helper, and the embedded
 * {@code LocalSchema.alterMaterializedView()} refuses the same arguments the remote one cannot express (a null mode, a
 * negative interval, an interval on a mode other than PERIODIC) instead of storing them - so the two paths agree.
 */
class Issue8726AlterMaterializedViewRefreshTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Source");
    database.transaction(() -> database.newDocument("Source").set("value", 1).save());
    database.getSchema().buildMaterializedView().withName("View").withQuery("SELECT value FROM Source")
        .withRefreshMode(MaterializedViewRefreshMode.MANUAL).create();
  }

  @Test
  void refreshClauseRendersEveryExpressibleModeAndInterval() {
    assertThat(MaterializedViewBuilder.renderRefreshClause(MaterializedViewRefreshMode.MANUAL, 0)).isEqualTo("REFRESH MANUAL");
    assertThat(MaterializedViewBuilder.renderRefreshClause(MaterializedViewRefreshMode.INCREMENTAL, 0))
        .isEqualTo("REFRESH INCREMENTAL");
    assertThat(MaterializedViewBuilder.renderRefreshClause(MaterializedViewRefreshMode.PERIODIC, 2 * 3_600_000L))
        .isEqualTo("REFRESH EVERY 2 HOUR");
    assertThat(MaterializedViewBuilder.renderRefreshClause(MaterializedViewRefreshMode.PERIODIC, 90 * 60_000L))
        .isEqualTo("REFRESH EVERY 90 MINUTE");
    assertThat(MaterializedViewBuilder.renderRefreshClause(MaterializedViewRefreshMode.PERIODIC, 45_000L))
        .isEqualTo("REFRESH EVERY 45 SECOND");
    assertThat(MaterializedViewBuilder.renderRefreshClause(MaterializedViewRefreshMode.PERIODIC, 0))
        .isEqualTo("REFRESH EVERY 0 SECOND");
  }

  @Test
  void refreshClauseRefusesWhatTheGrammarCannotExpress() {
    assertThatThrownBy(() -> MaterializedViewBuilder.renderRefreshClause(MaterializedViewRefreshMode.PERIODIC, 1_500L))
        .isInstanceOf(SchemaException.class).hasMessageContaining("whole number of seconds");
    assertThatThrownBy(() -> MaterializedViewBuilder.renderRefreshClause(MaterializedViewRefreshMode.MANUAL, 5_000L))
        .isInstanceOf(SchemaException.class).hasMessageContaining("only REFRESH EVERY");
    assertThatThrownBy(() -> MaterializedViewBuilder.renderRefreshClause(null, 0))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("refresh mode is required");
    assertThatThrownBy(() -> MaterializedViewBuilder.renderRefreshClause(MaterializedViewRefreshMode.PERIODIC, -1_000L))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("cannot be negative");
  }

  @Test
  void embeddedAlterRefusesAnIntervalOnANonPeriodicMode() {
    final Schema schema = database.getSchema();
    for (final MaterializedViewRefreshMode mode : new MaterializedViewRefreshMode[] { MaterializedViewRefreshMode.MANUAL,
        MaterializedViewRefreshMode.INCREMENTAL })
      assertThatThrownBy(() -> schema.alterMaterializedView("View", mode, 5_000L))
          .isInstanceOf(SchemaException.class).hasMessageContaining("only REFRESH EVERY");

    assertThat(schema.getMaterializedView("View").getRefreshMode()).isEqualTo(MaterializedViewRefreshMode.MANUAL);
    assertThat(schema.getMaterializedView("View").getRefreshInterval()).isZero();
  }

  @Test
  void embeddedAlterRefusesANullModeAndANegativeInterval() {
    final Schema schema = database.getSchema();
    assertThatThrownBy(() -> schema.alterMaterializedView("View", null, 0))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("refresh mode is required");
    assertThatThrownBy(() -> schema.alterMaterializedView("View", MaterializedViewRefreshMode.PERIODIC, -1L))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("cannot be negative");

    assertThat(schema.getMaterializedView("View").getRefreshMode()).isEqualTo(MaterializedViewRefreshMode.MANUAL);
  }

  @Test
  void embeddedAlterStillAcceptsAPeriodicIntervalBelowOneSecondGranularity() {
    // The embedded API is not bound by the grammar's SECOND granularity: only the remote rendering refuses 1500ms
    database.getSchema().alterMaterializedView("View", MaterializedViewRefreshMode.PERIODIC, 1_500L);
    assertThat(database.getSchema().getMaterializedView("View").getRefreshInterval()).isEqualTo(1_500L);
    database.getSchema().alterMaterializedView("View", MaterializedViewRefreshMode.MANUAL, 0);
  }

  /**
   * {@code EVERY 0 SECOND} parses to PERIODIC with a zero interval, which used to re-render as {@code REFRESH PERIODIC},
   * a statement the grammar does not accept.
   */
  @Test
  void everyZeroSecondReRendersAsParseableSql() {
    final Statement stmt = new SQLAntlrParser(null).parse("ALTER MATERIALIZED VIEW V1 REFRESH EVERY 0 SECOND");
    final String rendered = stmt.toString();
    assertThat(rendered).contains("REFRESH EVERY 0 SECOND");
    assertThat(new SQLAntlrParser(null).parse(rendered)).isEqualTo(stmt);
  }
}
