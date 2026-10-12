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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.AlterTimeSeriesTypeStatement;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.TimeSeriesType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9800: a TIMESERIES type's declared precision was accepted and then ignored by every time computation, all of
 * which count in milliseconds - for {@code PRECISION SECOND} the first retention pass deleted every sample. Only
 * MILLISECOND can be declared now; a type declared otherwise before the fix keeps loading, with retention and
 * downsampling suspended until {@code ALTER TIMESERIES TYPE ... PRECISION MILLISECOND} confirms the unit.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9800TimeSeriesPrecisionTest extends TestHelper {
  private static final long DAY_MS = 86_400_000L;

  @Test
  void createRefusesEveryPrecisionButMillisecond() {
    for (final String unit : new String[] { "SECOND", "MICROSECOND", "NANOSECOND" })
      assertThatThrownBy(() -> database.command("sql",
          "CREATE TIMESERIES TYPE p_" + unit + " TIMESTAMP ts PRECISION " + unit + " FIELDS (v DOUBLE)")).as(unit)
          .isInstanceOf(SchemaException.class).hasMessageContaining(unit).hasMessageContaining("MILLISECOND");

    database.command("sql", "CREATE TIMESERIES TYPE p_ms TIMESTAMP ts PRECISION MILLISECOND FIELDS (v DOUBLE)");
    final TimeSeriesType ms = (TimeSeriesType) database.getSchema().getType("p_ms");
    assertThat(ms.getPrecision()).isEqualTo("MILLISECOND");
    assertThat(ms.isPrecisionHonoured()).isTrue();

    database.command("sql", "CREATE TIMESERIES TYPE p_default TIMESTAMP ts FIELDS (v DOUBLE)");
    assertThat(((TimeSeriesType) database.getSchema().getType("p_default")).isPrecisionHonoured()).isTrue();
  }

  @Test
  void theBuilderRefusesItEmbeddedAndAsSQL() {
    assertThatThrownBy(() -> database.getSchema().buildTimeSeriesType().withName("b").withTimestamp("ts")
        .withPrecision("second").withField("v", Type.DOUBLE).create()).isInstanceOf(SchemaException.class)
        .hasMessageContaining("SECOND");
    assertThatThrownBy(() -> database.getSchema().buildTimeSeriesType().withName("b").withTimestamp("ts")
        .withPrecision("NANOSECOND").withField("v", Type.DOUBLE).toSQL()).isInstanceOf(SchemaException.class);

    // A restore keeps a legacy declaration embedded, and cannot render it as DDL
    assertThatThrownBy(() -> database.getSchema().buildTimeSeriesType().withName("b").withTimestamp("ts")
        .withLegacyPrecision("NANOSECOND").withField("v", Type.DOUBLE).toSQL()).isInstanceOf(SchemaException.class)
        .hasMessageContaining("embedded");
    assertThat(database.getSchema().existsType("b")).isFalse();
  }

  @Test
  void aLegacySecondTypeIsNotWipedByTheRetentionPass() {
    final LocalTimeSeriesType type = legacyType("sec_metric", "SECOND");
    final long nowSec = System.currentTimeMillis() / 1000;
    for (int i = 0; i < 5; i++)
      insert("sec_metric", nowSec - i);

    TimeSeriesMaintenanceScheduler.runMaintenance(database, type, "sec_metric");

    // Before the fix, the millisecond cutoff (~1.8e12) was larger than every SECOND timestamp (~1.8e9): 0 rows
    assertThat(count("sec_metric")).isEqualTo(5L);
  }

  @Test
  void alterPrecisionMillisecondResumesTheSuspendedRetention() throws Exception {
    // Declared NANOSECOND, but fed milliseconds - what every ingestion API (line protocol, Prometheus, gRPC) wrote
    final LocalTimeSeriesType type = legacyType("ns_metric", "NANOSECOND");
    final long nowMs = System.currentTimeMillis();
    insert("ns_metric", nowMs - 3 * DAY_MS);
    // Retention drops whole sealed blocks, so the old sample is sealed in a block of its own
    type.getEngine().compactAll();
    for (int i = 0; i < 4; i++)
      insert("ns_metric", nowMs - i * 1000L);

    TimeSeriesMaintenanceScheduler.runMaintenance(database, type, "ns_metric");
    assertThat(count("ns_metric")).as("retention suspended").isEqualTo(5L);
    assertThat(maintenanceSuspended("ns_metric")).isTrue();

    try (final ResultSet rs = database.command("sql", "ALTER TIMESERIES TYPE ns_metric PRECISION MILLISECOND")) {
      final Result r = rs.next();
      assertThat(r.<String>getProperty("previousPrecision")).isEqualTo("NANOSECOND");
      assertThat(r.<String>getProperty("precision")).isEqualTo("MILLISECOND");
    }
    assertThat(type.isPrecisionHonoured()).isTrue();
    assertThat(maintenanceSuspended("ns_metric")).isFalse();

    TimeSeriesMaintenanceScheduler.runMaintenance(database, type, "ns_metric");
    assertThat(count("ns_metric")).as("the 3-day-old sample is past the 1-day retention").isEqualTo(4L);

    // Persisted with the schema
    reopenDatabase();
    assertThat(((TimeSeriesType) database.getSchema().getType("ns_metric")).getPrecision()).isEqualTo("MILLISECOND");
  }

  @Test
  void alterPrecisionAcceptsOnlyMillisecond() {
    database.command("sql", "CREATE TIMESERIES TYPE alt TIMESTAMP ts FIELDS (v DOUBLE)");
    assertThatThrownBy(() -> database.command("sql", "ALTER TIMESERIES TYPE alt PRECISION SECOND"))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("MILLISECOND");

    // Idempotent on a type already in milliseconds
    try (final ResultSet rs = database.command("sql", "ALTER TIMESERIES TYPE alt PRECISION MILLISECOND")) {
      assertThat(rs.next().<String>getProperty("previousPrecision")).isNull();
    }
    assertThat(((TimeSeriesType) database.getSchema().getType("alt")).getPrecision()).isEqualTo("MILLISECOND");
  }

  @Test
  void alterPrecisionRendersAndReparses() {
    final SQLAntlrParser parser = new SQLAntlrParser(null);
    final AlterTimeSeriesTypeStatement stmt = (AlterTimeSeriesTypeStatement) parser.parse(
        "ALTER TIMESERIES TYPE alt PRECISION millisecond");
    assertThat(stmt.precision).isEqualTo("MILLISECOND");
    assertThat(stmt.toString()).isEqualTo("ALTER TIMESERIES TYPE alt PRECISION MILLISECOND");
    assertThat(parser.parse(stmt.toString())).isEqualTo(stmt);
    assertThat(stmt.copy()).isEqualTo(stmt);
  }

  private LocalTimeSeriesType legacyType(final String name, final String precision) {
    // The way a type declared before #9800 reaches this build: a schema that already carries the precision
    return (LocalTimeSeriesType) database.getSchema().buildTimeSeriesType().withName(name).withTimestamp("ts")
        .withLegacyPrecision(precision).withTag("host", Type.STRING).withField("v", Type.DOUBLE).withShards(1)
        .withRetention(DAY_MS).create();
  }

  private void insert(final String type, final long ts) {
    database.transaction(() -> database.command("sql", "INSERT INTO " + type + " SET ts = ?, host = 'h1', v = 1.0", ts));
  }

  private boolean maintenanceSuspended(final String type) {
    try (final ResultSet rs = database.query("sql", "SELECT maintenanceSuspended FROM schema:types WHERE name = ?", type)) {
      return Boolean.TRUE.equals(rs.next().getProperty("maintenanceSuspended"));
    }
  }

  private long count(final String type) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + type)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }
}
