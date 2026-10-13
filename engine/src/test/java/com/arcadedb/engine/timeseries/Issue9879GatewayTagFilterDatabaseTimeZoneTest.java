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
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9879: {@link TimeSeriesGateway#andTag} read a DATE/DATETIME tag value given as text with no database, in the JVM's
 * default time zone, while the {@code INSERT} that wrote the tag read the same text in the database's. On a JVM whose zone is
 * not the database's, the filter named another epoch than the stored one and selected nothing. Every protocol's tag selection
 * converges on {@code andTag}: the gRPC {@code TimeSeries*} RPCs through {@link TimeSeriesGateway#buildTagFilter}, the HTTP
 * {@code tags} object and {@code tag=name:value} parameter directly.
 * <p>
 * The database captures its zone when it opens. The JVM default is moved afterwards, to a zone with an offset on the stored
 * date, so the two differ wherever the test runs - CI runs in UTC, where they would otherwise coincide.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue9879GatewayTagFilterDatabaseTimeZoneTest extends TestHelper {
  private TimeZone jvmZone;

  @BeforeEach
  void moveTheJvmZoneAwayFromTheDatabases() {
    jvmZone = TimeZone.getDefault();
    final TimeZone databaseZone = database.getSchema().getTimeZone();
    TimeZone.setDefault(TimeZone.getTimeZone(databaseZone.getRawOffset() == 9 * 3_600_000 ? "America/New_York" : "Asia/Tokyo"));
  }

  @AfterEach
  void restoreTheJvmZone() {
    TimeZone.setDefault(jvmZone);
  }

  @Test
  void aDateTimeTagGivenAsTextSelectsTheRowTheInsertWroteWhateverTheJvmZone() throws Exception {
    final String type = createType("DATETIME", "'2024-01-01 00:00:00'");
    assertSelectedOnBothLayers(type, "2024-01-01 00:00:00");
  }

  @Test
  void aDateTagGivenAsTextSelectsTheRowTheInsertWroteWhateverTheJvmZone() throws Exception {
    final String type = createType("DATE", "'2024-01-01'");
    assertSelectedOnBothLayers(type, "2024-01-01");
  }

  @Test
  void theDateTimeTextReadWithoutTheDatabaseNamesAnotherEpoch() throws Exception {
    // The precondition the regression rests on: read with the JVM defaults, the same text names another instant here, which
    // is exactly what the gateway used to do. Without it the tests above could not tell the fix from the bug.
    final String type = createType("DATETIME", "'2024-01-01 00:00:00'");
    final ColumnDefinition code = columnsOf(type).get(1);
    assertThat(code.coerceValue(null, "2024-01-01 00:00:00")).isNotEqualTo(code.coerceValue(database, "2024-01-01 00:00:00"));
    assertThat(rows(type, TimeSeriesGateway.andTag(null, null, "code", "2024-01-01 00:00:00", columnsOf(type)))).isZero();
  }

  @Test
  void aDifferentDateTimeStillSelectsNothing() throws Exception {
    final String type = createType("DATETIME", "'2024-01-01 00:00:00'");
    assertThat(rows(type, TimeSeriesGateway.andTag(database, null, "code", "2025-06-01 00:00:00", columnsOf(type)))).isZero();
  }

  private void assertSelectedOnBothLayers(final String type, final String value) throws Exception {
    for (final boolean sealed : new boolean[] { false, true }) {
      if (sealed)
        engineOf(type).compactAll();
      final String layer = sealed ? "sealed store" : "mutable bucket";
      final List<ColumnDefinition> columns = columnsOf(type);

      assertThat(rows(type, TimeSeriesGateway.andTag(database, null, "code", value, columns)))
          .as("%s: andTag, the HTTP 'tags' object and 'tag=name:value' parameter", layer).isEqualTo(1);
      assertThat(rows(type, TimeSeriesGateway.buildTagFilter(database, Map.of("code", value), columns)))
          .as("%s: buildTagFilter, the gRPC TimeSeries* RPCs", layer).isEqualTo(1);
      assertThat(TimeSeriesGateway.latest(engineOf(type), TimeSeriesGateway.andTag(database, null, "code", value, columns)))
          .as("%s: latest", layer).isNotNull();
    }
  }

  private String createType(final String declaredType, final String stored) {
    final String type = "ts9879_" + declaredType.toLowerCase();
    database.command("sql", "CREATE TIMESERIES TYPE " + type + " TIMESTAMP ts TAGS (code " + declaredType + ") FIELDS (value DOUBLE)");
    database.transaction(() -> database.command("sql", "INSERT INTO " + type + " SET ts = 1000, code = " + stored + ", value = 1.0"));
    return type;
  }

  private int rows(final String type, final TagFilter filter) throws Exception {
    return engineOf(type).query(Long.MIN_VALUE, Long.MAX_VALUE, null, filter).size();
  }

  private List<ColumnDefinition> columnsOf(final String type) {
    return ((LocalTimeSeriesType) database.getSchema().getType(type)).getTsColumns();
  }

  private TimeSeriesEngine engineOf(final String type) {
    return ((LocalTimeSeriesType) database.getSchema().getType(type)).getEngine();
  }
}
