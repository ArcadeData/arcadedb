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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.LineProtocolParser.Sample;
import com.arcadedb.engine.timeseries.TimeSeriesGateway.WriteReport;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8646: {@link TimeSeriesGateway#write} built each row by looking up the type's DECLARED columns in the
 * sample, so a tag or field key the type does not declare was never read and never reported. A misspelled tag key
 * stored the point with the declared tag null - under a different series than the one sent - and the write was
 * answered as complete. The gateway is shared by the HTTP line-protocol route and both gRPC write RPCs, so this
 * test pins the rule at the one place they converge; {@code PostTimeSeriesWriteHandlerIT} and
 * {@code Issue7305TimeSeriesGrpcIT} pin how each protocol renders it.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/8646">issue #8646</a>
 */
class Issue8646TimeSeriesUndeclaredKeysTest extends TestHelper {

  private static final long T0 = 1_700_000_000_000L;

  @BeforeEach
  void createType() {
    database.command("sql", "CREATE TIMESERIES TYPE weather TIMESTAMP ts TAGS (city STRING) FIELDS (temp DOUBLE)");
  }

  private WriteReport write(final Sample... samples) throws Exception {
    return TimeSeriesGateway.write((DatabaseInternal) database, new ArrayList<>(List.of(samples)));
  }

  private long rows() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM weather")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  @Test
  void misspelledTagKeyIsDroppedAndNamed() throws Exception {
    final WriteReport report = write(new Sample("weather", Map.of("citty", "rome"), Map.of("temp", 21.5), T0));

    assertThat(report.isComplete()).isFalse();
    assertThat(report.written()).isZero();
    assertThat(report.dropped()).isEqualTo(1);
    assertThat(report.undeclaredKeys()).containsExactly("weather.citty (tag)");
    assertThat(rows()).as("no row under a null city").isZero();
  }

  @Test
  void undeclaredFieldKeyIsDroppedAndNamed() throws Exception {
    final WriteReport report = write(new Sample("weather", Map.of("city", "rome"), Map.of("tmp", 21.5), T0));

    assertThat(report.dropped()).isEqualTo(1);
    assertThat(report.undeclaredKeys()).containsExactly("weather.tmp (field)");
    assertThat(rows()).isZero();
  }

  @Test
  void keyDeclaredInTheOtherRoleIsUndeclared() throws Exception {
    // 'temp' is a FIELD column: sent as a tag, the column loop never reads it, so it would be lost as silently
    final WriteReport report = write(new Sample("weather", Map.of("city", "rome", "temp", "21"), Map.of(), T0),
        new Sample("weather", Map.of(), Map.of("city", "rome", "temp", 21.5), T0 + 1));

    assertThat(report.dropped()).isEqualTo(2);
    assertThat(report.undeclaredKeys()).containsExactly("weather.temp (tag)", "weather.city (field)");
    assertThat(rows()).isZero();
  }

  @Test
  void onlyTheOffendingSamplesAreDropped() throws Exception {
    final WriteReport report = write(new Sample("weather", Map.of("city", "rome"), Map.of("temp", 21.5), T0),
        new Sample("weather", Map.of("citty", "milan"), Map.of("temp", 18.0), T0 + 1),
        new Sample("weather", Map.of("city", "paris"), Map.of("temp", 15.0), T0 + 2));

    assertThat(report.written()).isEqualTo(2);
    assertThat(report.dropped()).isEqualTo(1);
    assertThat(report.written() + report.dropped()).isEqualTo(3);
    assertThat(rows()).isEqualTo(2);
    try (final ResultSet rs = database.query("sql", "SELECT FROM weather WHERE city IS NULL")) {
      assertThat(rs.hasNext()).as("the misspelled sample must not be stored with city = null").isFalse();
    }
  }

  @Test
  void aSampleOmittingADeclaredKeyIsStillAccepted() throws Exception {
    // Absence is not misspelling: a sample with no tags at all says "no value for this column"
    final WriteReport report = write(new Sample("weather", Map.of(), Map.of("temp", 21.5), T0));

    assertThat(report.isComplete()).isTrue();
    assertThat(report.undeclaredKeys()).isEmpty();
    assertThat(rows()).isEqualTo(1);
  }

  @Test
  void reportedKeysAreCappedButEveryDropIsCounted() throws Exception {
    final int count = TimeSeriesGateway.MAX_REPORTED_UNDECLARED_KEYS + 50;
    final Sample[] samples = new Sample[count];
    for (int i = 0; i < count; i++)
      samples[i] = new Sample("weather", Map.of("k" + i, "v"), Map.of("temp", 1.0), T0 + i);

    final WriteReport report = write(samples);

    assertThat(report.dropped()).isEqualTo(count);
    assertThat(report.undeclaredKeys()).hasSize(TimeSeriesGateway.MAX_REPORTED_UNDECLARED_KEYS);
  }

  @Test
  void ignorePolicyStoresTheSampleWithTheUndeclaredKeysDiscarded() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.TIMESERIES_UNDECLARED_KEYS, "ignore");

    // A Telegraf-style producer: the declared tag plus an extra 'host' the type never meant to hold
    final WriteReport report = write(
        new Sample("weather", Map.of("city", "rome", "host", "telegraf-1"), Map.of("temp", 21.5), T0));

    assertThat(report.isComplete()).isTrue();
    assertThat(report.undeclaredKeys()).isEmpty();
    try (final ResultSet rs = database.query("sql", "SELECT FROM weather")) {
      final Result row = rs.next();
      assertThat(row.<String>getProperty("city")).isEqualTo("rome");
      assertThat(row.<Double>getProperty("temp")).isEqualTo(21.5);
      assertThat(rs.hasNext()).isFalse();
    }
  }
}
