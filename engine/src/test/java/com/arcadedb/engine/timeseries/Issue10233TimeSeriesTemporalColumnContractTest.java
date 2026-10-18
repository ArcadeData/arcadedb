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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.LineProtocolParser.Sample;
import com.arcadedb.engine.timeseries.TimeSeriesGateway.WriteReport;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10233: SQL and the line protocol / gRPC / Prometheus read a number given to a DATE/DATETIME column of a TIMESERIES type
 * differently. That split is the intended contract of each surface and stays: SQL reads the column like a document property of the
 * same type (a number is epoch MILLIS whatever the precision, epoch DAYS for a DATE, issues #9643 and #9724), the wire protocols
 * read a number, and text of digits, as a count in the column's own unit. What was not intended is how SQL refused a value: by a
 * bare {@code IllegalArgumentException} out of the document layer, where every other refusal of an INSERT into a TIMESERIES type is
 * a {@link CommandExecutionException} naming the column. The temporal conversion now runs in the time series step, once.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10233TimeSeriesTemporalColumnContractTest extends TestHelper {
  private static final long MILLIS = 1_700_000_000_000L;

  @Test
  void textOfDigitsIsRefusedByCommandExecutionExceptionNamingTheColumn() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE D TIMESTAMP ts TAGS (k DATETIME) FIELDS (d DATETIME, dd DATE) SHARDS 1");

    for (final String column : new String[] { "k", "d", "dd" })
      assertThatThrownBy(() -> database.command("sql", "INSERT INTO D SET ts = 1, " + column + " = '" + MILLIS + "'"))
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("column '" + column + "'");
    // a document property of the same type refuses it too: SQL treats the column as its document twin
    database.command("sql", "CREATE DOCUMENT TYPE Twin");
    database.command("sql", "CREATE PROPERTY Twin.d DATETIME");
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO Twin SET d = '" + MILLIS + "'")))
        .isInstanceOf(IllegalArgumentException.class);

    assertThat(rows("D")).isEmpty();
  }

  @Test
  void aValueNoDateCanStandForIsRefusedNamingTheColumn() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE B TIMESTAMP ts FIELDS (d DATETIME, dd DATE) SHARDS 1");

    assertThatThrownBy(() -> database.command("sql", "INSERT INTO B SET ts = 1, d = 'not a date'"))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("column 'd'");
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO B SET ts = 1, d = true"))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("column 'd'");
    // a number of days no date can hold, as on a DATE document property
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO B SET ts = 1, dd = " + MILLIS))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("column 'dd'");
    assertThat(rows("B")).isEmpty();
  }

  /**
   * The documented split, pinned on both sides: the same number is epoch millis through SQL and a count in the column's unit through
   * the gateway, and SQL stores what the document twin stores.
   */
  @Test
  void sqlReadsANumberLikeTheDocumentTwinAndTheGatewayAsACountInTheColumnUnit() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE S TIMESTAMP ts FIELDS (d DATETIME, ds DATETIME_SECOND, dm DATETIME_MICROS, dn DATETIME_NANOS, dd DATE) SHARDS 1");
    database.command("sql", "INSERT INTO S SET ts = 1, d = " + MILLIS + ", ds = " + MILLIS + ", dm = " + MILLIS + ", dn = " + MILLIS
        + ", dd = 19675");

    final Object[] sql = rowAt("S", 1);
    assertThat(((Number) sql[1]).longValue()).isEqualTo(MILLIS);
    assertThat(((Number) sql[2]).longValue()).isEqualTo(MILLIS / 1_000L);
    assertThat(((Number) sql[3]).longValue()).isEqualTo(MILLIS * 1_000L);
    assertThat(((Number) sql[4]).longValue()).isEqualTo(MILLIS * 1_000_000L);
    assertThat(((Number) sql[5]).longValue()).isEqualTo(19675L);

    final WriteReport report = TimeSeriesGateway.write((DatabaseInternal) database, new ArrayList<>(List.of(
        new Sample("S", Map.of(), Map.of("d", MILLIS, "ds", MILLIS, "dm", MILLIS, "dn", "" + MILLIS, "dd", 19675L), 2))));
    assertThat(report.written()).isEqualTo(1);
    final Object[] gateway = rowAt("S", 2);
    for (int c = 1; c <= 4; c++)
      assertThat(((Number) gateway[c]).longValue()).as("column %d", c).isEqualTo(MILLIS);
    assertThat(((Number) gateway[5]).longValue()).isEqualTo(19675L);

    // the document twin of each column stores what SQL stored
    database.command("sql", "CREATE DOCUMENT TYPE Twin");
    database.command("sql", "CREATE PROPERTY Twin.dn DATETIME_NANOS");
    database.command("sql", "CREATE PROPERTY Twin.dd DATE");
    database.transaction(() -> database.command("sql", "INSERT INTO Twin SET dn = " + MILLIS + ", dd = 19675"));
    final var twin = database.query("sql", "SELECT FROM Twin").next();
    final var series = database.query("sql", "SELECT FROM S WHERE ts = 1").next();
    assertThat(series.<Object>getProperty("dn")).isEqualTo(twin.getProperty("dn"));
    assertThat(series.<Object>getProperty("dd")).isEqualTo(twin.getProperty("dd"));
  }

  /** A date given as text names the same instant on every surface. */
  @Test
  void aDateGivenAsTextIsTheSameInstantOnEverySurface() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE X TIMESTAMP ts FIELDS (dm DATETIME_MICROS) SHARDS 1");
    database.command("sql", "INSERT INTO X SET ts = 1, dm = '2023-11-14 22:13:20'");
    TimeSeriesGateway.write((DatabaseInternal) database,
        new ArrayList<>(List.of(new Sample("X", Map.of(), Map.of("dm", "2023-11-14 22:13:20"), 2))));
    assertThat(((Number) rowAt("X", 1)[1]).longValue()).isEqualTo(MILLIS * 1_000L).isEqualTo(((Number) rowAt("X", 2)[1]).longValue());
  }

  private Object[] rowAt(final String typeName, final long ts) throws Exception {
    for (final Object[] row : rows(typeName))
      if (((Number) row[0]).longValue() == ts)
        return row;
    throw new AssertionError("no sample at " + ts + " in " + typeName);
  }

  private List<Object[]> rows(final String typeName) throws Exception {
    return ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine().query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
  }
}
