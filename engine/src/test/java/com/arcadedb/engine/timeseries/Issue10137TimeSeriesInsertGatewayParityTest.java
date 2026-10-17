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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10137: {@code INSERT INTO} a TIMESERIES type called {@link ColumnDefinition#storableValue} (issue #10064), but only after
 * {@code MutableDocument} had already converted each value to the column's declared type, so a number written to a BOOLEAN column
 * was stored as {@code true} and a number written to a STRING column as its text, where the line protocol, gRPC and Prometheus
 * remote write refuse the same point. A non-temporal column's value now reaches {@code storableValue} as the statement wrote it.
 * <p>
 * The guarantee is the parity table: for each (column type, value) pair the SQL statement and the gateway every other surface
 * goes through reach the SAME outcome, both storing the same value or both refusing, rather than each surface being asserted on
 * its own, which is how a pair where they differ went unwritten.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue10137TimeSeriesInsertGatewayParityTest extends TestHelper {

  private static final long SQL_TS     = 1_000L;
  private static final long GATEWAY_TS = 2_000L;

  private record Pair(String columnType, Object value) {
  }

  /** The outcome of one write: the stored value, or {@link #REFUSED}. */
  private static final Object REFUSED = new Object() {
    @Override
    public String toString() {
      return "REFUSED";
    }
  };

  @Test
  void theSqlInsertAndTheGatewayReachTheSameOutcomeForEveryPair() throws Exception {
    final List<Pair> pairs = List.of(//
        // the disagreements the issue reported
        new Pair("BOOLEAN", 1), new Pair("BOOLEAN", 0), new Pair("BOOLEAN", 7), new Pair("BOOLEAN", 7L), new Pair("BOOLEAN", 4.25d),
        new Pair("STRING", 42), new Pair("STRING", 42L), new Pair("STRING", 4.25d), new Pair("STRING", true),
        new Pair("BYTE", 300),
        // pairs the two already agreed on, kept so a fix for the above cannot move them
        new Pair("BOOLEAN", true), new Pair("BOOLEAN", false), new Pair("BOOLEAN", "TRUE"), new Pair("BOOLEAN", "false"),
        new Pair("BOOLEAN", "maybe"),
        new Pair("STRING", "x"),
        new Pair("INTEGER", true), new Pair("INTEGER", 5), new Pair("INTEGER", 5L), new Pair("INTEGER", "5"),
        new Pair("INTEGER", 1e12d), new Pair("INTEGER", 3.7d), new Pair("INTEGER", "abc"),
        new Pair("LONG", 7), new Pair("LONG", "123"), new Pair("LONG", new BigDecimal("9223372036854775808")),
        new Pair("SHORT", 70000), new Pair("SHORT", -32768),
        new Pair("BYTE", 127), new Pair("BYTE", "12"),
        new Pair("DOUBLE", true), new Pair("DOUBLE", "2.5"), new Pair("DOUBLE", 2.5d), new Pair("DOUBLE", 0.05f), new Pair("DOUBLE", 3),
        new Pair("FLOAT", 7.5d), new Pair("FLOAT", "1.5"),
        // a temporal column keeps the document conversion (a number is epoch MILLIS, issues #9643 and #9724); on a millisecond
        // column that is the gateway's count too
        new Pair("DATETIME", 0L), new Pair("DATETIME", "2023-11-14 22:13:20"));

    final List<String> disagreements = new ArrayList<>();
    for (int i = 0; i < pairs.size(); i++) {
      final Pair pair = pairs.get(i);
      final String typeName = "P" + i;
      database.command("sql", "CREATE TIMESERIES TYPE " + typeName + " TIMESTAMP ts FIELDS (c " + pair.columnType() + ") SHARDS 1");

      final Object viaSql = viaSql(typeName, pair.value());
      final Object viaGateway = viaGateway(typeName, pair.value());
      if (!sameOutcome(viaSql, viaGateway))
        disagreements.add(pair.value() + " (" + pair.value().getClass().getSimpleName() + ") -> " + pair.columnType() + ": SQL="
            + viaSql + " GATEWAY=" + viaGateway);
    }

    assertThat(disagreements).as("pairs where SQL INSERT and the gateway disagree").isEmpty();
  }

  /** The issue's literal repro: the statement refuses the point the line protocol refuses, naming the column. */
  @Test
  void aNumberIntoABooleanOrAStringColumnIsRefusedByTheStatement() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE M TIMESTAMP ts FIELDS (i INTEGER, d DOUBLE, s STRING, bo BOOLEAN) SHARDS 1");

    assertThatThrownBy(() -> database.command("sql", "INSERT INTO M SET ts = 7000, i = 1, d = 1, s = 'x', bo = 7"))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("'bo'").hasMessageContaining("BOOLEAN");
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO M SET ts = 7001, i = 1, d = 1, s = 'x', bo = 1"))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("'bo'");
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO M SET ts = 7002, i = 1, d = 1, s = 42, bo = true"))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("'s'").hasMessageContaining("STRING");
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO M SET ts = 7003, i = 1, d = 1, s = 4.25, bo = true"))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("'s'");
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO M SET ts = 7004, i = 1, d = 1, s = true, bo = true"))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("'s'");
    assertThat(rows("M")).as("no refused statement stored a sample").isEmpty();

    database.command("sql", "INSERT INTO M SET ts = 7005, i = 1, d = 1, s = '42', bo = 'TRUE'");
    assertThat(rows("M")).singleElement().satisfies(row -> {
      assertThat(row[3]).isEqualTo("42");
      assertThat(row[4]).isEqualTo(Boolean.TRUE);
    });
  }

  /** The same refusal through {@code CONTENT}, which reaches the document through its map setter rather than SET. */
  @Test
  void aNumberIntoABooleanColumnIsRefusedThroughContent() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE C TIMESTAMP ts FIELDS (bo BOOLEAN, s STRING) SHARDS 1");

    assertThatThrownBy(() -> database.command("sql", "INSERT INTO C CONTENT {\"ts\": 7000, \"bo\": 1, \"s\": \"x\"}"))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("'bo'");
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO C CONTENT {\"ts\": 7001, \"bo\": true, \"s\": 42}"))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("'s'");
    assertThat(rows("C")).isEmpty();
  }

  /** The same refusal through {@code (columns) VALUES (...)}. */
  @Test
  void aNumberIntoABooleanColumnIsRefusedThroughValues() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE V TIMESTAMP ts FIELDS (bo BOOLEAN, s STRING) SHARDS 1");

    assertThatThrownBy(() -> database.command("sql", "INSERT INTO V (ts, bo, s) VALUES (7000, 7, 'x')"))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("'bo'");
    assertThat(rows("V")).isEmpty();
  }

  /**
   * Secondary finding of the issue: a number the column cannot hold used to be refused by the document layer first, with a bare
   * {@code IllegalArgumentException} ("Cannot convert type 'class java.lang.Integer' to 'BYTE'"), rather than with the reason
   * {@code storableValue} gives every other surface.
   */
  @Test
  void anOutOfRangeNumberIsRefusedWithTheReasonEverySurfaceGives() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE R TIMESTAMP ts FIELDS (b BYTE) SHARDS 1");

    assertThatThrownBy(() -> database.command("sql", "INSERT INTO R SET ts = 7000, b = 300"))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("'b'").hasMessageContaining("out of range");
    assertThat(rows("R")).isEmpty();
  }

  private Object viaSql(final String typeName, final Object value) throws Exception {
    try {
      database.command("sql", "INSERT INTO " + typeName + " SET ts = ?, c = ?", SQL_TS, value);
    } catch (final CommandExecutionException e) {
      // only the statement's refusal counts as one: any other exception (a typo in the statement, a bare conversion error from the
      // document layer) fails the test instead of matching a gateway refusal by accident
      return REFUSED;
    }
    return storedAt(typeName, SQL_TS);
  }

  private Object viaGateway(final String typeName, final Object value) throws Exception {
    final WriteReport report = TimeSeriesGateway.write((DatabaseInternal) database,
        new ArrayList<>(List.of(new Sample(typeName, Map.of(), Map.of("c", value), GATEWAY_TS))));
    if (report.written() == 0)
      return REFUSED;
    return storedAt(typeName, GATEWAY_TS);
  }

  private Object storedAt(final String typeName, final long ts) throws Exception {
    for (final Object[] row : rows(typeName))
      if (((Number) row[0]).longValue() == ts)
        return row[1];
    throw new AssertionError("no sample at " + ts + " in " + typeName);
  }

  private static boolean sameOutcome(final Object a, final Object b) {
    if (a == REFUSED || b == REFUSED)
      return a == b;
    if (a instanceof Number x && b instanceof Number y)
      return Double.compare(x.doubleValue(), y.doubleValue()) == 0;
    return a == null ? b == null : a.equals(b);
  }

  private List<Object[]> rows(final String typeName) throws Exception {
    return ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine().query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
  }
}
