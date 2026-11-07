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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * SQL INSERT and gateway ingest of time series samples: storable values, parity between surfaces, INSERT FROM SELECT and undeclared keys.
 */
class TimeSeriesInsertTest extends TestHelper {
  private static final List<ColumnDefinition> COLUMNS = List.of(//
      new ColumnDefinition("ts", Type.DATETIME, ColumnDefinition.ColumnRole.TIMESTAMP),//
      new ColumnDefinition("host", Type.INTEGER, ColumnDefinition.ColumnRole.TAG),//
      new ColumnDefinition("v", Type.INTEGER, ColumnDefinition.ColumnRole.FIELD),//
      new ColumnDefinition("w", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD));

  // Issue #10030: the conversion-and-refusal loop that turns the raw values of an ingestion batch into the grid
  // {@link TimeSeriesEngine#appendBatch(long[], Object[][])} takes lives in one engine-level helper,
  // {@link TimeSeriesStorableRows}, shared by {@link TimeSeriesGateway#write} (line protocol, gRPC) and the Prometheus remote-write
  // handler, so there is one implementation of the conversion contract instead of two.
  // #10030
  @Test
  void cleanRowsAreConvertedIntoTheGridWithoutDroppingAnything() {
    final Object[][] raw = { { "7", 1, 1.5 }, { "8", "2", 2.5 } };
    final Set<String> unreadable = new LinkedHashSet<>();

    final TimeSeriesStorableRows rows = TimeSeriesStorableRows.convert(database, "m", COLUMNS, null, new long[] { 10, 20 }, 2, false,
        (row, column, def) -> raw[row][column], unreadable);

    assertThat(rows.size()).isEqualTo(2);
    assertThat(rows.refused()).isZero();
    assertThat(rows.sharedValueRefused()).isFalse();
    assertThat(rows.timestamps()).containsExactly(10, 20);
    assertThat(rows.columnValues()).hasDimensions(3, 2);
    assertThat(rows.columnValues()[0]).as("the INTEGER tag sent as text is stored as a number").containsExactly(7, 8);
    assertThat(rows.columnValues()[1]).containsExactly(1, 2);
    assertThat(rows.columnValues()[2]).containsExactly(1.5, 2.5);
    assertThat(unreadable).isEmpty();
  }

  // #10030
  @Test
  void aValueAColumnCannotHoldDropsOnlyItsRowAndIsNamed() {
    final Object[][] raw = { { "7", 1, 1.0 }, { "8", 1e12, 2.0 }, { "9", 3, 3.0 } };
    final Set<String> unreadable = new LinkedHashSet<>();

    final TimeSeriesStorableRows rows = TimeSeriesStorableRows.convert(database, "m", COLUMNS, null, new long[] { 10, 20, 30 }, 3,
        false, (row, column, def) -> raw[row][column], unreadable);

    assertThat(rows.size()).isEqualTo(2);
    assertThat(rows.refused()).isEqualTo(1);
    assertThat(rows.sharedValueRefused()).isFalse();
    assertThat(rows.timestamps()).as("the refused row's timestamp is compacted away").containsExactly(10, 30);
    assertThat(rows.columnValues()[0]).containsExactly(7, 9);
    assertThat(rows.columnValues()[1]).containsExactly(1, 3);
    assertThat(rows.columnValues()[2]).containsExactly(1.0, 3.0);
    assertThat(unreadable).singleElement().asString().startsWith("m.v (field): ");
  }

  // #10030
  @Test
  void aRowIsRefusedAtItsFirstUnreadableColumnAndTheRestOfItIsNotRead() {
    final AtomicInteger reads = new AtomicInteger();
    final Object[][] raw = { { "not-a-number", "also-not", 1.0 } };
    final Set<String> unreadable = new LinkedHashSet<>();

    final TimeSeriesStorableRows rows = TimeSeriesStorableRows.convert(database, "m", COLUMNS, null, new long[] { 10 }, 1, false,
        (row, column, def) -> {
          reads.incrementAndGet();
          return raw[row][column];
        }, unreadable);

    assertThat(rows.size()).isZero();
    assertThat(rows.refused()).isEqualTo(1);
    assertThat(reads.get()).as("the columns after the refused one are not read").isEqualTo(1);
    assertThat(unreadable).singleElement().asString().startsWith("m.host (tag): ");
  }

  // #10030
  @Test
  void aSharedTagIsReadAndConvertedOnceAndBroadcastDownItsColumn() {
    final AtomicInteger tagReads = new AtomicInteger();
    final Set<String> unreadable = new LinkedHashSet<>();

    final TimeSeriesStorableRows rows = TimeSeriesStorableRows.convert(database, "m", COLUMNS, null, new long[] { 10, 20, 30 }, 3, true,
        (row, column, def) -> {
          if (def.getRole() == ColumnDefinition.ColumnRole.TAG) {
            tagReads.incrementAndGet();
            return "42";
          }
          return row;
        }, unreadable);

    assertThat(tagReads.get()).as("a shared tag is read once per batch, not once per row").isEqualTo(1);
    assertThat(rows.size()).isEqualTo(3);
    assertThat(rows.columnValues()[0]).containsExactly(42, 42, 42);
    assertThat(rows.columnValues()[1]).containsExactly(0, 1, 2);
  }

  // #10030
  @Test
  void anUnreadableSharedTagRefusesEveryRowOfTheBatch() {
    final AtomicInteger fieldReads = new AtomicInteger();
    final Set<String> unreadable = new LinkedHashSet<>();

    final TimeSeriesStorableRows rows = TimeSeriesStorableRows.convert(database, "m", COLUMNS, null, new long[] { 10, 20 }, 2, true,
        (row, column, def) -> {
          if (def.getRole() == ColumnDefinition.ColumnRole.TAG)
            return "not-a-number";
          fieldReads.incrementAndGet();
          return 1;
        }, unreadable);

    assertThat(rows.size()).isZero();
    assertThat(rows.refused()).isEqualTo(2);
    assertThat(rows.sharedValueRefused()).isTrue();
    assertThat(fieldReads.get()).as("no row is read once the batch is refused").isZero();
    assertThat(unreadable).singleElement().asString().startsWith("m.host (tag): ");
  }

  // #10030
  @Test
  void theReportedReasonsAreCapped() {
    final int count = TimeSeriesGateway.MAX_REPORTED_UNREADABLE_VALUES + 50;
    final Set<String> unreadable = new LinkedHashSet<>();
    final List<ColumnDefinition> columns = List.of(//
        new ColumnDefinition("ts", Type.DATETIME, ColumnDefinition.ColumnRole.TIMESTAMP),//
        new ColumnDefinition("v", Type.INTEGER, ColumnDefinition.ColumnRole.FIELD));

    // A distinct reason per row: the column is the same, so the message must differ through the measurement name
    for (int i = 0; i < count; i++)
      TimeSeriesStorableRows.convert(database, "m" + i, columns, null, new long[] { i }, 1, false, (row, column, def) -> "x", unreadable);

    assertThat(unreadable).hasSize(TimeSeriesGateway.MAX_REPORTED_UNREADABLE_VALUES);
  }

  // #10030
  @Test
  void aNullValueIsStoredAsAbsentAndRefusesNothing() {
    final Set<String> unreadable = new LinkedHashSet<>();

    final TimeSeriesStorableRows rows = TimeSeriesStorableRows.convert(database, "m", COLUMNS, null, new long[] { 10 }, 1, false,
        (row, column, def) -> null, unreadable);

    assertThat(rows.size()).isEqualTo(1);
    assertThat(rows.columnValues()[0]).containsExactly((Object) null);
    assertThat(unreadable).isEmpty();
  }

  private static final long BASE_TS = 1_700_000_000_000L;

  private TimeSeriesEngine engineOf(final String typeName) {
    return ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();
  }

  private List<Object[]> rows(final String typeName) throws Exception {
    return engineOf(typeName).query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
  }

  // Issue #10064: {@code INSERT INTO} a TIMESERIES type used to convert each value with a private {@code convertValue} that applied a
  // different contract than {@link ColumnDefinition#storableValue}, the one the line protocol, gRPC and Prometheus remote write share
  // (issue #10030). The same value written to the same column was stored differently, or refused, depending on the surface.
  // #10064
  /**
   * A number the column cannot hold is refused and no sample is stored. Since issue #10137 the document layer no longer converts a
   * non-temporal column first, so {@code storableValue} is the one that refuses, as it does on every other surface.
   */
  @Test
  void anOutOfRangeNumberIsRefusedAndNothingIsStored() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Range TIMESTAMP ts FIELDS (i INTEGER, s SHORT, b BYTE) SHARDS 1");

    assertThatThrownBy(() -> database.command("sql", "INSERT INTO Range SET ts = " + BASE_TS + ", i = 1e12, s = 1, b = 1"))
        .isInstanceOf(RuntimeException.class).hasMessageContaining("'i'");
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO Range SET ts = " + (BASE_TS + 1) + ", i = 1, s = 70000.5, b = 1"))
        .isInstanceOf(RuntimeException.class).hasMessageContaining("'s'");
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO Range SET ts = " + (BASE_TS + 2) + ", i = 1, s = 1, b = 300.5"))
        .isInstanceOf(RuntimeException.class).hasMessageContaining("'b'");

    assertThat(rows("Range")).as("a refused statement stores no sample").isEmpty();

    database.command("sql", "INSERT INTO Range SET ts = " + (BASE_TS + 3) + ", i = 2147483647, s = -32768, b = 127");
    assertThat(rows("Range")).hasSize(1);
  }

  // #10064
  /**
   * A fractional number into an integral column keeps its integral part, as it always did: the contract is a RANGE check, not a
   * rounding rule, and the private conversion this replaced ({@code longValue()}, {@code intValue()}, {@code shortValue()}) agreed.
   */
  @Test
  void aFractionalNumberIntoAnIntegralColumnKeepsItsIntegralPart() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Fractional TIMESTAMP ts FIELDS (l LONG, i INTEGER, s SHORT) SHARDS 1");

    database.command("sql", "INSERT INTO Fractional SET ts = " + BASE_TS + ", l = 3.7, i = 3.7, s = -3.7");

    assertThat(rows("Fractional")).singleElement().satisfies(row -> {
      assertThat(((Number) row[1]).longValue()).isEqualTo(3L);
      assertThat(((Number) row[2]).intValue()).isEqualTo(3);
      assertThat(((Number) row[3]).shortValue()).isEqualTo((short) -3);
    });
  }

  // #10064
  /**
   * A batch {@code INSERT} with a value the column cannot hold is refused at that row, naming the column, and the value is not stored:
   * the refusal is the statement's, as it is for a single row. A time series append commits on its own, so a row before the refused
   * one may stay.
   */
  @Test
  void aBatchInsertRefusesTheRowWithAValueTheColumnCannotHold() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE BatchRows TIMESTAMP ts FIELDS (i INTEGER) SHARDS 1");

    assertThatThrownBy(() -> database.command("sql", "INSERT INTO BatchRows CONTENT [{\"ts\": " + BASE_TS + ", \"i\": 1}, {\"ts\": "
        + (BASE_TS + 1) + ", \"i\": 1e12}]")).isInstanceOf(RuntimeException.class).hasMessageContaining("'i'");

    final List<Object[]> rows = rows("BatchRows");
    assertThat(rows.size()).as("at most the row before the refused one").isLessThanOrEqualTo(1);
    for (final Object[] row : rows)
      assertThat(((Number) row[1]).longValue()).as("only the row that was fine is stored").isEqualTo(1L);
  }

  // #10064
  /** A FLOAT literal into a DOUBLE column reads as its decimal, not as the single precision rounding error (issue #7609). */
  @Test
  void aFloatIntoADoubleColumnWidensThroughItsDecimalForm() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Widen TIMESTAMP ts FIELDS (d DOUBLE) SHARDS 1");
    database.command("sql", "INSERT INTO Widen SET ts = " + BASE_TS + ", d = 0.05F");

    assertThat(rows("Widen")).singleElement().satisfies(row -> assertThat(row[1]).isEqualTo(0.05d));
  }

  // #10064
  /** The widening lives in {@code storableValue} itself, so the line protocol, gRPC and Prometheus store a Float the same way. */
  @Test
  void storableValueWidensAFloatForADoubleColumnOnlyThroughItsDecimalForm() {
    final ColumnDefinition doubleColumn = new ColumnDefinition("d", Type.DOUBLE, ColumnDefinition.ColumnRole.FIELD);
    final ColumnDefinition floatColumn = new ColumnDefinition("f", Type.FLOAT, ColumnDefinition.ColumnRole.FIELD);

    assertThat(doubleColumn.storableValue(database, 0.05f)).isEqualTo(0.05d);
    assertThat(doubleColumn.storableValue(database, Float.NaN)).isEqualTo(Double.NaN);
    assertThat(doubleColumn.storableValue(database, Float.POSITIVE_INFINITY)).isEqualTo(Double.POSITIVE_INFINITY);
    assertThat(doubleColumn.storableValue(database, 0.05d)).isEqualTo(0.05d);
    assertThat(doubleColumn.storableValue(database, 3)).isEqualTo(3);
    assertThat(floatColumn.storableValue(database, 0.05f)).as("a FLOAT column keeps the Float").isEqualTo(0.05f);
  }

  // #10064
  /** A BOOLEAN column keeps its contract: text that is not a boolean is refused, {@code 'TRUE'} in any case is the boolean. */
  @Test
  void aBooleanColumnKeepsItsContract() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Kinds TIMESTAMP ts TAGS (host STRING) FIELDS (ok BOOLEAN, v DOUBLE) SHARDS 1");

    assertThatThrownBy(() -> database.command("sql", "INSERT INTO Kinds SET ts = " + BASE_TS + ", host = 'a', ok = 'maybe', v = 1.0"))
        .isInstanceOf(RuntimeException.class).hasMessageContaining("'ok'");
    assertThat(rows("Kinds")).isEmpty();

    database.command("sql", "INSERT INTO Kinds SET ts = " + BASE_TS + ", host = 'a', ok = 'TRUE', v = 1.0");
    database.command("sql", "INSERT INTO Kinds SET ts = " + (BASE_TS + 1) + ", host = 'b', ok = false, v = 2.0");

    final List<Object[]> rows = rows("Kinds");
    assertThat(rows).hasSize(2);
    assertThat(rows.get(0)[1]).isEqualTo("a");
    assertThat(rows.get(0)[2]).isEqualTo(Boolean.TRUE);
    assertThat(rows.get(1)[2]).isEqualTo(Boolean.FALSE);
  }

  // #10064
  /** A text longer than the row can hold is refused by the statement, as the other surfaces refuse it (issue #10030). */
  @Test
  void aTextBeyondTheRowLimitIsRefusedByTheStatement() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Long TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1");
    final String huge = "x".repeat(TimeSeriesBucket.MAX_STRING_BYTES + 1);

    assertThatThrownBy(() -> database.command("sql", "INSERT INTO Long SET ts = " + BASE_TS + ", host = '" + huge + "', v = 1.0"))
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("'host'");
    assertThat(rows("Long")).isEmpty();
  }

  // #10064
  /** Whatever {@code storableValue} accepts, INSERT stores exactly that: the two cannot drift again. */
  @Test
  void insertStoresWhatStorableValueReturnsForEveryNumericColumn() throws Exception {
    database.command("sql",
        "CREATE TIMESERIES TYPE Parity TIMESTAMP ts FIELDS (l LONG, i INTEGER, s SHORT, b BYTE, f FLOAT, d DOUBLE) SHARDS 1");
    database.command("sql", "INSERT INTO Parity SET ts = " + BASE_TS + ", l = 7, i = 7, s = 7, b = 7, f = 7.5, d = 7.5");

    final LocalTimeSeriesType type = (LocalTimeSeriesType) database.getSchema().getType("Parity");
    final Object[] row = rows("Parity").getFirst();
    int idx = 1;
    for (final ColumnDefinition col : type.getTsColumns()) {
      if (col.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
        continue;
      final Object sent = switch (col.getDataType()) {
        case FLOAT, DOUBLE -> 7.5d;
        default -> 7L;
      };
      final Object expected = col.storableValue(database, sent);
      assertThat(((Number) row[idx]).doubleValue()).as("column %s", col.getName()).isEqualTo(((Number) expected).doubleValue());
      idx++;
    }
  }

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
  @Nested
  class TimeSeriesInsertGatewayParity {
    private static final long SQL_TS     = 1_000L;
    private static final long GATEWAY_TS = 2_000L;

    private record Pair(String columnType, Object value) {
    }

    /** The outcome of a refused write, with the reason the surface gave. */
    private record Refused(String reason) {
    }

    // #10137
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
      int typeIdx = 0;
      for (final Pair pair : pairs) {
        // every pair as a FIELD, and as a TAG for the kinds a tag takes, since the fix covers both roles. The line protocol carries a
        // tag as text only, so a tag is held to the contract itself (storableValue on the declared column) rather than to the gateway
        final boolean asTagToo = switch (pair.columnType()) {
          case "BOOLEAN", "STRING", "INTEGER", "LONG" -> true;
          default -> false;
        };
        for (final boolean asTag : asTagToo ? new boolean[] { false, true } : new boolean[] { false }) {
          final String typeName = "P" + typeIdx++;
          database.command("sql", "CREATE TIMESERIES TYPE " + typeName + " TIMESTAMP ts " + (asTag ?
              "TAGS (c " + pair.columnType() + ") FIELDS (v DOUBLE)" :
              "FIELDS (c " + pair.columnType() + ")") + " SHARDS 1");

          final Object viaSql = viaSql(typeName, asTag, pair.value());
          final Object viaGateway = asTag ? viaContract(typeName, pair.value()) : viaGateway(typeName, pair.value());
          if (!sameOutcome(viaSql, viaGateway))
            disagreements.add(pair.value() + " (" + pair.value().getClass().getSimpleName() + ") -> " + pair.columnType() + (asTag ?
                " tag" :
                " field") + ": SQL=" + viaSql + " GATEWAY=" + viaGateway);
        }
      }

      assertThat(disagreements).as("pairs where SQL INSERT and the gateway disagree").isEmpty();
    }

    // #10137
    /** A multi-row {@code VALUES} refuses at the row whose value the column cannot hold, as a single row does. */
    @Test
    void aMultiRowValuesIsRefusedAtTheRowWithANumberForABooleanColumn() throws Exception {
      database.command("sql", "CREATE TIMESERIES TYPE MV TIMESTAMP ts FIELDS (bo BOOLEAN) SHARDS 1");

      assertThatThrownBy(() -> database.command("sql", "INSERT INTO MV (ts, bo) VALUES (7000, true), (7001, 1)"))
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("'bo'");
      final List<Object[]> rows = rows("MV");
      assertThat(rows.size()).as("at most the row before the refused one (a time series append commits on its own)")
          .isLessThanOrEqualTo(1);
      for (final Object[] row : rows)
        assertThat(row[1]).isEqualTo(Boolean.TRUE);
    }

    // #10137
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

    // #10137
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

    // #10137
    /** The same refusal through {@code (columns) VALUES (...)}. */
    @Test
    void aNumberIntoABooleanColumnIsRefusedThroughValues() throws Exception {
      database.command("sql", "CREATE TIMESERIES TYPE V TIMESTAMP ts FIELDS (bo BOOLEAN, s STRING) SHARDS 1");

      assertThatThrownBy(() -> database.command("sql", "INSERT INTO V (ts, bo, s) VALUES (7000, 7, 'x')"))
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("'bo'");
      assertThat(rows("V")).isEmpty();
    }

    // #10137
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

    private Object viaSql(final String typeName, final boolean asTag, final Object value) throws Exception {
      try {
        database.command("sql", "INSERT INTO " + typeName + " SET ts = ?, c = ?" + (asTag ? ", v = 1.0" : ""), SQL_TS, value);
      } catch (final CommandExecutionException e) {
        // only the statement's refusal counts as one: any other exception (a typo in the statement, a bare conversion error from the
        // document layer) fails the test instead of matching a gateway refusal by accident
        return new Refused(e.getMessage());
      }
      return storedAt(typeName, SQL_TS);
    }

    /** What {@code storableValue} makes of the value, appended through the engine as the gateway appends it, and read back. */
    private Object viaContract(final String typeName, final Object value) throws Exception {
      final LocalTimeSeriesType type = (LocalTimeSeriesType) database.getSchema().getType(typeName);
      final Object storable;
      try {
        storable = type.getTsColumn("c").storableValue(database, value);
      } catch (final IllegalArgumentException e) {
        return new Refused(e.getMessage());
      }
      type.getEngine().appendSamples(new long[] { GATEWAY_TS }, new Object[][] { { storable }, { 1.0d } });
      return storedAt(typeName, GATEWAY_TS);
    }

    private Object viaGateway(final String typeName, final Object value) throws Exception {
      final WriteReport report = TimeSeriesGateway.write((DatabaseInternal) database,
          new ArrayList<>(List.of(new Sample(typeName, Map.of(), Map.of("c", value), GATEWAY_TS))));
      if (report.written() == 0)
        return new Refused(String.join("; ", report.unreadableValues()));
      return storedAt(typeName, GATEWAY_TS);
    }

    private Object storedAt(final String typeName, final long ts) throws Exception {
      for (final Object[] row : rows(typeName))
        if (((Number) row[0]).longValue() == ts)
          return row[1];
      throw new AssertionError("no sample at " + ts + " in " + typeName);
    }

    /**
     * Both stored the same value, or both refused for the same reason: the gateway reports {@code <type>.c (field): <reason>}, the
     * statement and the contract the bare reason.
     */
    private static boolean sameOutcome(final Object sql, final Object gateway) {
      if (sql instanceof Refused(final String sqlReason) && gateway instanceof Refused(final String gatewayReason))
        return gatewayReason.equals(sqlReason) || gatewayReason.endsWith(": " + sqlReason);
      if (sql instanceof Refused || gateway instanceof Refused)
        return false;
      if (sql instanceof Number x && gateway instanceof Number y)
        return Double.compare(x.doubleValue(), y.doubleValue()) == 0;
      return sql == null ? gateway == null : sql.equals(gateway);
    }

    private List<Object[]> rows(final String typeName) throws Exception {
      return ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine().query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    }
  }

  /**
   * Issue #10232: {@code INSERT INTO <timeseries type> FROM SELECT} could not write a sample, because the copy step saved each
   * projected row as a plain document into a type that owns no bucket before the row reached the TimeSeries engine. The copy step no
   * longer saves (issue #10207), so every row reaches {@code SaveElementStep.saveToTimeSeries}: these tests pin that the statement
   * appends one sample per selected row, with the conversion and the column constraints of {@code INSERT ... SET}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class TimeSeriesInsertFromSelect {
    // #10232
    @Test
    void theReportedScenarioAppendsTheSampleInAndOutsideATransaction() throws Exception {
      database.command("sql", "CREATE TIMESERIES TYPE F TIMESTAMP ts FIELDS (bo BOOLEAN, s STRING) SHARDS 1");
      database.transaction(() -> database.command("sql", "INSERT INTO F FROM SELECT 7001 AS ts, true AS bo, 'x' AS s"));
      database.command("sql", "INSERT INTO F FROM SELECT 7002 AS ts, false AS bo, 'y' AS s");

      final List<Object[]> rows = rows("F");
      assertThat(rows).hasSize(2);
      assertThat(rows.get(0)).containsExactly(7001L, true, "x");
      assertThat(rows.get(1)).containsExactly(7002L, false, "y");
    }

    // #10232
    /** The element branch of the copy step: the selected rows are records of another type. */
    @Test
    void everyRecordOfASourceTypeBecomesOneSample() throws Exception {
      database.command("sql", "CREATE TIMESERIES TYPE Readings TIMESTAMP ts TAGS (sensor STRING) FIELDS (v DOUBLE) SHARDS 2");
      database.command("sql", "CREATE DOCUMENT TYPE Raw");
      database.transaction(() -> {
        for (int i = 0; i < 10; i++)
          database.command("sql", "INSERT INTO Raw SET ts = ?, sensor = ?, v = ?", 1000L + i, "s" + (i % 2), i * 1.5d);
      });

      database.command("sql", "INSERT INTO Readings FROM SELECT ts, sensor, v FROM Raw");
      final List<Object[]> rows = rows("Readings");
      assertThat(rows).hasSize(10);
      for (int i = 0; i < 10; i++) {
        assertThat(rows.get(i)[0]).isEqualTo(1000L + i);
        assertThat(rows.get(i)[1]).isEqualTo("s" + (i % 2));
        assertThat(((Number) rows.get(i)[2]).doubleValue()).isEqualTo(i * 1.5d);
      }

      // the whole record, not a projection of it
      database.command("sql", "CREATE TIMESERIES TYPE Copy TIMESTAMP ts TAGS (sensor STRING) FIELDS (v DOUBLE) SHARDS 1");
      database.command("sql", "INSERT INTO Copy FROM SELECT FROM Raw WHERE sensor = 's0'");
      assertThat(rows("Copy")).hasSize(5);
    }

    // #10232
    /** A selected row is held to what {@code INSERT ... SET} holds it to: the column conversion and the constraints. */
    @Test
    void aSelectedRowIsConvertedAndValidatedLikeInsertSet() throws Exception {
      database.command("sql", "CREATE TIMESERIES TYPE C TIMESTAMP ts FIELDS (bo BOOLEAN, i INTEGER, d DATETIME) SHARDS 1");

      // a number into a BOOLEAN column is refused, as by INSERT ... SET since #10137
      assertThatThrownBy(() -> database.command("sql", "INSERT INTO C FROM SELECT 1 AS ts, 7 AS bo"))
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("column 'bo'");
      // text of digits into a DATETIME column is refused naming it (#10233)
      assertThatThrownBy(() -> database.command("sql", "INSERT INTO C FROM SELECT 1 AS ts, '1700000000000' AS d"))
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("column 'd'");

      database.command("sql", "ALTER PROPERTY C.i MAX 10");
      assertThatThrownBy(() -> database.command("sql", "INSERT INTO C FROM SELECT 1 AS ts, 50 AS i"))
          .isInstanceOf(ValidationException.class).hasMessageContaining("MAX 10");
      assertThat(rows("C")).isEmpty();

      database.command("sql", "INSERT INTO C FROM SELECT 1 AS ts, true AS bo, 10 AS i, 1700000000000 AS d");
      assertThat(rows("C")).singleElement().satisfies(row -> {
        assertThat(row[1]).isEqualTo(true);
        assertThat(((Number) row[2]).intValue()).isEqualTo(10);
        assertThat(((Number) row[3]).longValue()).isEqualTo(1_700_000_000_000L);
      });
    }

    private List<Object[]> rows(final String typeName) throws Exception {
      return ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine().query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    }
  }

  /**
   * Issue #9365 (follow-up to #8646): {@code INSERT INTO <timeseries type> SET ...} built the row from the DECLARED columns
   * only, so a misspelled tag was stored as an empty tag, under another series than the one sent, and answered as a
   * success. It is now governed by {@link GlobalConfiguration#TIMESERIES_UNDECLARED_KEYS} like the line protocol and gRPC
   * writes: {@code reject} (default) fails the statement naming the key, {@code ignore} stores the row without it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SqlInsertUndeclaredKeys {
    @BeforeEach
    void createType() {
      database.command("sql", "CREATE TIMESERIES TYPE weather TIMESTAMP ts TAGS (city STRING) FIELDS (temp DOUBLE)");
    }

    // #9365
    @Test
    void aMisspelledTagFailsTheStatementAndStoresNothing() {
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql",
          "INSERT INTO weather SET ts = 1700000000000, citty = 'rome', temp = 21.5")))
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("citty").hasMessageContaining("weather");
      assertThat(count()).isZero();
    }

    // #9365
    @Test
    void anUndeclaredFieldFailsToo() {
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql",
          "INSERT INTO weather SET ts = 1700000000000, city = 'rome', temp = 21.5, humidity = 40")))
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("humidity");
      assertThat(count()).isZero();
    }

    // #9365
    @Test
    void aDeclaredRowStillInserts() {
      database.transaction(() -> database.command("sql", "INSERT INTO weather SET ts = 1700000000000, city = 'rome', temp = 21.5"));
      assertThat(count()).isEqualTo(1);
    }

    // #9365
    @Test
    void insertFromSelectAndContentAreCheckedAndInternalKeysAreNotProperties() {
      // CONTENT builds the document from a map: no internal @rid / @type key reaches the check
      database.transaction(() -> database.command("sql",
          "INSERT INTO weather CONTENT {\"ts\": 1700000000001, \"city\": \"oslo\", \"temp\": 2.5}"));
      assertThat(count()).isEqualTo(1);

      assertThatThrownBy(() -> database.transaction(() -> database.command("sql",
          "INSERT INTO weather CONTENT {\"ts\": 1700000000002, \"citty\": \"oslo\", \"temp\": 2.5}")))
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("citty");
      assertThat(count()).isEqualTo(1);
    }

    // #9365
    @Test
    void ignoreStoresTheRowWithoutTheUndeclaredKey() {
      database.getConfiguration().setValue(GlobalConfiguration.TIMESERIES_UNDECLARED_KEYS, "ignore");
      database.transaction(() -> database.command("sql",
          "INSERT INTO weather SET ts = 1700000000000, city = 'rome', temp = 21.5, extra = 1"));
      assertThat(count()).isEqualTo(1);
    }

    private long count() {
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM weather")) {
        return rs.next().<Long>getProperty("n");
      }
    }
  }

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
  @Nested
  class TimeSeriesUndeclaredKeys {
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

    // #8646
    @Test
    void misspelledTagKeyIsDroppedAndNamed() throws Exception {
      final WriteReport report = write(new Sample("weather", Map.of("citty", "rome"), Map.of("temp", 21.5), T0));

      assertThat(report.isComplete()).isFalse();
      assertThat(report.written()).isZero();
      assertThat(report.dropped()).isEqualTo(1);
      assertThat(report.undeclaredKeys()).containsExactly("weather.citty (tag)");
      assertThat(rows()).as("no row under a null city").isZero();
    }

    // #8646
    @Test
    void undeclaredFieldKeyIsDroppedAndNamed() throws Exception {
      final WriteReport report = write(new Sample("weather", Map.of("city", "rome"), Map.of("tmp", 21.5), T0));

      assertThat(report.dropped()).isEqualTo(1);
      assertThat(report.undeclaredKeys()).containsExactly("weather.tmp (field)");
      assertThat(rows()).isZero();
    }

    // #8646
    @Test
    void keyDeclaredInTheOtherRoleIsUndeclared() throws Exception {
      // 'temp' is a FIELD column: sent as a tag, the column loop never reads it, so it would be lost as silently
      final WriteReport report = write(new Sample("weather", Map.of("city", "rome", "temp", "21"), Map.of(), T0),
          new Sample("weather", Map.of(), Map.of("city", "rome", "temp", 21.5), T0 + 1));

      assertThat(report.dropped()).isEqualTo(2);
      assertThat(report.undeclaredKeys()).containsExactly("weather.temp (tag)", "weather.city (field)");
      assertThat(rows()).isZero();
    }

    // #8646
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

    // #8646
    @Test
    void aSampleOmittingADeclaredKeyIsStillAccepted() throws Exception {
      // Absence is not misspelling: a sample with no tags at all says "no value for this column"
      final WriteReport report = write(new Sample("weather", Map.of(), Map.of("temp", 21.5), T0));

      assertThat(report.isComplete()).isTrue();
      assertThat(report.undeclaredKeys()).isEmpty();
      assertThat(rows()).isEqualTo(1);
    }

    // #8646
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

    // #8646
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

    // #8646
    /**
     * A mistyped policy must be refused, not read as some other value: the gateway treats anything but {@code ignore}
     * as {@code reject}, so an operator who wrote {@code 'ignroe'} would otherwise keep rejecting without being told.
     */
    @Test
    void aMistypedPolicyIsRefused() {
      assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `arcadedb.timeSeriesUndeclaredKeys` 'ignroe'"))
          .hasStackTraceContaining("Allowed values are");
      assertThat(database.getConfiguration().getValueAsString(GlobalConfiguration.TIMESERIES_UNDECLARED_KEYS))
          .isEqualTo("reject");
    }
  }
}
