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
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10064: {@code INSERT INTO} a TIMESERIES type used to convert each value with a private {@code convertValue} that applied a
 * different contract than {@link ColumnDefinition#storableValue}, the one the line protocol, gRPC and Prometheus remote write share
 * (issue #10030). The same value written to the same column was stored differently, or refused, depending on the surface.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10064TimeSeriesInsertStorableValueTest extends TestHelper {

  private static final long BASE_TS = 1_700_000_000_000L;

  private TimeSeriesEngine engineOf(final String typeName) {
    return ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();
  }

  private List<Object[]> rows(final String typeName) throws Exception {
    return engineOf(typeName).query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
  }

  /**
   * A number the column cannot hold is refused and no sample is stored. The document layer has already converted each property to
   * its declared type by the time the step runs, so it is the first to refuse (which is why the INTEGER overflow of the issue never
   * narrowed through SQL); whichever layer refuses, the statement stores nothing.
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

  /** A FLOAT literal into a DOUBLE column reads as its decimal, not as the single precision rounding error (issue #7609). */
  @Test
  void aFloatIntoADoubleColumnWidensThroughItsDecimalForm() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE Widen TIMESTAMP ts FIELDS (d DOUBLE) SHARDS 1");
    database.command("sql", "INSERT INTO Widen SET ts = " + BASE_TS + ", d = 0.05F");

    assertThat(rows("Widen")).singleElement().satisfies(row -> assertThat(row[1]).isEqualTo(0.05d));
  }

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
}
