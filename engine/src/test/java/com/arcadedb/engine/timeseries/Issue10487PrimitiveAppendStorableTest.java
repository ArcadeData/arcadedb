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
import com.arcadedb.exception.ValidationException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10487: {@code database.async().appendSamples(type, TimeSeriesRowSource)} promised that a primitive source "holds values its
 * columns can store by construction", and it does not. A TAG or STRING text over the 256 UTF-8 bytes a row can hold was accepted by
 * the call and then failed inside the async worker, taking the whole batch (the conforming rows too) with it, with nobody to tell. And
 * {@link TimeSeriesBatch#setValue}, documented to store "exactly what the {@code Object[][]} path would have stored", narrowed a long
 * past the INTEGER range modulo 2^32, turned any text of a BOOLEAN column into {@code false} and threw a bare
 * {@code ClassCastException} for numeric text, which the SQL INSERT converts. The door now refuses an unstorable text on the calling
 * thread, as the boxed overload does, and {@code setValue} converts and refuses like {@link ColumnDefinition#storableValue}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10487PrimitiveAppendStorableTest extends TestHelper {
  private static final int MAX_BYTES = TimeSeriesBucket.MAX_STRING_BYTES;

  @BeforeEach
  void createType() {
    database.command("sql",
        "CREATE TIMESERIES TYPE M TIMESTAMP ts TAGS (host STRING) FIELDS (a INTEGER, l LONG, d DOUBLE, s STRING, b BOOLEAN) SHARDS 2");
  }

  @Test
  void aTagOverTheLimitIsRefusedToTheCallerAndNothingOfTheBatchIsLost() throws Exception {
    final AtomicInteger workerErrors = new AtomicInteger();
    database.async().onError(e -> workerErrors.incrementAndGet());

    final TimeSeriesBatch batch = engine().newBatch(3);
    fill(batch, 1000L, "h1", 10);
    fill(batch, 2000L, "h".repeat(3000), 10);
    fill(batch, 3000L, "h3", 10);

    assertThatThrownBy(() -> database.async().appendSamples("M", batch)).isInstanceOf(ValidationException.class)
        .hasMessageContaining("M.host").hasMessageContaining(String.valueOf(MAX_BYTES)).hasMessageContaining("1 of 3 rows");
    database.async().waitCompletion();

    assertThat(count()).isZero();
    assertThat(workerErrors.get()).as("the worker never saw the batch").isZero();
  }

  @Test
  void aStringFieldOverTheLimitIsRefusedToTheCaller() {
    final TimeSeriesBatch batch = engine().newBatch(2);
    int row = batch.addRow(1000L);
    batch.setString(row, 0, "h");
    batch.setString(row, 4, "s".repeat(MAX_BYTES + 1));
    row = batch.addRow(2000L);
    batch.setString(row, 0, "h");
    batch.setString(row, 4, "ok");

    assertThatThrownBy(() -> database.async().appendSamples("M", batch)).isInstanceOf(ValidationException.class)
        .hasMessageContaining("M.s").hasMessageContaining("1 of 2 rows");
    database.async().waitCompletion();
    assertThat(count()).isZero();
  }

  /** The limit counts UTF-8 bytes, not chars: 86 three-byte characters are 258 bytes, 85 are 255. */
  @Test
  void theLimitCountsUtf8BytesAndAtTheLimitIsStored() {
    final String threeBytes = "€"; // EURO SIGN, 3 UTF-8 bytes

    final TimeSeriesBatch over = engine().newBatch(1);
    fill(over, 1000L, threeBytes.repeat(86), 1);
    assertThatThrownBy(() -> database.async().appendSamples("M", over)).isInstanceOf(ValidationException.class)
        .hasMessageContaining("M.host");

    final TimeSeriesBatch within = engine().newBatch(2);
    fill(within, 2000L, threeBytes.repeat(85), 1);
    fill(within, 3000L, "x".repeat(MAX_BYTES), 2);
    assertThatCode(() -> database.async().appendSamples("M", within)).doesNotThrowAnyException();
    database.async().waitCompletion();

    assertThat(count()).isEqualTo(2);
  }

  /** The type has constraints too: the length is still checked, together with them, and the whole batch is refused. */
  @Test
  void aTypeWithConstraintsRefusesBothTheConstraintAndTheLengthInOneAnswer() {
    database.command("sql", "ALTER PROPERTY M.a MAX 50");

    final TimeSeriesBatch batch = engine().newBatch(3);
    fill(batch, 1000L, "h1", 10);
    fill(batch, 2000L, "h".repeat(3000), 10);
    fill(batch, 3000L, "h3", 999);

    assertThatThrownBy(() -> database.async().appendSamples("M", batch)).isInstanceOf(ValidationException.class)
        .hasMessageContaining("M.host").hasMessageContaining("M.a").hasMessageContaining("2 of 3 rows");
    database.async().waitCompletion();
    assertThat(count()).isZero();
  }

  @Test
  void aConformingBatchIsStoredAsBefore() {
    final TimeSeriesBatch batch = engine().newBatch(2);
    fill(batch, 1000L, "h1", 10);
    fill(batch, 2000L, "h2", 20);
    database.async().appendSamples("M", batch);
    database.async().waitCompletion();

    assertThat(count()).isEqualTo(2);
  }

  @Test
  void aNullTagIsNotATextOverTheLimit() {
    final TimeSeriesBatch batch = engine().newBatch(1);
    fill(batch, 1000L, null, 10);
    assertThatCode(() -> database.async().appendSamples("M", batch)).doesNotThrowAnyException();
    database.async().waitCompletion();
    assertThat(count()).isEqualTo(1);
  }

  // ------------------------------------------------------------------------------------------------------------------------------
  // TimeSeriesBatch.setValue converts like ColumnDefinition.storableValue, i.e. like the INSERT

  @Test
  void setValueRefusesALongPastTheIntegerRangeInsteadOfStoringItModulo2To32() {
    final TimeSeriesBatch batch = engine().newBatch(1);
    final int row = batch.addRow(1000L);
    assertThatThrownBy(() -> batch.setValue(row, 1, 4294967316L)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("INTEGER column 'a'").hasMessageContaining("out of range");
    // the INSERT refuses the same value
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO M SET ts = 1000, host = 'h', a = 4294967316")).isNotNull();
  }

  @Test
  void setValueDoesNotTurnTextOrANumberIntoFalseForABooleanColumn() {
    final TimeSeriesBatch batch = engine().newBatch(1);
    final int row = batch.addRow(1000L);
    final int booleanColumn = 5;
    assertThatThrownBy(() -> batch.setValue(row, booleanColumn, "maybe")).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("BOOLEAN column 'b'");
    assertThatThrownBy(() -> batch.setValue(row, booleanColumn, 1)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("BOOLEAN column 'b'");

    batch.setValue(row, booleanColumn, "true");
    assertThat(batch.getRawValue(row, booleanColumn)).isEqualTo(1L);
    batch.setValue(row, booleanColumn, "FALSE");
    assertThat(batch.getRawValue(row, booleanColumn)).isZero();
    batch.setValue(row, booleanColumn, Boolean.TRUE);
    assertThat(batch.getRawValue(row, booleanColumn)).isEqualTo(1L);
  }

  @Test
  void setValueConvertsNumericTextLikeTheInsert() {
    final TimeSeriesBatch batch = engine().newBatch(1);
    final int row = batch.addRow(1000L);
    batch.setString(row, 0, "h");
    batch.setValue(row, 1, "20");
    batch.setValue(row, 2, "123456789012");
    batch.setValue(row, 3, "1.5");
    assertThat(batch.getRawValue(row, 1)).isEqualTo(20L);
    assertThat(batch.getRawValue(row, 2)).isEqualTo(123456789012L);
    assertThat(Double.longBitsToDouble(batch.getRawValue(row, 3))).isEqualTo(1.5d);

    assertThatThrownBy(() -> batch.setValue(row, 1, "abc")).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("INTEGER column 'a'");
  }

  @Test
  void setValueRefusesATextOverTheLimitForAStringColumnAndANonTextForIt() {
    final TimeSeriesBatch batch = engine().newBatch(1);
    final int row = batch.addRow(1000L);
    assertThatThrownBy(() -> batch.setValue(row, 0, "h".repeat(MAX_BYTES + 1))).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("host");
    assertThatThrownBy(() -> batch.setValue(row, 4, 42)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("STRING column 's'");
  }

  /** What setValue stores is what the SQL INSERT stores, value for value. */
  @Test
  void aBatchFilledWithSetValueStoresWhatTheInsertStores() throws Exception {
    database.command("sql", "INSERT INTO M SET ts = 1000, host = 'sql', a = '20', l = '123456789012', d = '1.5', s = 'x', b = 'true'");

    final TimeSeriesBatch batch = engine().newBatch(1);
    final int row = batch.addRow(1000L);
    batch.setValue(row, 0, "batch");
    batch.setValue(row, 1, "20");
    batch.setValue(row, 2, "123456789012");
    batch.setValue(row, 3, "1.5");
    batch.setValue(row, 4, "x");
    batch.setValue(row, 5, "true");
    database.async().appendSamples("M", batch);
    database.async().waitCompletion();

    final Result viaSql = row("sql");
    final Result viaBatch = row("batch");
    for (final String column : new String[] { "a", "l", "d", "s", "b" })
      assertThat(viaBatch.<Object>getProperty(column)).as(column).isEqualTo(viaSql.<Object>getProperty(column));
  }

  @Test
  void setValueKeepsTheNullAndTheNumberPaths() {
    final TimeSeriesBatch batch = engine().newBatch(1);
    final int row = batch.addRow(1000L);
    batch.setValue(row, 1, null);
    assertThat(batch.isNull(row, 1)).isTrue();
    batch.setValue(row, 1, 7);
    assertThat(batch.isNull(row, 1)).isFalse();
    assertThat(batch.getRawValue(row, 1)).isEqualTo(7L);
    batch.setValue(row, 3, 7.9d);
    assertThat(Double.longBitsToDouble(batch.getRawValue(row, 3))).isEqualTo(7.9d);
    batch.setValue(row, 5, null);
    assertThat(batch.isNull(row, 5)).isTrue();
  }

  /**
   * A batch built without a database (the public constructor) reads date text with the default formats and the JVM's time zone, as
   * {@code ColumnDefinition.storableValue} does with no database; one made by the engine reads it as the INSERT does, so the two name
   * the same instant only when the JVM's zone is UTC.
   */
  @Test
  void setValueOnABatchWithoutADatabaseReadsDateTextWithTheDefaultFormats() {
    database.command("sql", "CREATE TIMESERIES TYPE D TIMESTAMP ts TAGS (host STRING) FIELDS (t DATETIME) SHARDS 1");
    final LocalTimeSeriesType type = (LocalTimeSeriesType) database.getSchema().getType("D");
    final LocalDateTime text = LocalDateTime.of(2020, 1, 2, 3, 4, 5);

    final TimeSeriesBatch withoutDatabase = new TimeSeriesBatch(type.getTsColumns(), 1);
    final int row = withoutDatabase.addRow(1000L);
    withoutDatabase.setValue(row, 1, "2020-01-02 03:04:05");
    assertThat(withoutDatabase.getRawValue(row, 1)).isEqualTo(text.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());

    final TimeSeriesBatch withDatabase = type.getEngine().newBatch(1);
    final int other = withDatabase.addRow(1000L);
    withDatabase.setValue(other, 1, "2020-01-02 03:04:05");
    // the text names the same instant the INSERT stores for it: the wall clock of the text read as UTC
    assertThat(withDatabase.getRawValue(other, 1)).isEqualTo(text.toInstant(ZoneOffset.UTC).toEpochMilli());

    // a number is a count in the column's unit, whatever the constructor
    withoutDatabase.setValue(row, 1, 1_577_934_246_000L);
    assertThat(withoutDatabase.getRawValue(row, 1)).isEqualTo(1_577_934_246_000L);
  }

  private TimeSeriesEngine engine() {
    return ((LocalTimeSeriesType) database.getSchema().getType("M")).getEngine();
  }

  private static void fill(final TimeSeriesBatch batch, final long ts, final String host, final int a) {
    final int row = batch.addRow(ts);
    batch.setString(row, 0, host);
    batch.setInt(row, 1, a);
  }

  private Result row(final String host) {
    try (final ResultSet rs = database.query("sql", "SELECT a, l, d, s, b FROM M WHERE host = ?", host)) {
      assertThat(rs.hasNext()).as("a row for host " + host).isTrue();
      return rs.next();
    }
  }

  private long count() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM M")) {
      return rs.next().<Number>getProperty("n").longValue();
    }
  }
}
