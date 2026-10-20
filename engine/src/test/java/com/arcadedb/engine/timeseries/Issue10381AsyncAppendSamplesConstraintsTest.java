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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10381: {@code database.async().appendSamples()} handed the engine a grid of its own, so a TIMESERIES sample that
 * MANDATORY / MIN / MAX refuse everywhere else was stored (and the type was then wedged: the constraint already in force could not
 * be declared again, and the sample could not be deleted), while a value the column cannot store at all threw inside the async
 * worker and the batch was silently lost. Both overloads now convert and check on the calling thread, with the code every other
 * surface uses, and refuse the whole batch.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10381AsyncAppendSamplesConstraintsTest extends TestHelper {

  @BeforeEach
  void createType() {
    database.command("sql", "CREATE TIMESERIES TYPE M TIMESTAMP ts TAGS (host STRING) FIELDS (a INTEGER) SHARDS 1");
    database.command("sql", "ALTER PROPERTY M.host MANDATORY true");
    database.command("sql", "ALTER PROPERTY M.a MIN 5");
    database.command("sql", "ALTER PROPERTY M.a MAX 50");
  }

  @Test
  void sqlRefusesTheSampleTheAsyncApiUsedToStore() {
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO M SET ts = 3000, a = 999")).isInstanceOf(ValidationException.class);
  }

  @Test
  void aNullMandatoryTagIsRefusedToTheCaller() {
    assertThatThrownBy(() -> database.async().appendSamples("M", new long[] { 1000L }, new Object[] { null }, new Object[] { 10 }))
        .isInstanceOf(ValidationException.class).hasMessageContaining("M.host").hasMessageContaining("MANDATORY");
    database.async().waitCompletion();
    assertThat(count()).isZero();
  }

  @Test
  void aFieldPastItsMaxIsRefusedToTheCaller() {
    assertThatThrownBy(() -> database.async().appendSamples("M", new long[] { 1000L }, new Object[] { "h" }, new Object[] { 999 }))
        .isInstanceOf(ValidationException.class).hasMessageContaining("M.a").hasMessageContaining("MAX 50");
    assertThatThrownBy(() -> database.async().appendSamples("M", new long[] { 1000L }, new Object[] { "h" }, new Object[] { 1 }))
        .isInstanceOf(ValidationException.class).hasMessageContaining("M.a").hasMessageContaining("MIN 5");
    database.async().waitCompletion();
    assertThat(count()).isZero();
  }

  /** The type is not wedged any more: the constraint in force can be declared again, because nothing violating it was stored. */
  @Test
  void theTypeStaysAlterable() {
    assertThatThrownBy(() -> database.async().appendSamples("M", new long[] { 1000L }, new Object[] { "h" }, new Object[] { 999 }))
        .isInstanceOf(ValidationException.class);
    database.async().waitCompletion();

    assertThatCode(() -> database.command("sql", "ALTER PROPERTY M.a MAX 50")).doesNotThrowAnyException();
  }

  /** A value no column can store was lost inside the worker, with waitCompletion() returning normally. */
  @Test
  void aValueTheColumnCannotStoreIsRefusedToTheCaller() {
    assertThatThrownBy(() -> database.async().appendSamples("M", new long[] { 2000L }, new Object[] { "h" }, new Object[] { "not-a-number" }))
        .isInstanceOf(ValidationException.class).hasMessageContaining("INTEGER column 'a'");
    database.async().waitCompletion();
    assertThat(count()).isZero();
  }

  /** All or nothing: one refused row keeps the conforming ones of the same call out too, and the message counts them. */
  @Test
  void aBatchWithOneRefusedRowAppendsNothing() {
    assertThatThrownBy(() -> database.async()
        .appendSamples("M", new long[] { 1000L, 2000L, 3000L }, new Object[] { "h", "h", "h" }, new Object[] { 10, 999, 20 }))
        .isInstanceOf(ValidationException.class).hasMessageContaining("1 of 3 rows");
    database.async().waitCompletion();
    assertThat(count()).isZero();
  }

  @Test
  void aConformingBatchIsStoredAndTextForANumericColumnIsConvertedLikeEverySurface() {
    final long[] timestamps = { 1000L, 2000L };
    final Object[] hosts = { "h1", "h2" };
    final Object[] values = { 10, "20" };
    database.async().appendSamples("M", timestamps, hosts, values);
    database.async().waitCompletion();

    assertThat(count()).isEqualTo(2);
    try (final ResultSet rs = database.query("sql", "SELECT a FROM M WHERE host = 'h2'")) {
      assertThat(rs.next().<Number>getProperty("a").intValue()).isEqualTo(20);
    }
    // the caller's arrays are its own again
    hosts[0] = "changed";
    values[0] = 999;
    assertThat(count()).isEqualTo(2);
  }

  @Test
  void aShapeThatIsNotTheTypesIsRefused() {
    assertThatThrownBy(() -> database.async().appendSamples("M", new long[] { 1000L }, new Object[] { "h" }))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("2 value columns");
    assertThatThrownBy(() -> database.async().appendSamples("M", new long[] { 1000L, 2000L }, new Object[] { "h" }, new Object[] { 10 }))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("2 timestamps");
  }

  @Test
  void aPrimitiveSourceIsHeldToTheConstraintsToo() throws Exception {
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("M")).getEngine();

    final TimeSeriesBatch violating = engine.newBatch(2);
    int row = violating.addRow(1000L);
    violating.setString(row, 0, "h");
    violating.setInt(row, 1, 10);
    row = violating.addRow(2000L);
    violating.setString(row, 0, "h");
    violating.setInt(row, 1, 999);
    assertThatThrownBy(() -> database.async().appendSamples("M", violating)).isInstanceOf(ValidationException.class)
        .hasMessageContaining("M.a").hasMessageContaining("MAX 50").hasMessageContaining("1 of 2 rows");

    final TimeSeriesBatch noTag = engine.newBatch(1);
    row = noTag.addRow(3000L);
    noTag.setString(row, 0, null);
    noTag.setInt(row, 1, 10);
    assertThatThrownBy(() -> database.async().appendSamples("M", noTag)).isInstanceOf(ValidationException.class)
        .hasMessageContaining("M.host");

    database.async().waitCompletion();
    assertThat(count()).isZero();

    final TimeSeriesBatch conforming = engine.newBatch(1);
    row = conforming.addRow(4000L);
    conforming.setString(row, 0, "h");
    conforming.setInt(row, 1, 10);
    database.async().appendSamples("M", conforming);
    database.async().waitCompletion();
    assertThat(count()).isEqualTo(1);
  }

  /** A type with no constraint on any column pays nothing and refuses nothing it did not before. */
  @Test
  void aTypeWithoutConstraintsIsUnchanged() {
    database.command("sql", "CREATE TIMESERIES TYPE Plain TIMESTAMP ts TAGS (host STRING) FIELDS (a INTEGER) SHARDS 1");
    database.async().appendSamples("Plain", new long[] { 1000L }, new Object[] { null }, new Object[] { 999 });
    database.async().waitCompletion();

    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM Plain")) {
      assertThat(rs.next().<Number>getProperty("n").longValue()).isEqualTo(1L);
    }
  }

  private long count() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM M")) {
      return rs.next().<Number>getProperty("n").longValue();
    }
  }
}
