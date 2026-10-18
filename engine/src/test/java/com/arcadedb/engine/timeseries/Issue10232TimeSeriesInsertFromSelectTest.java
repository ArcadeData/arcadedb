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
import com.arcadedb.exception.ValidationException;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10232: {@code INSERT INTO <timeseries type> FROM SELECT} could not write a sample, because the copy step saved each
 * projected row as a plain document into a type that owns no bucket before the row reached the TimeSeries engine. The copy step no
 * longer saves (issue #10207), so every row reaches {@code SaveElementStep.saveToTimeSeries}: these tests pin that the statement
 * appends one sample per selected row, with the conversion and the column constraints of {@code INSERT ... SET}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10232TimeSeriesInsertFromSelectTest extends TestHelper {

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
