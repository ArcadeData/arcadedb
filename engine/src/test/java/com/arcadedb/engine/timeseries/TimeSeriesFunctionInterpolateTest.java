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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TimeSeriesFunctionInterpolateTest extends TestHelper {

  @Test
  public void testNoNulls() {
    database.command("sql",
        "CREATE TIMESERIES TYPE InterpSensor TIMESTAMP ts FIELDS (value DOUBLE)");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO InterpSensor SET ts = 1000, value = 1.0");
      database.command("sql", "INSERT INTO InterpSensor SET ts = 2000, value = 2.0");
      database.command("sql", "INSERT INTO InterpSensor SET ts = 3000, value = 3.0");
    });

    final ResultSet rs = database.query("sql", "SELECT ts.interpolate(value, 'zero') AS filled FROM InterpSensor");
    @SuppressWarnings("unchecked")
    final List<Object> filled = (List<Object>) rs.next().getProperty("filled");

    assertThat(filled).hasSize(3);
    assertThat(((Number) filled.get(0)).doubleValue()).isEqualTo(1.0);
    assertThat(((Number) filled.get(1)).doubleValue()).isEqualTo(2.0);
    assertThat(((Number) filled.get(2)).doubleValue()).isEqualTo(3.0);
  }

  @Test
  public void testZeroMethod() {
    database.command("sql", "CREATE DOCUMENT TYPE ZeroInterp");
    database.command("sql", "CREATE PROPERTY ZeroInterp.ts LONG");
    database.command("sql", "CREATE PROPERTY ZeroInterp.value DOUBLE");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO ZeroInterp SET ts = 1000, value = 10.0");
      database.command("sql", "INSERT INTO ZeroInterp SET ts = 2000");  // null value
      database.command("sql", "INSERT INTO ZeroInterp SET ts = 3000, value = 30.0");
    });

    final ResultSet rs = database.query("sql",
        "SELECT ts.interpolate(value, 'zero') AS filled FROM ZeroInterp ORDER BY ts");
    @SuppressWarnings("unchecked")
    final List<Object> filled = (List<Object>) rs.next().getProperty("filled");

    assertThat(filled).hasSize(3);
    assertThat(((Number) filled.get(0)).doubleValue()).isEqualTo(10.0);
    assertThat(((Number) filled.get(1)).doubleValue()).isEqualTo(0.0);
    assertThat(((Number) filled.get(2)).doubleValue()).isEqualTo(30.0);
  }

  @Test
  public void testPrevMethodWithDocumentType() {
    // Use a regular document type where nulls are properly preserved
    database.command("sql", "CREATE DOCUMENT TYPE PrevInterp");
    database.command("sql", "CREATE PROPERTY PrevInterp.ts LONG");
    database.command("sql", "CREATE PROPERTY PrevInterp.value DOUBLE");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO PrevInterp SET ts = 1000, value = 10.0");
      database.command("sql", "INSERT INTO PrevInterp SET ts = 2000");  // null value
      database.command("sql", "INSERT INTO PrevInterp SET ts = 3000");  // null value
      database.command("sql", "INSERT INTO PrevInterp SET ts = 4000, value = 40.0");
    });

    final ResultSet rs = database.query("sql",
        "SELECT ts.interpolate(value, 'prev') AS filled FROM PrevInterp ORDER BY ts");
    @SuppressWarnings("unchecked")
    final List<Object> filled = (List<Object>) rs.next().getProperty("filled");

    assertThat(filled).hasSize(4);
    assertThat(((Number) filled.get(0)).doubleValue()).isEqualTo(10.0);
    assertThat(((Number) filled.get(1)).doubleValue()).isEqualTo(10.0);
    assertThat(((Number) filled.get(2)).doubleValue()).isEqualTo(10.0);
    assertThat(((Number) filled.get(3)).doubleValue()).isEqualTo(40.0);
  }

  @Test
  public void testAllNullsWithZero() {
    database.command("sql", "CREATE DOCUMENT TYPE AllNullInterp");
    database.command("sql", "CREATE PROPERTY AllNullInterp.ts LONG");
    database.command("sql", "CREATE PROPERTY AllNullInterp.value DOUBLE");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO AllNullInterp SET ts = 1000");
      database.command("sql", "INSERT INTO AllNullInterp SET ts = 2000");
    });

    final ResultSet rs = database.query("sql",
        "SELECT ts.interpolate(value, 'zero') AS filled FROM AllNullInterp ORDER BY ts");
    @SuppressWarnings("unchecked")
    final List<Object> filled = (List<Object>) rs.next().getProperty("filled");

    assertThat(filled).hasSize(2);
    assertThat(((Number) filled.get(0)).doubleValue()).isEqualTo(0.0);
    assertThat(((Number) filled.get(1)).doubleValue()).isEqualTo(0.0);
  }
}
