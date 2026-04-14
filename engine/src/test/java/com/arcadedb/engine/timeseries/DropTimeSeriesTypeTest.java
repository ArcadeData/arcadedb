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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for DROP TIMESERIES TYPE SQL statement and data cleanup on drop.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class DropTimeSeriesTypeTest extends TestHelper {

  @Test
  void dropTimeSeriesTypeSyntax() {
    // Issue #3863: DROP TIMESERIES TYPE should be accepted as valid syntax
    database.command("sql",
        "CREATE TIMESERIES TYPE Ticker TIMESTAMP ts TAGS (name STRING) FIELDS (f1 DOUBLE) SHARDS 1");

    assertThat(database.getSchema().existsType("Ticker")).isTrue();

    // This should NOT throw a syntax error
    final ResultSet rs = database.command("sql", "DROP TIMESERIES TYPE Ticker");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat((String) row.getProperty("operation")).isEqualTo("drop type");
    assertThat((String) row.getProperty("typeName")).isEqualTo("Ticker");

    assertThat(database.getSchema().existsType("Ticker")).isFalse();
  }

  @Test
  void dropTimeSeriesTypeIfExists() {
    // DROP TIMESERIES TYPE ... IF EXISTS should not throw for nonexistent types
    final ResultSet rs = database.command("sql", "DROP TIMESERIES TYPE NonExistent IF EXISTS");
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void dropTimeSeriesTypeWithPlainDropType() {
    // Plain DROP TYPE should still work for timeseries types
    database.command("sql",
        "CREATE TIMESERIES TYPE PlainDrop TIMESTAMP ts FIELDS (value DOUBLE) SHARDS 1");

    assertThat(database.getSchema().existsType("PlainDrop")).isTrue();

    database.command("sql", "DROP TYPE PlainDrop");

    assertThat(database.getSchema().existsType("PlainDrop")).isFalse();
  }

  @Test
  void dropAndRecreateTimeSeriesTypeDataCleanup() {
    // Issue #3863: after dropping and recreating, old data must NOT reappear
    database.command("sql",
        "CREATE TIMESERIES TYPE Ticker TIMESTAMP ts TAGS (name STRING) FIELDS (f1 DOUBLE) SHARDS 1");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Ticker SET ts = 1000, name = 'AAPL', f1 = 150.0");
      database.command("sql", "INSERT INTO Ticker SET ts = 2000, name = 'GOOG', f1 = 2800.0");
      database.command("sql", "INSERT INTO Ticker SET ts = 3000, name = 'MSFT', f1 = 300.0");
    });

    // Verify data was inserted
    ResultSet rs = database.query("sql", "SELECT FROM Ticker");
    List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());
    assertThat(results).hasSize(3);

    // Drop the type
    database.command("sql", "DROP TYPE Ticker");
    assertThat(database.getSchema().existsType("Ticker")).isFalse();

    // Recreate the same type
    database.command("sql",
        "CREATE TIMESERIES TYPE Ticker TIMESTAMP ts TAGS (name STRING) FIELDS (f1 DOUBLE) SHARDS 1");

    // Verify the count is 0 - no old data should reappear
    rs = database.query("sql", "SELECT FROM Ticker");
    results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());
    assertThat(results).isEmpty();
  }
}
