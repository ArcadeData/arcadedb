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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that ts.* namespaced functions are correctly resolved by the SQL parser.
 */
public class TimeSeriesNamespaceTest extends TestHelper {

  @Test
  public void testTsFirstNamespace() {
    database.command("sql", "CREATE TIMESERIES TYPE NsSensor TIMESTAMP ts FIELDS (value DOUBLE)");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO NsSensor SET ts = 1000, value = 10.0");
      database.command("sql", "INSERT INTO NsSensor SET ts = 2000, value = 20.0");
      database.command("sql", "INSERT INTO NsSensor SET ts = 3000, value = 30.0");
    });

    final ResultSet rs = database.query("sql",
        "SELECT ts.first(value, ts) AS first_val FROM NsSensor");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(((Number) row.getProperty("first_val")).doubleValue()).isEqualTo(10.0);
    rs.close();
  }

  @Test
  public void testTsTimeBucketNamespace() {
    database.command("sql", "CREATE TIMESERIES TYPE BucketNs TIMESTAMP ts FIELDS (value DOUBLE)");
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.command("sql", "INSERT INTO BucketNs SET ts = " + (i * 1000) + ", value = " + (i * 1.0));
    });

    final ResultSet rs = database.query("sql",
        "SELECT ts.timeBucket('5s', ts) AS tb, avg(value) AS avg_val FROM BucketNs GROUP BY tb ORDER BY tb");
    assertThat(rs.hasNext()).isTrue();
    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    assertThat(count).isEqualTo(2);
    rs.close();
  }

  @Test
  public void testTsRateWithTimeBucket() {
    database.command("sql", "CREATE TIMESERIES TYPE RateNs TIMESTAMP ts FIELDS (value DOUBLE)");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO RateNs SET ts = 1000, value = 10.0");
      database.command("sql", "INSERT INTO RateNs SET ts = 2000, value = 20.0");
      database.command("sql", "INSERT INTO RateNs SET ts = 3000, value = 30.0");
      database.command("sql", "INSERT INTO RateNs SET ts = 4000, value = 40.0");
    });

    final ResultSet rs = database.query("sql",
        "SELECT ts.timeBucket('2s', ts) AS tb, ts.rate(value, ts) AS r FROM RateNs GROUP BY tb ORDER BY tb");
    assertThat(rs.hasNext()).isTrue();
    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    assertThat(count).isGreaterThan(0);
    rs.close();
  }

  @Test
  public void testMixedNamespacedAndRegularFunctions() {
    database.command("sql", "CREATE TIMESERIES TYPE MixedNs TIMESTAMP ts FIELDS (value DOUBLE)");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO MixedNs SET ts = 1000, value = 10.0");
      database.command("sql", "INSERT INTO MixedNs SET ts = 2000, value = 20.0");
      database.command("sql", "INSERT INTO MixedNs SET ts = 3000, value = 30.0");
    });

    final ResultSet rs = database.query("sql",
        "SELECT ts.first(value, ts) AS first_val, avg(value) AS avg_val, count(*) AS cnt FROM MixedNs");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(((Number) row.getProperty("first_val")).doubleValue()).isEqualTo(10.0);
    assertThat(((Number) row.getProperty("avg_val")).doubleValue()).isEqualTo(20.0);
    assertThat(((Number) row.getProperty("cnt")).longValue()).isEqualTo(3L);
    rs.close();
  }
}
