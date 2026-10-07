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
import com.arcadedb.function.sql.math.SQLFunctionSum;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9351: the generic SQL {@code sum()} answered the Integer 0 for a group holding no value (an empty group or an
 * all-NULL/absent one) while the time-series push-down answered NULL for the same samples. SQL says SUM over no value is
 * NULL, which is also what {@link TimeSeriesNaN} documents as its model.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9351GenericSumAllAbsentTest extends TestHelper {

  private static final long HOUR = 3_600_000L;

  @Test
  void genericSumOverAnAllAbsentTimeSeriesWindowIsNull() throws Exception {
    database.command("sql", "CREATE TIMESERIES TYPE P9351 TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1");
    final int n = 20;
    final long[] ts = new long[n];
    final Object[] host = new Object[n];
    final Object[] v = new Object[n];
    for (int i = 0; i < n; i++) {
      ts[i] = (i < 10 ? 0L : HOUR) + (i % 10) * 60_000L;
      host[i] = "h1";
      v[i] = i < 10 ? (double) (i + 1) : Double.NaN;
    }
    ((LocalTimeSeriesType) database.getSchema().getType("P9351")).getEngine().appendSamples(ts, host, v);

    final String window = "ts >= " + HOUR + " AND ts < " + (2 * HOUR);

    try (final ResultSet rs = database.query("sql", "SELECT sum(v) AS s, count(*) AS n FROM P9351 WHERE " + window)) {
      final Result r = rs.next();
      assertThat(r.<Object>getProperty("s")).as("generic SUM over only absent samples").isNull();
      assertThat(r.<Long>getProperty("n")).isEqualTo(10L);
    }
    try (final ResultSet rs = database.query("sql",
        "SELECT host, sum(v) AS s FROM P9351 WHERE " + window + " GROUP BY host")) {
      assertThat(rs.next().<Object>getProperty("s")).isNull();
    }
    // the window that holds real samples still sums
    try (final ResultSet rs = database.query("sql", "SELECT sum(v) AS s FROM P9351 WHERE ts < " + HOUR)) {
      assertThat(rs.next().<Number>getProperty("s").doubleValue()).isEqualTo(55.0);
    }
  }

  @Test
  void aGroupWithSomeNullsStillSumsTheRest() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Partial9351");
      database.newDocument("Partial9351").set("g", "a").set("x", 5).save();
      database.newDocument("Partial9351").set("g", "a").save();
      database.newDocument("Partial9351").set("g", "b").save();
    });
    try (final ResultSet rs = database.query("sql", "SELECT g, sum(x) AS s FROM Partial9351 GROUP BY g ORDER BY g")) {
      assertThat(rs.next().<Number>getProperty("s").intValue()).isEqualTo(5);
      assertThat(rs.next().<Object>getProperty("s")).as("the all-NULL group").isNull();
    }
  }

  @Test
  void mergingAPartialThatSawNoValueKeepsTheOtherSide() {
    final SQLFunctionSum seen = new SQLFunctionSum();
    seen.execute(null, null, null, new Object[] { 4 }, null);
    final SQLFunctionSum none = new SQLFunctionSum();

    seen.mergePartial(none);
    assertThat(seen.getResult()).isEqualTo(4);

    none.mergePartial(new SQLFunctionSum());
    assertThat(none.getResult()).as("neither side saw a value").isNull();
  }

  @Test
  void sumOverNoValueOnADocumentTypeIsNull() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("D9351");
      for (int k = 0; k < 4; k++)
        database.newDocument("D9351").set("k", k).save();
    });

    try (final ResultSet rs = database.query("sql", "SELECT sum(x) AS s, count(*) AS n FROM D9351 WHERE k > 1000")) {
      assertThat(rs.next().<Object>getProperty("s")).as("empty group").isNull();
    }
    try (final ResultSet rs = database.query("sql", "SELECT sum(x) AS s, count(*) AS n FROM D9351")) {
      final Result r = rs.next();
      assertThat(r.<Object>getProperty("s")).as("all-NULL group").isNull();
      assertThat(r.<Long>getProperty("n")).isEqualTo(4L);
    }
    try (final ResultSet rs = database.query("sql", "SELECT sum(k) AS s FROM D9351")) {
      assertThat(rs.next().<Number>getProperty("s").intValue()).isEqualTo(6);
    }
  }
}
