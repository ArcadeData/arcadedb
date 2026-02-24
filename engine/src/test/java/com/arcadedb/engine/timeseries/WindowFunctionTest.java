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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class WindowFunctionTest extends TestHelper {

  @Test
  public void testLagBasic() {
    database.command("sql", "CREATE TIMESERIES TYPE LagSensor TIMESTAMP ts FIELDS (temperature DOUBLE)");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO LagSensor SET ts = 1000, temperature = 10.0");
      database.command("sql", "INSERT INTO LagSensor SET ts = 2000, temperature = 20.0");
      database.command("sql", "INSERT INTO LagSensor SET ts = 3000, temperature = 30.0");
      database.command("sql", "INSERT INTO LagSensor SET ts = 4000, temperature = 40.0");
      database.command("sql", "INSERT INTO LagSensor SET ts = 5000, temperature = 50.0");
    });

    final ResultSet rs = database.query("sql", "SELECT ts.lag(temperature, 1, ts) AS prev FROM LagSensor");
    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> prev = (List<Object>) rs.next().getProperty("prev");

    assertThat(prev).hasSize(5);
    assertThat(prev.get(0)).isNull();
    assertThat(prev.get(1)).isEqualTo(10.0);
    assertThat(prev.get(2)).isEqualTo(20.0);
    assertThat(prev.get(3)).isEqualTo(30.0);
    assertThat(prev.get(4)).isEqualTo(40.0);
  }

  @Test
  public void testLagWithOffset2() {
    database.command("sql", "CREATE TIMESERIES TYPE LagOff2 TIMESTAMP ts FIELDS (temperature DOUBLE)");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO LagOff2 SET ts = 1000, temperature = 10.0");
      database.command("sql", "INSERT INTO LagOff2 SET ts = 2000, temperature = 20.0");
      database.command("sql", "INSERT INTO LagOff2 SET ts = 3000, temperature = 30.0");
      database.command("sql", "INSERT INTO LagOff2 SET ts = 4000, temperature = 40.0");
      database.command("sql", "INSERT INTO LagOff2 SET ts = 5000, temperature = 50.0");
    });

    final ResultSet rs = database.query("sql", "SELECT ts.lag(temperature, 2, ts) AS prev FROM LagOff2");
    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> prev = (List<Object>) rs.next().getProperty("prev");

    assertThat(prev).hasSize(5);
    assertThat(prev.get(0)).isNull();
    assertThat(prev.get(1)).isNull();
    assertThat(prev.get(2)).isEqualTo(10.0);
    assertThat(prev.get(3)).isEqualTo(20.0);
    assertThat(prev.get(4)).isEqualTo(30.0);
  }

  @Test
  public void testLagWithDefault() {
    database.command("sql", "CREATE TIMESERIES TYPE LagDef TIMESTAMP ts FIELDS (temperature DOUBLE)");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO LagDef SET ts = 1000, temperature = 10.0");
      database.command("sql", "INSERT INTO LagDef SET ts = 2000, temperature = 20.0");
      database.command("sql", "INSERT INTO LagDef SET ts = 3000, temperature = 30.0");
      database.command("sql", "INSERT INTO LagDef SET ts = 4000, temperature = 40.0");
      database.command("sql", "INSERT INTO LagDef SET ts = 5000, temperature = 50.0");
    });

    final ResultSet rs = database.query("sql", "SELECT ts.lag(temperature, 1, ts, -1) AS prev FROM LagDef");
    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> prev = (List<Object>) rs.next().getProperty("prev");

    assertThat(prev).hasSize(5);
    assertThat(prev.get(0)).isEqualTo(-1);
    assertThat(prev.get(1)).isEqualTo(10.0);
    assertThat(prev.get(2)).isEqualTo(20.0);
    assertThat(prev.get(3)).isEqualTo(30.0);
    assertThat(prev.get(4)).isEqualTo(40.0);
  }

  @Test
  public void testLeadBasic() {
    database.command("sql", "CREATE TIMESERIES TYPE LeadSensor TIMESTAMP ts FIELDS (temperature DOUBLE)");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO LeadSensor SET ts = 1000, temperature = 10.0");
      database.command("sql", "INSERT INTO LeadSensor SET ts = 2000, temperature = 20.0");
      database.command("sql", "INSERT INTO LeadSensor SET ts = 3000, temperature = 30.0");
      database.command("sql", "INSERT INTO LeadSensor SET ts = 4000, temperature = 40.0");
      database.command("sql", "INSERT INTO LeadSensor SET ts = 5000, temperature = 50.0");
    });

    final ResultSet rs = database.query("sql", "SELECT ts.lead(temperature, 1, ts) AS next FROM LeadSensor");
    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> next = (List<Object>) rs.next().getProperty("next");

    assertThat(next).hasSize(5);
    assertThat(next.get(0)).isEqualTo(20.0);
    assertThat(next.get(1)).isEqualTo(30.0);
    assertThat(next.get(2)).isEqualTo(40.0);
    assertThat(next.get(3)).isEqualTo(50.0);
    assertThat(next.get(4)).isNull();
  }

  @Test
  public void testLeadWithDefault() {
    database.command("sql", "CREATE TIMESERIES TYPE LeadDef TIMESTAMP ts FIELDS (temperature DOUBLE)");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO LeadDef SET ts = 1000, temperature = 10.0");
      database.command("sql", "INSERT INTO LeadDef SET ts = 2000, temperature = 20.0");
      database.command("sql", "INSERT INTO LeadDef SET ts = 3000, temperature = 30.0");
      database.command("sql", "INSERT INTO LeadDef SET ts = 4000, temperature = 40.0");
      database.command("sql", "INSERT INTO LeadDef SET ts = 5000, temperature = 50.0");
    });

    final ResultSet rs = database.query("sql", "SELECT ts.lead(temperature, 1, ts, -1) AS next FROM LeadDef");
    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> next = (List<Object>) rs.next().getProperty("next");

    assertThat(next).hasSize(5);
    assertThat(next.get(0)).isEqualTo(20.0);
    assertThat(next.get(1)).isEqualTo(30.0);
    assertThat(next.get(2)).isEqualTo(40.0);
    assertThat(next.get(3)).isEqualTo(50.0);
    assertThat(next.get(4)).isEqualTo(-1);
  }

  @Test
  public void testRowNumber() {
    database.command("sql", "CREATE TIMESERIES TYPE RnSensor TIMESTAMP ts FIELDS (temperature DOUBLE)");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO RnSensor SET ts = 1000, temperature = 10.0");
      database.command("sql", "INSERT INTO RnSensor SET ts = 2000, temperature = 20.0");
      database.command("sql", "INSERT INTO RnSensor SET ts = 3000, temperature = 30.0");
      database.command("sql", "INSERT INTO RnSensor SET ts = 4000, temperature = 40.0");
      database.command("sql", "INSERT INTO RnSensor SET ts = 5000, temperature = 50.0");
    });

    final ResultSet rs = database.query("sql", "SELECT ts.rowNumber(ts) AS rn FROM RnSensor");
    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Integer> rn = (List<Integer>) rs.next().getProperty("rn");

    assertThat(rn).hasSize(5);
    assertThat(rn).containsExactly(1, 2, 3, 4, 5);
  }

  @Test
  public void testRankWithTies() {
    database.command("sql", "CREATE TIMESERIES TYPE RankTies TIMESTAMP ts FIELDS (temperature DOUBLE)");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO RankTies SET ts = 1000, temperature = 10.0");
      database.command("sql", "INSERT INTO RankTies SET ts = 2000, temperature = 20.0");
      database.command("sql", "INSERT INTO RankTies SET ts = 3000, temperature = 20.0");
      database.command("sql", "INSERT INTO RankTies SET ts = 4000, temperature = 30.0");
    });

    final ResultSet rs = database.query("sql", "SELECT ts.rank(temperature, ts) AS rnk FROM RankTies");
    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Integer> rnk = (List<Integer>) rs.next().getProperty("rnk");

    assertThat(rnk).hasSize(4);
    assertThat(rnk).containsExactly(1, 2, 2, 4);
  }

  @Test
  public void testRankNoDuplicates() {
    database.command("sql", "CREATE TIMESERIES TYPE RankUniq TIMESTAMP ts FIELDS (temperature DOUBLE)");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO RankUniq SET ts = 1000, temperature = 10.0");
      database.command("sql", "INSERT INTO RankUniq SET ts = 2000, temperature = 20.0");
      database.command("sql", "INSERT INTO RankUniq SET ts = 3000, temperature = 30.0");
      database.command("sql", "INSERT INTO RankUniq SET ts = 4000, temperature = 40.0");
      database.command("sql", "INSERT INTO RankUniq SET ts = 5000, temperature = 50.0");
    });

    final ResultSet rs = database.query("sql", "SELECT ts.rank(temperature, ts) AS rnk FROM RankUniq");
    assertThat(rs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Integer> rnk = (List<Integer>) rs.next().getProperty("rnk");

    assertThat(rnk).hasSize(5);
    assertThat(rnk).containsExactly(1, 2, 3, 4, 5);
  }

  @Test
  public void testLagWithGroupBy() {
    database.command("sql", "CREATE TIMESERIES TYPE LagBucket TIMESTAMP ts FIELDS (temperature DOUBLE)");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO LagBucket SET ts = 1000, temperature = 10.0");
      database.command("sql", "INSERT INTO LagBucket SET ts = 2000, temperature = 20.0");
      database.command("sql", "INSERT INTO LagBucket SET ts = 3000, temperature = 30.0");
      database.command("sql", "INSERT INTO LagBucket SET ts = 60000, temperature = 40.0");
      database.command("sql", "INSERT INTO LagBucket SET ts = 61000, temperature = 50.0");
      database.command("sql", "INSERT INTO LagBucket SET ts = 62000, temperature = 60.0");
    });

    final ResultSet rs = database.query("sql",
        "SELECT ts.timeBucket('60s', ts) AS tb, ts.lag(temperature, 1, ts) AS prev " +
            "FROM LagBucket GROUP BY tb ORDER BY tb");

    int count = 0;
    while (rs.hasNext()) {
      final Result row = rs.next();
      @SuppressWarnings("unchecked")
      final List<Object> prev = (List<Object>) row.getProperty("prev");
      assertThat(prev).isNotNull();
      assertThat(prev).hasSize(3);
      // First element in each bucket should be null (no previous)
      assertThat(prev.get(0)).isNull();
      count++;
    }
    assertThat(count).isEqualTo(2);
  }

  @Test
  public void testWindowFunctionsOnEmptyType() {
    database.command("sql", "CREATE TIMESERIES TYPE EmptyWin TIMESTAMP ts FIELDS (temperature DOUBLE)");

    final ResultSet lagRs = database.query("sql", "SELECT ts.lag(temperature, 1, ts) AS prev FROM EmptyWin");
    if (lagRs.hasNext()) {
      final Object prev = lagRs.next().getProperty("prev");
      if (prev instanceof List)
        assertThat((List<?>) prev).isEmpty();
    }

    final ResultSet leadRs = database.query("sql", "SELECT ts.lead(temperature, 1, ts) AS next FROM EmptyWin");
    if (leadRs.hasNext()) {
      final Object next = leadRs.next().getProperty("next");
      if (next instanceof List)
        assertThat((List<?>) next).isEmpty();
    }

    final ResultSet rnRs = database.query("sql", "SELECT ts.rowNumber(ts) AS rn FROM EmptyWin");
    if (rnRs.hasNext()) {
      final Object rn = rnRs.next().getProperty("rn");
      if (rn instanceof List)
        assertThat((List<?>) rn).isEmpty();
    }

    final ResultSet rankRs = database.query("sql", "SELECT ts.rank(temperature, ts) AS rnk FROM EmptyWin");
    if (rankRs.hasNext()) {
      final Object rnk = rankRs.next().getProperty("rnk");
      if (rnk instanceof List)
        assertThat((List<?>) rnk).isEmpty();
    }
  }

  @Test
  public void testLagLeadWithTimeSeries() {
    database.command("sql", "CREATE TIMESERIES TYPE TsWinSensor TIMESTAMP ts FIELDS (value DOUBLE)");

    database.transaction(() -> {
      for (int i = 1; i <= 10; i++)
        database.command("sql", "INSERT INTO TsWinSensor SET ts = " + (i * 1000) + ", value = " + (i * 10.0));
    });

    // Verify lag
    final ResultSet lagRs = database.query("sql", "SELECT ts.lag(value, 1, ts) AS prev FROM TsWinSensor");
    assertThat(lagRs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> prev = (List<Object>) lagRs.next().getProperty("prev");
    assertThat(prev).hasSize(10);
    assertThat(prev.get(0)).isNull();
    assertThat(prev.get(1)).isEqualTo(10.0);
    assertThat(prev.get(9)).isEqualTo(90.0);

    // Verify lead
    final ResultSet leadRs = database.query("sql", "SELECT ts.lead(value, 1, ts) AS next FROM TsWinSensor");
    assertThat(leadRs.hasNext()).isTrue();
    @SuppressWarnings("unchecked")
    final List<Object> next = (List<Object>) leadRs.next().getProperty("next");
    assertThat(next).hasSize(10);
    assertThat(next.get(0)).isEqualTo(20.0);
    assertThat(next.get(8)).isEqualTo(100.0);
    assertThat(next.get(9)).isNull();
  }
}
