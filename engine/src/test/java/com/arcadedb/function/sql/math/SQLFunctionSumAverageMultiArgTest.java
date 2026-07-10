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
package com.arcadedb.function.sql.math;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Regression test for issue #4517: a multi-arg per-row {@code sum(a,b,c)} / {@code avg(a,b,c)} call must not
 * reset/pollute the cross-row accumulator held in the function instance, otherwise a subsequent
 * {@link com.arcadedb.function.AggregatedFunction#getResult()} would only return the last row's contribution.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SQLFunctionSumAverageMultiArgTest {

  @Test
  void sumMultiArgDoesNotClobberAccumulator() {
    final SQLFunctionSum f = new SQLFunctionSum();

    // ACCUMULATE ACROSS ROWS (SINGLE-ARG, AGGREGATE SEMANTICS)
    f.execute(null, null, null, new Object[] { 10 }, null);
    f.execute(null, null, null, new Object[] { 20 }, null);
    assertThat(((Number) f.getResult()).intValue()).isEqualTo(30);

    // A PER-ROW MULTI-ARG COMPUTATION MUST RETURN THE ROW SUM...
    final Object rowResult = f.execute(null, null, null, new Object[] { 1, 2, 3 }, null);
    assertThat(((Number) rowResult).intValue()).isEqualTo(6);

    // ...WITHOUT DESTROYING THE PREVIOUSLY ACCUMULATED TOTAL
    assertThat(((Number) f.getResult()).intValue()).isEqualTo(30);
  }

  @Test
  void avgMultiArgDoesNotClobberAccumulator() {
    final SQLFunctionAverage f = new SQLFunctionAverage();

    // ACCUMULATE ACROSS ROWS (SINGLE-ARG, AGGREGATE SEMANTICS): avg(10, 20) = 15
    f.execute(null, null, null, new Object[] { 10 }, null);
    f.execute(null, null, null, new Object[] { 20 }, null);
    assertThat(((Number) f.getResult()).intValue()).isEqualTo(15);

    // A PER-ROW MULTI-ARG COMPUTATION MUST RETURN THE ROW AVERAGE: avg(3, 6, 9) = 6
    final Object rowResult = f.execute(null, null, null, new Object[] { 3, 6, 9 }, null);
    assertThat(((Number) rowResult).intValue()).isEqualTo(6);

    // ...WITHOUT DESTROYING THE PREVIOUSLY ACCUMULATED AVERAGE
    assertThat(((Number) f.getResult()).intValue()).isEqualTo(15);
  }

  @Test
  void perRowMultiArgViaSql() throws Exception {
    TestHelper.executeInNewDatabase("issue-4517", db -> {
      db.command("sql", "CREATE DOCUMENT TYPE Nums");
      db.command("sql", "INSERT INTO Nums SET a = 1, b = 2, c = 3");
      db.command("sql", "INSERT INTO Nums SET a = 10, b = 20, c = 30");
      db.command("sql", "INSERT INTO Nums SET a = 100, b = 200, c = 300");

      // PER-ROW SUM OVER MULTIPLE ARGS
      try (final ResultSet rs = db.query("sql", "SELECT a, sum(a, b, c) AS s FROM Nums ORDER BY a")) {
        assertThat(((Number) rs.next().getProperty("s")).intValue()).isEqualTo(6);
        assertThat(((Number) rs.next().getProperty("s")).intValue()).isEqualTo(60);
        assertThat(((Number) rs.next().getProperty("s")).intValue()).isEqualTo(600);
        assertThat(rs.hasNext()).isFalse();
      }

      // PER-ROW AVG OVER MULTIPLE ARGS
      try (final ResultSet rs = db.query("sql", "SELECT a, avg(a, b, c) AS s FROM Nums ORDER BY a")) {
        assertThat(((Number) rs.next().getProperty("s")).intValue()).isEqualTo(2);
        assertThat(((Number) rs.next().getProperty("s")).intValue()).isEqualTo(20);
        assertThat(((Number) rs.next().getProperty("s")).intValue()).isEqualTo(200);
        assertThat(rs.hasNext()).isFalse();
      }

      // SINGLE-ARG AGGREGATE STILL ACCUMULATES ACROSS ROWS
      try (final ResultSet rs = db.query("sql", "SELECT sum(a) AS s, avg(a) AS av FROM Nums")) {
        final Result r = rs.next();
        assertThat(((Number) r.getProperty("s")).intValue()).isEqualTo(111);
        assertThat(((Number) r.getProperty("av")).intValue()).isEqualTo(37);
      }
    });
  }

  /**
   * Issue #4518: integer/long averages must not truncate, BigDecimal averages must keep fractional digits, and an empty
   * group must return null instead of throwing a division by zero.
   */
  @Test
  void avgDoesNotTruncate() throws Exception {
    TestHelper.executeInNewDatabase("issue-4518", db -> {
      db.command("sql", "CREATE DOCUMENT TYPE Nums");
      db.command("sql", "INSERT INTO Nums SET a = 1");
      db.command("sql", "INSERT INTO Nums SET a = 2");

      // INTEGER AVG MUST RETURN A FRACTIONAL VALUE (NOT TRUNCATED TO 1)
      try (final ResultSet rs = db.query("sql", "SELECT avg(a) AS s FROM Nums")) {
        assertThat(((Number) rs.next().getProperty("s")).doubleValue()).isEqualTo(1.5);
      }

      // PER-ROW MULTI-ARG INTEGER AVG MUST ALSO BE FRACTIONAL
      try (final ResultSet rs = db.query("sql", "SELECT avg(1, 2) AS s")) {
        assertThat(((Number) rs.next().getProperty("s")).doubleValue()).isEqualTo(1.5);
      }

      // EMPTY GROUP RETURNS NO ROW (NO DIVISION BY ZERO)
      try (final ResultSet rs = db.query("sql", "SELECT avg(a) AS s FROM Nums WHERE a > 1000")) {
        assertThat(rs.hasNext()).isFalse();
      }
    });

    // BIGDECIMAL AVG MUST KEEP FRACTIONAL DIGITS (10/3 = 3.333..., NOT 3) AND HANDLE THE EMPTY CASE
    final SQLFunctionAverage f = new SQLFunctionAverage();
    f.execute(null, null, null, new Object[] { new BigDecimal("10") }, null);
    f.execute(null, null, null, new Object[] { new BigDecimal("0") }, null);
    f.execute(null, null, null, new Object[] { new BigDecimal("0") }, null);
    assertThat(((BigDecimal) f.getResult()).doubleValue()).isCloseTo(3.3333333333, within(1e-9));

    assertThat(new SQLFunctionAverage().getResult()).isNull();
  }
}
