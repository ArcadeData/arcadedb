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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Regression test for GitHub issue #3416: stDev and stDevP returning the same value.
 * stDev should return the sample standard deviation (divides by n-1),
 * stDevP should return the population standard deviation (divides by n).
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
class OpenCypherStDevTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./databases/test-stdev").create();
    database.getSchema().createVertexType("Val");
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Val {v: 10.0}), (:Val {v: 20.0}), (:Val {v: 30.0})");
    });
  }

  @AfterEach
  void teardown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void testStDevReturnsSampleStandardDeviation() {
    // For [10, 20, 30]: sample stdev = sqrt(((10-20)^2 + (20-20)^2 + (30-20)^2) / 2) = sqrt(200/2) = 10.0
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:Val) RETURN stDev(n.v) as result")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("result")).doubleValue()).isCloseTo(10.0, within(0.001));
    }
  }

  @Test
  void testStDevPReturnsPopulationStandardDeviation() {
    // For [10, 20, 30]: population stdev = sqrt(((10-20)^2 + (20-20)^2 + (30-20)^2) / 3) = sqrt(200/3) ≈ 8.165
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:Val) RETURN stDevP(n.v) as result")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(((Number) row.getProperty("result")).doubleValue()).isCloseTo(8.16496580927726, within(0.001));
    }
  }

  @Test
  void testStDevAndStDevPReturnDifferentValues() {
    // The core of issue #3416: stDev and stDevP must not return the same value
    double stdevResult;
    double stdevpResult;

    try (final ResultSet rs = database.query("opencypher", "MATCH (n:Val) RETURN stDev(n.v) as result")) {
      assertThat(rs.hasNext()).isTrue();
      stdevResult = ((Number) rs.next().getProperty("result")).doubleValue();
    }

    try (final ResultSet rs = database.query("opencypher", "MATCH (n:Val) RETURN stDevP(n.v) as result")) {
      assertThat(rs.hasNext()).isTrue();
      stdevpResult = ((Number) rs.next().getProperty("result")).doubleValue();
    }

    assertThat(stdevResult).isNotEqualTo(stdevpResult);
    assertThat(stdevResult).isGreaterThan(stdevpResult);
  }

  @Test
  void testStDevWithSingleValue() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Val {v: 999.0})");
    });

    // Drop and recreate with single value
    database.drop();
    database = new DatabaseFactory("./databases/test-stdev").create();
    database.getSchema().createVertexType("Val");
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Val {v: 42.0})");
    });

    try (final ResultSet rs = database.query("opencypher", "MATCH (n:Val) RETURN stDev(n.v) as result")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("result")).doubleValue()).isCloseTo(0.0, within(0.001));
    }

    try (final ResultSet rs = database.query("opencypher", "MATCH (n:Val) RETURN stDevP(n.v) as result")) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("result")).doubleValue()).isCloseTo(0.0, within(0.001));
    }
  }
}
