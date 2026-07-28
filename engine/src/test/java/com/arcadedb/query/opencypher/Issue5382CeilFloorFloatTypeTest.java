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

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #5382: {@code ceil()} and {@code floor()} must return FLOAT
 * (Java Double) even when the result is mathematically integral, matching the openCypher
 * return-type contract implemented by Neo4j. Collapsing whole-number results to Long is
 * semantically observable because it silently switches subsequent divisions to integer
 * division ({@code ceil(2.5)/2} returned 1 instead of 1.5).
 * <p>
 * The same FLOAT contract applies to every math function whose Cypher signature declares
 * a FLOAT return (sqrt, exp, log, log10, trig/hyperbolic functions, degrees, radians,
 * atan2), while {@code abs()} is type-preserving (INTEGER→INTEGER, FLOAT→FLOAT) and
 * {@code sign()} returns INTEGER.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5382CeilFloorFloatTypeTest extends TestHelper {

  private Object single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      assertThat(rs.hasNext()).isTrue();
      final Object value = rs.next().getProperty("v");
      assertThat(rs.hasNext()).isFalse();
      return value;
    }
  }

  // ---- reporter's queries ----

  @Test
  void ceilDividedByIntegerUsesFloatDivision() {
    assertThat(single("RETURN ceil(2.5)/2 AS v")).isEqualTo(1.5);
  }

  @Test
  void floorDividedByIntegerUsesFloatDivision() {
    assertThat(single("RETURN floor(3.5)/2 AS v")).isEqualTo(1.5);
  }

  /**
   * Issue #5389 (duplicate of #5382, reported against a Docker image built before the fix
   * landed) listed ceil/floor/cos/exp arithmetic witnesses. Keep them as explicit
   * regressions so a future re-collapse of whole doubles to Long is caught on all four.
   */
  @Test
  void floatContractHoldsInDivisionForAllReportedFunctions() {
    assertThat(single("RETURN ceil(2.1)/2 AS v")).isEqualTo(1.5);
    assertThat(single("RETURN floor(3.5)/2 AS v")).isEqualTo(1.5);
    assertThat(single("RETURN cos(0)/2 AS v")).isEqualTo(0.5);
    assertThat(single("RETURN exp(0)/2 AS v")).isEqualTo(0.5);
    assertThat(single("RETURN ceil(2.1) AS v")).isEqualTo(3.0);
  }

  @Test
  void compositeExpressionKeepsFloatType() {
    // abs(-5) is INTEGER 5, ceil(3.5) is FLOAT 4.0 → 9.0 / 2 = 4.5
    assertThat(single("RETURN (abs(-5) + ceil(3.5)) / 2 AS v")).isEqualTo(4.5);
  }

  // ---- return types of ceil/floor/ceiling ----

  @Test
  void ceilReturnsDouble() {
    assertThat(single("RETURN ceil(2.5) AS v")).isEqualTo(3.0);
    assertThat(single("RETURN ceil(3) AS v")).isEqualTo(3.0);
    assertThat(single("RETURN ceiling(2.5) AS v")).isEqualTo(3.0);
  }

  @Test
  void floorReturnsDouble() {
    assertThat(single("RETURN floor(3.5) AS v")).isEqualTo(3.0);
    assertThat(single("RETURN floor(3) AS v")).isEqualTo(3.0);
  }

  // ---- other FLOAT-contract math functions ----

  @Test
  void sqrtReturnsDouble() {
    assertThat(single("RETURN sqrt(4) AS v")).isEqualTo(2.0);
    assertThat(single("RETURN sqrt(9.0)/2 AS v")).isEqualTo(1.5);
  }

  @Test
  void expLogTrigReturnDouble() {
    assertThat(single("RETURN exp(0) AS v")).isEqualTo(1.0);
    assertThat(single("RETURN log10(100) AS v")).isEqualTo(2.0);
    assertThat(single("RETURN cos(0) AS v")).isEqualTo(1.0);
    assertThat(single("RETURN degrees(0) AS v")).isEqualTo(0.0);
    assertThat(single("RETURN atan2(0, 1) AS v")).isEqualTo(0.0);
  }

  // ---- abs() is type-preserving, sign() returns INTEGER (Neo4j contract) ----

  @Test
  void absPreservesInputType() {
    assertThat(single("RETURN abs(-5) AS v")).isEqualTo(5L);
    assertThat(single("RETURN abs(-5.0) AS v")).isEqualTo(5.0);
    assertThat(single("RETURN abs(-5.5) AS v")).isEqualTo(5.5);
  }

  @Test
  void signStillReturnsInteger() {
    assertThat(single("RETURN sign(-3.5) AS v")).isEqualTo(-1L);
  }

  // ---- large values must not saturate (issue #4391 guard preserved) ----

  @Test
  void largeDoublesStayDoubles() {
    assertThat(single("RETURN floor(1e30) AS v")).isEqualTo(1e30);
    assertThat(single("RETURN ceil(-1e30) AS v")).isEqualTo(-1e30);
  }

  @Test
  void nullInputReturnsNull() {
    assertThat(single("RETURN ceil(null) AS v")).isNull();
    assertThat(single("RETURN floor(null) AS v")).isNull();
    assertThat(single("RETURN abs(null) AS v")).isNull();
  }
}
