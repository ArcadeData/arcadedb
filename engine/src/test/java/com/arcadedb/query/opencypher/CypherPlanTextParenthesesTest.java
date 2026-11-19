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
 * Issue #11820: EXPLAIN and PROFILE print an arithmetic expression from its tree, and the tree has no parentheses:
 * {@code 2.0 * (1.0 - s)} was printed {@code 2.0 * 1.0 - s}, a different expression from the one that runs.
 */
class CypherPlanTextParenthesesTest extends TestHelper {

  @Test
  void anOperandThatBindsLooserKeepsItsParentheses() {
    final String plan;
    try (final ResultSet rs = database.query("opencypher",
        "EXPLAIN UNWIND [0.5] AS s WITH 2.0 * (1.0 - s) AS d, (1 + 2) * 3 AS e, 10 - (4 - 1) AS f, 10 - 4 - 1 AS g, 2 + 3 * 4 AS h "
            + "RETURN d, e, f, g, h")) {
      plan = rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }

    assertThat(plan).contains("2.0 * (1.0 - s) AS d", "(1 + 2) * 3 AS e", "10 - (4 - 1) AS f", "10 - 4 - 1 AS g", "2 + 3 * 4 AS h");
  }

  @Test
  void powerIsLeftAssociativeSoOnlyARightPowerOperandKeepsItsParentheses() {
    final String plan = explain("UNWIND [2] AS s WITH 2 ^ (3 ^ s) AS a, 2 ^ 3 ^ s AS b, 2 * s ^ 2 AS c, (2 * s) ^ 2 AS d RETURN a, b, c, d");

    assertThat(plan).contains("2 ^ (3 ^ s) AS a", "2 ^ 3 ^ s AS b", "2 * s ^ 2 AS c", "(2 * s) ^ 2 AS d");
  }

  @Test
  void concatKeepsParenthesesOnItsRightOperandOnly() {
    final String plan = explain("UNWIND ['x'] AS s WITH 'a' || ('b' || s) AS a, 'a' || 'b' || s AS b RETURN a, b");

    assertThat(plan).contains("'a' || ('b' || s) AS a", "'a' || 'b' || s AS b");
  }

  @Test
  void aUnaryMinusOperandIsPrintedAsOneOperand() {
    final String plan = explain("UNWIND [2] AS s WITH 3 * (-s) AS a, 3 - -s AS b RETURN a, b");

    assertThat(plan).contains("3 * (-s) AS a");
    assertThat(plan).doesNotContain("3 * -s");
  }

  private String explain(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query)) {
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }
}
