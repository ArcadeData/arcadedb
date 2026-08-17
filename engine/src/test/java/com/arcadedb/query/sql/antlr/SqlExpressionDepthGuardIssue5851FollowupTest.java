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
package com.arcadedb.query.sql.antlr;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Follow-up to issue #5851: unlike the OpenCypher parser, the ANTLR-based SQL parser is not at risk of a
 * {@link StackOverflowError} from deeply nested parentheses - its recursive-descent grammar costs far fewer
 * Java stack frames per nesting level. It is, however, at risk of a worse failure: {@link SQLAntlrParser}
 * resolves the ambiguity between the several grammar rules that all start with a bare '(' (a parenthesized
 * expression, condition, or sub-statement) by trying a fast SLL prediction first and falling back to full
 * ALL(*) prediction on failure. That fallback's cost grows steeply enough with nesting depth that a query of
 * only a few KB can tie up a worker thread for minutes without ever crashing - confirmed here by timing a
 * depth (6000) that previously took well over two minutes of CPU before being interrupted; a legitimate,
 * merely-slow query is not distinguishable from that hang from the outside, which is what makes it worse
 * than a fast StackOverflowError.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SqlExpressionDepthGuardIssue5851FollowupTest {

  @AfterEach
  void resetConfig() {
    GlobalConfiguration.SQL_MAX_EXPRESSION_DEPTH.setValue(GlobalConfiguration.SQL_MAX_EXPRESSION_DEPTH.getDefValue());
  }

  @Test
  void deeplyNestedParenthesesAreRejectedFastRatherThanHanging() {
    final SQLAntlrParser parser = new SQLAntlrParser(null);
    final String sql = "SELECT FROM V WHERE " + "(".repeat(6000) + "1=1" + ")".repeat(6000);

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> parser.parse(sql))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("nest")
        .hasMessageContaining("arcadedb.sql.maxExpressionDepth");

    // Rejected on the O(n) token-stream pre-scan, well before any ANTLR prediction work: previously this
    // exact input burned well over two minutes of CPU before being forcibly interrupted.
    stopwatch.assertGaveUpWithin(5000, "an O(n) token-stream rejection from the two minutes of ANTLR prediction it replaced");
  }

  @Test
  void moderatelyNestedParenthesesStillParseFine() {
    final SQLAntlrParser parser = new SQLAntlrParser(null);
    final String sql = "SELECT FROM V WHERE " + "(".repeat(50) + "1=1" + ")".repeat(50);

    assertThatCode(() -> parser.parse(sql)).doesNotThrowAnyException();
  }

  @Test
  void nestedFunctionCallsAndSubSelectsStillParseFine() {
    final SQLAntlrParser parser = new SQLAntlrParser(null);
    final Statement stmt = parser.parse(
        "SELECT FROM V WHERE ((a = 1) AND (b = 2)) OR (c IN (SELECT FROM V2 WHERE (d > 0)))");
    assertThat(stmt).isNotNull();
  }

  /** A '(' inside a string literal must not count toward the nesting depth. */
  @Test
  void parenthesesInsideAStringLiteralAreNotCounted() {
    final SQLAntlrParser parser = new SQLAntlrParser(null);
    final String literalFullOfParens = "(".repeat(6000);
    final String sql = "SELECT FROM V WHERE name = '" + literalFullOfParens + "'";

    assertThatCode(() -> parser.parse(sql)).doesNotThrowAnyException();
  }

  @Test
  void raisingTheConfiguredLimitAllowsADeeperQueryThatWasPreviouslyRejected() {
    final SQLAntlrParser parser = new SQLAntlrParser(null);
    final String sql = "SELECT FROM V WHERE " + "(".repeat(300) + "1=1" + ")".repeat(300);

    // Rejected at the default (200)
    assertThatThrownBy(() -> parser.parse(sql)).isInstanceOf(CommandSQLParsingException.class);

    // Accepted once the operator raises the knob
    GlobalConfiguration.SQL_MAX_EXPRESSION_DEPTH.setValue(500);
    assertThatCode(() -> parser.parse(sql)).doesNotThrowAnyException();
  }
}
