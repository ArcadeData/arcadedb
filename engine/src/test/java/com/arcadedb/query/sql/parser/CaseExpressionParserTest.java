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
package com.arcadedb.query.sql.parser;

import org.junit.jupiter.api.Test;

/**
 * Tests for SQL CASE expression parsing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CaseExpressionParserTest extends AbstractParserTest {

  @Test
  void testSearchedCaseBasic() {
    checkRightSyntax("SELECT CASE WHEN x > 0 THEN 'positive' ELSE 'negative' END FROM t");
  }

  @Test
  void testSearchedCaseMultipleWhen() {
    checkRightSyntax("SELECT CASE WHEN x < 0 THEN 'negative' WHEN x = 0 THEN 'zero' ELSE 'positive' END FROM t");
  }

  @Test
  void testSearchedCaseNoElse() {
    checkRightSyntax("SELECT CASE WHEN x > 0 THEN 'positive' END FROM t");
  }

  @Test
  void testSimpleCaseBasic() {
    checkRightSyntax("SELECT CASE color WHEN 1 THEN 'red' WHEN 2 THEN 'blue' ELSE 'unknown' END FROM t");
  }

  @Test
  void testSimpleCaseNoElse() {
    checkRightSyntax("SELECT CASE status WHEN 'active' THEN 1 WHEN 'inactive' THEN 0 END FROM t");
  }

  @Test
  void testCaseInSubquery() {
    // CASE in subquery
    checkRightSyntax("SELECT * FROM (SELECT CASE WHEN x > 0 THEN 'positive' ELSE 'negative' END as status FROM t)");
  }

  @Test
  void testCaseWithAlias() {
    checkRightSyntax("SELECT CASE WHEN x > 0 THEN 'yes' END as result FROM t");
  }

  @Test
  void testNestedCase() {
    checkRightSyntax("SELECT CASE WHEN x > 0 THEN CASE WHEN y > 0 THEN 'both' ELSE 'x only' END END FROM t");
  }

  @Test
  void testCaseWithExpressions() {
    // CASE with arithmetic expressions in THEN clause
    checkRightSyntax("SELECT CASE WHEN x > 10 THEN x + 2 ELSE 0 END FROM t");
  }

  @Test
  void testCaseWithFunctions() {
    // CASE with function call in simple CASE expression
    checkRightSyntax("SELECT CASE name.toUpperCase() WHEN 'JOHN' THEN 'long' ELSE 'short' END FROM t");
  }

  @Test
  void testCaseWithAndOr() {
    checkRightSyntax("SELECT CASE WHEN x > 0 AND y > 0 THEN 'quadrant1' WHEN x < 0 OR y < 0 THEN 'other' END FROM t");
  }

  @Test
  void testCaseMultipleProjections() {
    // Multiple CASE expressions in SELECT
    checkRightSyntax("SELECT CASE WHEN a > 0 THEN 1 END, CASE WHEN b > 0 THEN 2 END FROM t");
  }

  @Test
  void testCaseInLetClause() {
    checkRightSyntax("SELECT $result FROM t LET $result = CASE WHEN x > 0 THEN 'positive' ELSE 'negative' END");
  }

  @Test
  void testCaseLowerCase() {
    checkRightSyntax("select case when x > 0 then 'positive' else 'negative' end from t");
  }

  @Test
  void testCaseMixedCase() {
    checkRightSyntax("Select Case When x > 0 Then 'positive' Else 'negative' End From t");
  }

  @Test
  void testCaseMissingEnd() {
    checkWrongSyntax("SELECT CASE WHEN x > 0 THEN 'yes' FROM t");
  }

  @Test
  void testCaseMissingThen() {
    checkWrongSyntax("SELECT CASE WHEN x > 0 'yes' END FROM t");
  }

  @Test
  void testCaseMissingWhen() {
    checkWrongSyntax("SELECT CASE x > 0 THEN 'yes' END FROM t");
  }

  @Test
  void testCaseEmptyWhenList() {
    checkWrongSyntax("SELECT CASE ELSE 'default' END FROM t");
  }
}
