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
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * An EXPLAIN builds the step chain of the statement it wraps, and building it evaluates the expressions the steps need to
 * know up front: the {@code SKIP} and {@code LIMIT} values, the top-K bound of an {@code ORDER BY}, those of a {@code WITH},
 * the {@code WHEN} conditions of a conditional query. A call to a function that is not a built-in one (a {@code DEFINE
 * FUNCTION} body can write) is therefore run by the EXPLAIN as it is by the statement, so such a statement is classified
 * and refused as the plain one is, while the EXPLAIN of a statement that calls no such function keeps being answered by
 * every read-only entry point.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class CypherPlanTimeFunctionClassificationTest extends TestHelper {
  private static final String FUNCTION_CALL = "planlib.bump()";

  @BeforeEach
  void defineTheFunction() {
    database.getSchema().createVertexType("PlanItem");
    database.getSchema().createDocumentType("PlanLog");
    // Answers 1 and records that it ran
    database.command("sql", "DEFINE FUNCTION planlib.bump \"BEGIN; INSERT INTO PlanLog SET ran = true; COMMIT; RETURN 1;\" LANGUAGE sql");
  }

  private boolean isIdempotent(final String query) {
    final QueryEngine.AnalyzedQuery analyzed = database.getQueryEngine("opencypher").analyze(query);
    return analyzed.isIdempotent();
  }

  private long storedLogs() {
    try (final ResultSet rs = database.query("sql", "SELECT count() AS c FROM PlanLog")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  @Test
  void explainOfAStatementCallingAFunctionInItsLimitIsNotIdempotent() {
    assertThat(isIdempotent("EXPLAIN MATCH (n) RETURN n LIMIT " + FUNCTION_CALL)).isFalse();
    assertThat(isIdempotent("MATCH (n) RETURN n LIMIT " + FUNCTION_CALL)).isFalse();
  }

  @Test
  void explainOfAStatementCallingAFunctionInItsSkipIsNotIdempotent() {
    assertThat(isIdempotent("EXPLAIN MATCH (n) RETURN n SKIP " + FUNCTION_CALL)).isFalse();
  }

  @Test
  void explainOfAStatementCallingAFunctionInAnOrderedLimitIsNotIdempotent() {
    assertThat(isIdempotent("EXPLAIN MATCH (n) RETURN n ORDER BY n.ran LIMIT " + FUNCTION_CALL)).isFalse();
  }

  @Test
  void explainOfAStatementCallingAFunctionInAWithLimitIsNotIdempotent() {
    assertThat(isIdempotent("EXPLAIN MATCH (n) WITH n LIMIT " + FUNCTION_CALL + " RETURN n")).isFalse();
    assertThat(isIdempotent("EXPLAIN MATCH (n) WITH n SKIP " + FUNCTION_CALL + " RETURN n")).isFalse();
  }

  @Test
  void explainOfAUnionWithAFunctionCallInABranchIsNotIdempotent() {
    assertThat(isIdempotent("EXPLAIN MATCH (n) RETURN n AS x UNION MATCH (m) RETURN m AS x LIMIT " + FUNCTION_CALL)).isFalse();
    assertThat(isIdempotent("EXPLAIN MATCH (n) RETURN n AS x LIMIT " + FUNCTION_CALL + " UNION MATCH (m) RETURN m AS x")).isFalse();
    assertThat(isIdempotent("EXPLAIN MATCH (n) RETURN n AS x LIMIT 1 UNION MATCH (m) RETURN m AS x LIMIT 1")).isTrue();
  }

  @Test
  void explainOfAConditionalQueryCallingAFunctionInAConditionIsNotIdempotent() {
    final String conditional = "WHEN " + FUNCTION_CALL + " = 1 THEN MATCH (n) RETURN n AS x ELSE MATCH (m) RETURN m AS x";
    assertThat(isIdempotent("EXPLAIN " + conditional)).isFalse();
    assertThat(isIdempotent(conditional)).isFalse();
  }

  @Test
  void explainOfAConditionalQueryCallingAFunctionInABranchIsNotIdempotent() {
    assertThat(isIdempotent("EXPLAIN WHEN $p = 1 THEN MATCH (n) RETURN n AS x LIMIT " + FUNCTION_CALL + " ELSE MATCH (m) RETURN m AS x"))
        .isFalse();
  }

  @Test
  void explainOfAStatementWithoutSuchACallStaysIdempotent() {
    assertThat(isIdempotent("EXPLAIN MATCH (n) RETURN n LIMIT 5")).isTrue();
    assertThat(isIdempotent("EXPLAIN MATCH (n) RETURN n LIMIT toInteger('5')")).isTrue();
    assertThat(isIdempotent("EXPLAIN MATCH (n) WITH n LIMIT size([1, 2]) RETURN n")).isTrue();
    assertThat(isIdempotent("EXPLAIN WHEN $p = 1 THEN MATCH (n) RETURN n AS x ELSE MATCH (m) RETURN m AS x")).isTrue();
    // The EXPLAIN of a write only plans it
    assertThat(isIdempotent("EXPLAIN CREATE (:PlanItem {ran: false})")).isTrue();
    assertThat(isIdempotent("EXPLAIN MATCH (n:PlanItem) SET n.ran = true")).isTrue();
    assertThat(isIdempotent("EXPLAIN MATCH (n:PlanItem) DELETE n")).isTrue();
  }

  @Test
  void aQueryOfAnExplainedFunctionCallIsRefusedAndRunsNothing() {
    for (final String query : new String[] {//
        "EXPLAIN MATCH (n) RETURN n LIMIT " + FUNCTION_CALL,//
        "EXPLAIN MATCH (n) RETURN n SKIP " + FUNCTION_CALL,//
        "EXPLAIN MATCH (n) RETURN n ORDER BY n.ran LIMIT " + FUNCTION_CALL,//
        "EXPLAIN MATCH (n) WITH n LIMIT " + FUNCTION_CALL + " RETURN n",//
        "EXPLAIN WHEN " + FUNCTION_CALL + " = 1 THEN MATCH (n) RETURN n AS x ELSE MATCH (m) RETURN m AS x" })
      assertThatThrownBy(() -> database.query("opencypher", query).close()).as(query).isInstanceOf(QueryNotIdempotentException.class);

    assertThat(storedLogs()).isZero();
  }

  @Test
  void aQueryOfAConditionalQueryCallingAFunctionInAConditionIsRefusedAndRunsNothing() {
    assertThatThrownBy(() -> database.query("opencypher",
        "WHEN " + FUNCTION_CALL + " = 1 THEN MATCH (n) RETURN n AS x ELSE MATCH (m) RETURN m AS x").close())
        .isInstanceOf(QueryNotIdempotentException.class);

    assertThat(storedLogs()).isZero();
  }

  @Test
  void anExplainOfAStatementWithoutSuchACallIsStillAnswered() {
    database.command("opencypher", "CREATE (:PlanItem {ran: false})").close();
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN MATCH (n:PlanItem) RETURN n LIMIT 1")) {
      assertThat(rs.getExecutionPlan()).isPresent();
    }
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN CREATE (:PlanItem {ran: false})")) {
      assertThat(rs.getExecutionPlan()).isPresent();
    }
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:PlanItem) RETURN count(n) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(1);
    }
    assertThat(storedLogs()).isZero();
  }
}
