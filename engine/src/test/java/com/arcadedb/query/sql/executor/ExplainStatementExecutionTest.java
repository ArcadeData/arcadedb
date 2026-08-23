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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
class ExplainStatementExecutionTest extends TestHelper {
  public ExplainStatementExecutionTest() {
    autoStartTx = true;
  }

  @Test
  void explainSelectNoTarget() {
    final ResultSet result = database.query("sql", "explain select 1 as one, 2 as two, 2+3");
    assertThat(result.hasNext()).isTrue();
    final Result next = result.next();
    assertThat(next.<ResultInternal>getProperty("executionPlan")).isNotNull();
    assertThat(next.<String>getProperty("executionPlanAsString")).isNotNull();

    final Optional<ExecutionPlan> plan = result.getExecutionPlan();
    assertThat(plan.isPresent()).isTrue();
    assertThat(plan.get() instanceof SelectExecutionPlan).isTrue();

    result.close();
  }

  @Test
  void regressionIssue1488ExplainSubqueryRecursion() {
    // Test for issue #1488: EXPLAIN should recursively resolve subqueries
    database.getSchema().createVertexType("vec", 1);

    // Insert test data
    database.command("sql", "INSERT INTO vec SET x = 1");
    database.command("sql", "INSERT INTO vec SET x = 2");
    database.getSchema().createEdgeType("vecEdge", 1);

    // Execute EXPLAIN on CREATE EDGE with subquery
    final ResultSet result = database.query("sql", "EXPLAIN CREATE EDGE vecEdge FROM (SELECT FROM vec WHERE x = 1) TO (SELECT FROM vec WHERE x = 2)");
    assertThat(result.hasNext()).isTrue();

    final Result next = result.next();
    final String executionPlanAsString = next.getProperty("executionPlanAsString");

    assertThat(executionPlanAsString).isNotNull();

    // The execution plan should contain details about the subqueries, not just the raw SQL
    // It should show steps like "FETCH FROM TYPE" for the SELECT queries
    // and not just "(SELECT FROM vec WHERE x = 1)"
    assertThat(executionPlanAsString).contains("FETCH FROM TYPE vec");
    assertThat(executionPlanAsString).contains("SCAN WITH FILTER BUCKET");

    // Should NOT contain the raw SQL subquery text
    assertThat(executionPlanAsString).doesNotContain("(SELECT FROM vec WHERE x = 1)");

    result.close();
  }

  /**
   * Regression test for issue #6648: {@code EXPLAIN <write statement>} sent through the
   * {@code sqlscript} language engine used to actually perform the wrapped write, because
   * {@code SQLScriptQueryEngine} chains whatever {@code Statement.createExecutionPlan()} returns
   * and pulls it to completion, and {@code ExplainStatement.createExecutionPlan()} used to pass
   * the wrapped statement's own executable plan straight through. Plain {@code "sql"} was never
   * affected, since it goes through {@code ExplainStatement.execute()}, which never pulls the
   * plan it builds.
   */
  @Test
  void explainUpdateInSqlScriptDoesNotExecuteWrite() {
    final String typeName = "Issue6648UpdateTarget";
    database.getSchema().createDocumentType(typeName);
    database.transaction(() -> database.command("sql", "INSERT INTO " + typeName + " SET bar = 0"));

    database.transaction(() -> {
      final ResultSet result = database.command("sqlscript", "EXPLAIN UPDATE " + typeName + " SET bar = 1;");
      assertThat(result.hasNext()).isTrue();
      final Result next = result.next();
      assertThat(next.<Object>getProperty("executionPlan")).isNotNull();
      assertThat(next.<String>getProperty("executionPlanAsString")).isNotNull();
      result.close();
    });

    final ResultSet rs = database.query("sql", "SELECT bar FROM " + typeName);
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Integer>getProperty("bar")).isEqualTo(0);
    rs.close();
  }

  @Test
  void explainInsertInSqlScriptDoesNotExecuteWrite() {
    final String typeName = "Issue6648InsertTarget";
    database.getSchema().createDocumentType(typeName);

    database.transaction(() -> {
      final ResultSet result = database.command("sqlscript", "EXPLAIN INSERT INTO " + typeName + " SET bar = 1;");
      assertThat(result.hasNext()).isTrue();
      result.close();
    });

    final ResultSet rs = database.query("sql", "SELECT count(*) as count FROM " + typeName);
    assertThat(rs.next().<Long>getProperty("count")).isEqualTo(0L);
    rs.close();
  }

  @Test
  void explainDeleteInSqlScriptDoesNotExecuteWrite() {
    final String typeName = "Issue6648DeleteTarget";
    database.getSchema().createDocumentType(typeName);
    database.transaction(() -> database.command("sql", "INSERT INTO " + typeName + " SET bar = 0"));

    database.transaction(() -> {
      final ResultSet result = database.command("sqlscript", "EXPLAIN DELETE FROM " + typeName + ";");
      assertThat(result.hasNext()).isTrue();
      result.close();
    });

    final ResultSet rs = database.query("sql", "SELECT count(*) as count FROM " + typeName);
    assertThat(rs.next().<Long>getProperty("count")).isEqualTo(1L);
    rs.close();
  }

  /**
   * Same bug, reached through a different chaining caller: {@code IfStep} also chains whatever
   * {@code Statement.createExecutionPlan()} returns for the statements in its branch. Confirms the
   * fix lives where it belongs (in {@code ExplainStatement.createExecutionPlan()}), not as a
   * one-off special case in {@code SQLScriptQueryEngine} alone.
   */
  @Test
  void explainUpdateInsideIfBlockDoesNotExecuteWrite() {
    final String typeName = "Issue6648IfBlockTarget";
    database.getSchema().createDocumentType(typeName);
    database.transaction(() -> database.command("sql", "INSERT INTO " + typeName + " SET bar = 0"));

    database.transaction(() -> {
      final String script = """
              IF(true){
                  EXPLAIN UPDATE %s SET bar = 1;
              }
          """.formatted(typeName);
      final ResultSet result = database.command("sqlscript", script);
      result.close();
    });

    final ResultSet rs = database.query("sql", "SELECT bar FROM " + typeName);
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Integer>getProperty("bar")).isEqualTo(0);
    rs.close();
  }

  /**
   * {@code PROFILE EXPLAIN <statement>} is a third way to reach the same bug:
   * {@code ProfileStatement.execute()} is itself a caller that pulls whatever
   * {@code statement.createExecutionPlan()} hands back (see {@code ProfileStatement.java:58-64}), so
   * before this fix {@code PROFILE EXPLAIN UPDATE ...} silently ran the update exactly like the
   * {@code sqlscript} case did. EXPLAIN's non-execution contract is defined to win regardless of what
   * encloses it, so {@code PROFILE EXPLAIN <statement>} degrades to plan-only output - no execution,
   * no real cost numbers - rather than PROFILE unwrapping the inner statement and running it for real.
   */
  @Test
  void profileExplainUpdateDoesNotExecuteWrite() {
    final String typeName = "Issue6648ProfileExplainTarget";
    database.getSchema().createDocumentType(typeName);
    database.transaction(() -> database.command("sql", "INSERT INTO " + typeName + " SET bar = 0"));

    database.transaction(() -> {
      final ResultSet result = database.command("sql", "PROFILE EXPLAIN UPDATE " + typeName + " SET bar = 1");
      assertThat(result.hasNext()).isTrue();
      final Result next = result.next();
      assertThat(next.<Object>getProperty("executionPlan")).isNotNull();
      assertThat(next.<String>getProperty("executionPlanAsString")).isNotNull();
      result.close();
    });

    final ResultSet rs = database.query("sql", "SELECT bar FROM " + typeName);
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Integer>getProperty("bar")).isEqualTo(0);
    rs.close();
  }
}
