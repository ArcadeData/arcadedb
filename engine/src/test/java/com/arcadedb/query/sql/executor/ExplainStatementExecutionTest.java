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

import java.util.*;

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
}
