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
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for issue #3407: OpenCypher PROFILE doesn't give much information.
 * PROFILE should show detailed execution plan, not just "Step-by-step interpretation".
 *
 * @author ArcadeDB Team
 */
class Issue3407Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testopencypher-issue3407").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void profileShouldShowDetailedExecutionPlan() {
    // Setup: Create data similar to the issue description
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("Device");
    database.getSchema().createVertexType("Ping");
    database.getSchema().createEdgeType("OWNS");
    database.getSchema().createEdgeType("GENERATED");

    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (p:Person {name: 'Suspect_1'})\n" +
          "CREATE (d:Device {imei: 'IMEI_1', num: '0612345678'})\n" +
          "CREATE (p)-[:OWNS]->(d)\n" +
          "CREATE (ping:Ping {location: point(48.85, 2.35), time: datetime('2024-01-01T12:00:00')})\n" +
          "CREATE (d)-[:GENERATED]->(ping)");
    });

    // Test PROFILE with a query that uses traditional execution
    // (queries with UNWIND, WITH, or complex patterns often use traditional execution)
    final ResultSet result = database.command("opencypher",
        "PROFILE WITH 5 AS nb_persons " +
        "UNWIND range(1, nb_persons) AS i " +
        "CREATE (p:Person {name: 'Suspect_' + tostring(i)}) " +
        "RETURN count(p) AS created");

    assertThat(result.getExecutionPlan().isPresent()).isTrue();
    final String profile = result.getExecutionPlan().get().prettyPrint(0, 2);

    // Verify basic profile information is present
    assertThat(profile).contains("OpenCypher Query Profile");
    assertThat(profile).contains("Execution Time");
    assertThat(profile).contains("Rows Returned");

    // Verify execution plan section is present
    assertThat(profile).contains("Execution Plan");

    // The key issue: profile should show actual execution steps, not just "Step-by-step interpretation"
    // When traditional execution is used, we should see step details like:
    // - UNWIND
    // - CREATE
    // - RETURN or PROJECT
    // Not just a placeholder message
    if (profile.contains("Traditional")) {
      // Traditional execution should show actual steps
      assertThat(profile)
          .withFailMessage("PROFILE should show detailed execution steps, not just 'Step-by-step interpretation'.\nActual output:\n%s", profile)
          .doesNotContain("Step-by-step interpretation")
          .containsAnyOf("UNWIND", "CREATE", "PROJECT", "RETURN", "WITH", "MATCH");
    }

    // Consume results
    while (result.hasNext()) {
      result.next();
    }
  }

  @Test
  void profileSimpleMatchShouldShowPlan() {
    // Setup: Create simple data
    database.getSchema().createVertexType("Person");

    database.transaction(() -> {
      database.command("opencypher", "CREATE (n:Person {name: 'Alice'})");
      database.command("opencypher", "CREATE (n:Person {name: 'Bob'})");
      database.command("opencypher", "CREATE (n:Person {name: 'Charlie'})");
    });

    // Test PROFILE with simple MATCH query
    final ResultSet result = database.query("opencypher",
        "PROFILE MATCH (n:Person) RETURN n.name");

    assertThat(result.getExecutionPlan().isPresent()).isTrue();
    final String profile = result.getExecutionPlan().get().prettyPrint(0, 2);

    // Verify basic profile information
    assertThat(profile).contains("OpenCypher Query Profile");
    assertThat(profile).contains("Execution Time");
    assertThat(profile).contains("Rows Returned: 3");

    // Verify execution plan shows details
    // Either optimizer plan or traditional plan should show actual steps
    if (profile.contains("Cost-Based Optimizer")) {
      // Optimizer execution should show operator details
      assertThat(profile).containsAnyOf("NodeByLabelScan", "NodeIndexSeek", "Estimated Cost");
    } else {
      // Traditional execution should show step details, not just placeholder text
      assertThat(profile)
          .withFailMessage("PROFILE should show detailed execution steps.\nActual output:\n%s", profile)
          .doesNotContain("Step-by-step interpretation")
          .containsAnyOf("MATCH", "PROJECT", "RETURN");
    }

    // Consume results
    while (result.hasNext()) {
      result.next();
    }
  }

  @Test
  void explainShouldAlsoShowPlan() {
    // EXPLAIN should also show plan details (without executing)
    database.getSchema().createVertexType("Person");

    final ResultSet result = database.query("opencypher",
        "EXPLAIN MATCH (n:Person) RETURN n.name");

    assertThat(result.getExecutionPlan().isPresent()).isTrue();
    final String plan = result.getExecutionPlan().get().prettyPrint(0, 2);

    assertThat(plan).contains("OpenCypher Native Execution Plan");

    // Should show plan details
    if (plan.contains("Cost-Based Optimizer")) {
      assertThat(plan).containsAnyOf("NodeByLabelScan", "Total Estimated Cost");
    } else if (plan.contains("Traditional")) {
      // Even for traditional execution, EXPLAIN should provide some detail
      assertThat(plan).contains("step-by-step interpretation");
    }

    // Consume the explain result
    while (result.hasNext()) {
      result.next();
    }
  }

  @Test
  void profileShowsEnhancedMetrics() {
    // Test that PROFILE shows timing, row counts, and index usage
    database.getSchema().createVertexType("Person");
    database.getSchema().getType("Person").createProperty("name", String.class);
    database.getSchema().getType("Person").createTypeIndex(com.arcadedb.schema.Schema.INDEX_TYPE.LSM_TREE, true, "name");

    database.transaction(() -> {
      for (int i = 0; i < 50; i++) {
        database.command("opencypher", "CREATE (p:Person {name: 'Person_" + i + "'})");
      }
    });

    final ResultSet result = database.query("opencypher",
        "PROFILE MATCH (p:Person) WHERE p.name = 'Person_5' RETURN p.name");

    assertThat(result.getExecutionPlan().isPresent()).isTrue();
    final String profile = result.getExecutionPlan().get().prettyPrint(0, 2);


    // Verify profile contains either timing (traditional) or cost (optimizer)
    assertThat(profile).containsAnyOf("μs", "cost=", "Estimated Cost");

    // Verify index usage is shown
    assertThat(profile).containsAnyOf("[index", "NodeIndexSeek", "IndexSeek");

    while (result.hasNext()) {
      result.next();
    }
  }

  @Test
  void profileShouldShowStepTiming() {
    // Test that PROFILE shows timing information for each step (like SQL engine does)
    database.getSchema().createVertexType("Person");
    database.getSchema().createVertexType("Device");
    database.getSchema().createVertexType("Ping");
    database.getSchema().createEdgeType("OWNS");
    database.getSchema().createEdgeType("GENERATED");

    // Execute a query with PROFILE that exercises multiple steps
    final ResultSet result = database.command("opencypher",
        "PROFILE WITH 10 AS nb_persons " +
        "UNWIND range(1, nb_persons) AS i " +
        "CREATE (p:Person {name: 'Person_' + tostring(i)}) " +
        "WITH p " +
        "CREATE (d:Device {imei: 'IMEI_' + p.name}) " +
        "CREATE (p)-[:OWNS]->(d) " +
        "RETURN count(p) AS created");

    assertThat(result.getExecutionPlan().isPresent()).isTrue();
    final String profile = result.getExecutionPlan().get().prettyPrint(0, 2);


    // Verify basic profile information
    assertThat(profile).contains("OpenCypher Query Profile");
    assertThat(profile).contains("Execution Time");
    assertThat(profile).contains("Rows Returned");

    // The key check: each step should show timing information in μs (microseconds)
    // When traditional execution is used, steps should show (XXXμs) after the step name
    if (profile.contains("Traditional")) {
      // Check that timing appears with steps
      // Format should be something like: "+ WITH ... (123μs, 10 rows)"
      assertThat(profile)
          .withFailMessage("PROFILE should show timing for each step.\nActual output:\n%s", profile)
          .containsPattern("\\+ WITH .*\\([\\d,]+μs(, [\\d,]+ rows)?\\)|\\+ WITH .*\\(\\d+\\.\\d+ms(, [\\d,]+ rows)?\\)");

      // Should also have timing for other steps
      assertThat(profile)
          .withFailMessage("PROFILE should show timing for UNWIND step.\nActual output:\n%s", profile)
          .containsPattern("\\+ UNWIND .*\\([\\d,]+μs(, [\\d,]+ rows)?\\)|\\+ UNWIND .*\\(\\d+\\.\\d+ms(, [\\d,]+ rows)?\\)");

      assertThat(profile)
          .withFailMessage("PROFILE should show timing for CREATE step.\nActual output:\n%s", profile)
          .containsPattern("\\+ CREATE .*\\([\\d,]+μs(, [\\d,]+ rows)?\\)|\\+ CREATE .*\\(\\d+\\.\\d+ms(, [\\d,]+ rows)?\\)");
    }

    // Consume results
    while (result.hasNext()) {
      result.next();
    }
  }
}
