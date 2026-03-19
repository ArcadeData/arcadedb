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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that the profiling execution plan exposes sub-execution plans (subqueries, LET) as nested
 * subSteps in the JSON result so the Studio flamegraph can render them at full depth.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ProfileFlameGraphTest {

  @Test
  void subqueryProfileIncludesInnerPlanSteps() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      db.getSchema().createDocumentType("TestSubQuery");
      db.command("sql", "INSERT INTO TestSubQuery SET name = 'a'");
      db.command("sql", "INSERT INTO TestSubQuery SET name = 'b'");

      final ResultSet result = db.query("sql", "PROFILE SELECT FROM (SELECT FROM TestSubQuery)");

      assertThat(result.getExecutionPlan()).isPresent();
      final ExecutionPlan plan = result.getExecutionPlan().get();
      final Result planResult = plan.toResult();
      final JSONObject planJson = planResult.toJSON();

      final JSONArray steps = planJson.getJSONArray("steps");
      assertThat(steps.length()).isGreaterThan(0);

      // Find the SubQueryStep and verify it has subSteps from the inner plan
      boolean foundSubQueryWithSubSteps = false;
      for (int i = 0; i < steps.length(); i++) {
        final JSONObject step = steps.getJSONObject(i);
        if (step.getString("name").contains("SubQuery")) {
          assertThat(step.has("subSteps")).isTrue();
          final JSONArray subSteps = step.getJSONArray("subSteps");
          assertThat(subSteps.length()).as("SubQueryStep should expose inner plan steps").isGreaterThan(0);
          foundSubQueryWithSubSteps = true;
        }
      }
      assertThat(foundSubQueryWithSubSteps).as("Expected a SubQueryStep with subSteps from inner plan").isTrue();

      result.close();
    });
  }

  @Test
  void selectFromSubqueryLiteralProfileIncludesInnerSteps() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      final ResultSet result = db.query("sql", "PROFILE SELECT FROM (SELECT 1)");

      assertThat(result.getExecutionPlan()).isPresent();
      final ExecutionPlan plan = result.getExecutionPlan().get();
      final Result planResult = plan.toResult();
      final JSONObject planJson = planResult.toJSON();

      final JSONArray steps = planJson.getJSONArray("steps");
      assertThat(steps.length()).isGreaterThan(0);

      // The SubQueryStep should have subSteps
      boolean foundSubQueryWithSubSteps = false;
      for (int i = 0; i < steps.length(); i++) {
        final JSONObject step = steps.getJSONObject(i);
        if (step.getString("name").contains("SubQuery")) {
          assertThat(step.has("subSteps")).isTrue();
          final JSONArray subSteps = step.getJSONArray("subSteps");
          assertThat(subSteps.length()).as("SubQueryStep should have inner steps for SELECT 1").isGreaterThan(0);
          foundSubQueryWithSubSteps = true;
        }
      }
      assertThat(foundSubQueryWithSubSteps).as("Expected a SubQueryStep with subSteps").isTrue();

      result.close();
    });
  }

  @Test
  void letSubqueryProfileIncludesInnerPlanSteps() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      db.getSchema().createDocumentType("TestLet");
      db.command("sql", "INSERT INTO TestLet SET name = 'x'");
      db.command("sql", "INSERT INTO TestLet SET name = 'y'");

      final ResultSet result = db.query("sql", "PROFILE SELECT $x LET $x = (SELECT FROM TestLet LIMIT 1)");

      assertThat(result.getExecutionPlan()).isPresent();
      final ExecutionPlan plan = result.getExecutionPlan().get();
      final Result planResult = plan.toResult();
      final JSONObject planJson = planResult.toJSON();

      final JSONArray steps = planJson.getJSONArray("steps");
      assertThat(steps.length()).isGreaterThan(0);

      // Find the GlobalLetQueryStep and verify it has subSteps from the LET subquery
      boolean foundLetWithSubSteps = false;
      for (int i = 0; i < steps.length(); i++) {
        final JSONObject step = steps.getJSONObject(i);
        if (step.getString("name").contains("GlobalLet") || step.getString("name").contains("LetQuery")) {
          assertThat(step.has("subSteps")).isTrue();
          final JSONArray subSteps = step.getJSONArray("subSteps");
          assertThat(subSteps.length()).as("GlobalLetQueryStep should expose LET subquery plan steps").isGreaterThan(0);
          foundLetWithSubSteps = true;
        }
      }
      assertThat(foundLetWithSubSteps).as("Expected a GlobalLetQueryStep with subSteps from inner plan").isTrue();

      result.close();
    });
  }

  @Test
  void profilePlanStepsCostIsPositive() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      db.getSchema().createDocumentType("TestCost");
      db.command("sql", "INSERT INTO TestCost SET name = 'test'");

      final ResultSet result = db.query("sql", "PROFILE SELECT FROM TestCost");

      assertThat(result.getExecutionPlan()).isPresent();
      final ExecutionPlan plan = result.getExecutionPlan().get();
      final Result planResult = plan.toResult();
      final JSONObject planJson = planResult.toJSON();

      // At least one step should have a positive cost when profiling is enabled
      final JSONArray steps = planJson.getJSONArray("steps");
      boolean hasCost = false;
      for (int i = 0; i < steps.length(); i++) {
        if (steps.getJSONObject(i).getLong("cost") > 0) {
          hasCost = true;
          break;
        }
      }
      assertThat(hasCost).as("At least one step should have positive cost when profiling").isTrue();

      result.close();
    });
  }
}
