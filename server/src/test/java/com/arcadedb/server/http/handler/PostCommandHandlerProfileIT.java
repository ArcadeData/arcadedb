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
package com.arcadedb.server.http.handler;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import java.net.*;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

class PostCommandHandlerProfileIT extends BaseGraphServerTest {

  @Test
  void sqlProfileQueryReturnsExplainPlan() throws Exception {
    final JSONObject response = executeCommandWithProfile("sql", "SELECT FROM " + VERTEX1_TYPE_NAME + " LIMIT 10");

    assertThat(response.has("explain")).isTrue();
    assertThat(response.has("explainPlan")).isTrue();

    final JSONObject plan = response.getJSONObject("explainPlan");
    assertThat(plan.has("type")).isTrue();
    assertThat(plan.has("cost")).isTrue();
    assertThat(plan.has("steps")).isTrue();

    final JSONArray steps = plan.getJSONArray("steps");
    assertThat(steps.length()).isGreaterThan(0);

    final JSONObject firstStep = steps.getJSONObject(0);
    assertThat(firstStep.has("name")).isTrue();
    assertThat(firstStep.has("cost")).isTrue();
    assertThat(firstStep.has("subSteps")).isTrue();
  }

  @Test
  void sqlNonProfileQueryDoesNotIncludeExplainPlan() throws Exception {
    final JSONObject response = executeCommandWithoutProfile("sql", "SELECT FROM " + VERTEX1_TYPE_NAME + " LIMIT 10");

    assertThat(response).isNotNull();
    assertThat(response.has("explainPlan")).isFalse();
    assertThat(response.has("explain")).isFalse();
  }

  @Test
  void openCypherProfileQueryReturnsExplainPlan() throws Exception {
    // Test Studio flow: profileExecution:"detailed" param without PROFILE prefix in query
    final JSONObject response = executeCommandWithProfile("opencypher",
        "MATCH (n:" + VERTEX1_TYPE_NAME + ") RETURN n LIMIT 10");

    assertThat(response.has("explain")).isTrue();
    assertThat(response.has("explainPlan")).isTrue();

    final JSONObject plan = response.getJSONObject("explainPlan");
    assertThat(plan.getString("type")).isEqualTo("OpenCypherExecutionPlan");
    assertThat(plan.has("cost")).isTrue();
    assertThat(plan.has("steps")).isTrue();

    final JSONArray steps = plan.getJSONArray("steps");
    assertThat(steps.length()).isGreaterThan(0);

    final JSONObject firstStep = steps.getJSONObject(0);
    assertThat(firstStep.has("name")).isTrue();
    assertThat(firstStep.has("cost")).isTrue();
  }

  @Test
  void sqlProfileSubqueryIncludesSubSteps() throws Exception {
    final JSONObject response = executeCommandWithProfile("sql", "SELECT FROM (SELECT 1)");

    assertThat(response.has("explainPlan")).isTrue();

    final JSONObject plan = response.getJSONObject("explainPlan");
    final JSONArray steps = plan.getJSONArray("steps");
    assertThat(steps.length()).isGreaterThan(0);

    // Find the SubQueryStep — it should have subSteps from the inner query
    boolean foundSubQueryWithSubSteps = false;
    for (int i = 0; i < steps.length(); i++) {
      final JSONObject step = steps.getJSONObject(i);
      if (step.getString("name").contains("SubQuery")) {
        final JSONArray subSteps = step.getJSONArray("subSteps");
        assertThat(subSteps.length()).as("SubQueryStep should expose inner plan steps").isGreaterThan(0);
        foundSubQueryWithSubSteps = true;
      }
    }
    assertThat(foundSubQueryWithSubSteps).as("Expected a SubQueryStep with subSteps").isTrue();
  }

  @Test
  void sqlProfileLetSubqueryIncludesSubSteps() throws Exception {
    final JSONObject response = executeCommandWithProfile("sql",
        "SELECT $x LET $x = (SELECT FROM " + VERTEX1_TYPE_NAME + " LIMIT 1)");

    assertThat(response.has("explainPlan")).isTrue();

    final JSONObject plan = response.getJSONObject("explainPlan");
    final JSONArray steps = plan.getJSONArray("steps");
    assertThat(steps.length()).isGreaterThan(0);

    // Find the GlobalLetQueryStep — it should have subSteps from the LET subquery
    boolean foundLetWithSubSteps = false;
    for (int i = 0; i < steps.length(); i++) {
      final JSONObject step = steps.getJSONObject(i);
      if (step.getString("name").contains("GlobalLet") || step.getString("name").contains("LetQuery")) {
        final JSONArray subSteps = step.getJSONArray("subSteps");
        assertThat(subSteps.length()).as("GlobalLetQueryStep should expose inner plan steps").isGreaterThan(0);
        foundLetWithSubSteps = true;
      }
    }
    assertThat(foundLetWithSubSteps).as("Expected a GlobalLetQueryStep with subSteps").isTrue();
  }

  @Test
  void openCypherExplainQueryReturnsExplainPlan() throws Exception {
    final JSONObject response = executeCommandWithoutProfile("opencypher",
        "EXPLAIN MATCH (n:" + VERTEX1_TYPE_NAME + ") RETURN n LIMIT 10");

    assertThat(response.has("explain")).isTrue();
    assertThat(response.has("explainPlan")).isTrue();

    final JSONObject plan = response.getJSONObject("explainPlan");
    assertThat(plan.getString("type")).isEqualTo("OpenCypherExecutionPlan");
  }

  private JSONObject executeCommandWithProfile(final String language, final String command) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:2480/api/v1/command/graph").toURL().openConnection();

    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

    final JSONObject payload = new JSONObject();
    payload.put("language", language);
    payload.put("command", command);
    payload.put("serializer", "studio");
    payload.put("profileExecution", "detailed");
    formatPayload(connection, payload);
    connection.connect();

    try {
      final String response = readResponse(connection);
      assertThat(connection.getResponseCode()).isEqualTo(200);
      return new JSONObject(response);
    } finally {
      connection.disconnect();
    }
  }

  private JSONObject executeCommandWithoutProfile(final String language, final String command) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:2480/api/v1/command/graph").toURL().openConnection();

    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

    final JSONObject payload = new JSONObject();
    payload.put("language", language);
    payload.put("command", command);
    payload.put("serializer", "studio");
    formatPayload(connection, payload);
    connection.connect();

    try {
      final String response = readResponse(connection);
      assertThat(connection.getResponseCode()).isEqualTo(200);
      return new JSONObject(response);
    } finally {
      connection.disconnect();
    }
  }
}
