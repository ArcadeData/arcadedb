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
package com.arcadedb.gremlin;

import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7408: under {@code $profileExecution} - which {@code ServerDatabase} injects for every statement while the
 * server profiler is recording - a read-only Gremlin traversal ran twice: once for the rows, once more with
 * {@code .profile()} appended to build the plan. The plan now comes from the run the caller drains.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7408GremlinProfileSingleRunTest {
  private static final int PERSONS = 20;

  private ArcadeGraph graph;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-gremlin-7408-profile");
    graph.getDatabase().getSchema().createVertexType("Person");
    graph.getDatabase().transaction(() -> {
      for (int i = 0; i < PERSONS; i++)
        graph.addVertex("Person").property("name", "p" + i);
    });
  }

  @AfterEach
  void teardown() {
    if (graph != null)
      graph.drop();
  }

  /** Records read plus types scanned or iterated: whatever reaches storage, so a second run cannot hide. */
  private long storageReads() {
    final Map<String, Object> stats = graph.getDatabase().getStats();
    return ((Number) stats.get("readRecord")).longValue() + ((Number) stats.get("scanType")).longValue()
        + ((Number) stats.get("iterateType")).longValue();
  }

  private List<Result> drain(final ResultSet rs) {
    final List<Result> rows = new ArrayList<>();
    while (rs.hasNext())
      rows.add(rs.next());
    return rows;
  }

  @Test
  void aProfiledReadReachesStorageExactlyAsOftenAsAnUnprofiledOne() {
    final String query = "g.V().hasLabel('Person').has('name', 'p3')";

    long before = storageReads();
    try (final ResultSet rs = graph.gremlin(query).execute()) {
      assertThat(drain(rs)).hasSize(1);
    }
    final long unprofiled = storageReads() - before;
    assertThat(unprofiled).isPositive();

    before = storageReads();
    try (final ResultSet rs = graph.gremlin(query).setParameters(Map.of("$profileExecution", true)).execute()) {
      assertThat(drain(rs)).hasSize(1);
      assertThat(rs.getExecutionPlan()).isPresent();
    }
    assertThat(storageReads() - before).isEqualTo(unprofiled);
  }

  @Test
  void theRowsAreTheTraversalsOwnNotTheMetrics() {
    try (final ResultSet rs = graph.gremlin("g.V().hasLabel('Person').values('name')")
        .setParameters(Map.of("$profileExecution", true)).execute()) {
      final List<Result> rows = drain(rs);
      assertThat(rows).hasSize(PERSONS);
      for (final Result row : rows)
        assertThat((String) row.getProperty("result")).startsWith("p");
    }
  }

  /** The plan describes the run the caller got: its element counts are the rows the caller consumed. */
  @Test
  void thePlanDescribesTheRunTheCallerConsumed() {
    try (final ResultSet rs = graph.gremlin("g.V().hasLabel('Person')").setParameters(Map.of("$profileExecution", true))
        .execute()) {
      assertThat(drain(rs)).hasSize(PERSONS);

      final ExecutionPlan plan = rs.getExecutionPlan().orElseThrow();
      assertThat(plan.prettyPrint(0, 2)).contains("Traversal Metrics");

      // toResult() used to answer null, so every consumer calling toResult().toJSON() - the server profiler, the
      // HTTP 'explainPlan' - failed on a Gremlin plan
      final JSONObject json = plan.toResult().toJSON();
      assertThat(json.getString("type")).isEqualTo("GremlinExecutionPlan");
      assertThat(json.getLong("cost")).isGreaterThanOrEqualTo(0L);

      final JSONArray steps = json.getJSONArray("steps");
      assertThat(steps.length()).isPositive();
      final JSONObject last = steps.getJSONObject(steps.length() - 1);
      assertThat(last.getLong("elements")).isEqualTo(PERSONS);
      for (int i = 0; i < steps.length(); i++)
        assertThat(steps.getJSONObject(i).getLong("cost")).isGreaterThanOrEqualTo(0L);
    }
  }

  @Test
  void aTraversalThatWasNeverIteratedReportsNoPlan() {
    try (final ResultSet rs = graph.gremlin("g.V().hasLabel('Person')").setParameters(Map.of("$profileExecution", true))
        .execute()) {
      assertThat(rs.getExecutionPlan()).isEmpty();
    }
  }

  /** A statement carrying its own {@code .profile()} gets its metrics as the row, exactly as without the flag. */
  @Test
  void anExplicitProfileStepIsLeftAlone() {
    try (final ResultSet rs = graph.gremlin("g.V().hasLabel('Person').profile()")
        .setParameters(Map.of("$profileExecution", true)).execute()) {
      final List<Result> rows = drain(rs);
      assertThat(rows).hasSize(1);
      assertThat(rows.getFirst().getProperty("result").toString()).contains("Traversal Metrics");
    }
  }

  @Test
  void withoutTheFlagNoPlanIsBuilt() {
    try (final ResultSet rs = graph.gremlin("g.V().hasLabel('Person')").execute()) {
      assertThat(drain(rs)).hasSize(PERSONS);
      assertThat(rs.getExecutionPlan()).isEmpty();
    }
  }
}
