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
package com.arcadedb.server.monitor;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.FakeArcadeDBServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.ref.WeakReference;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What one recorded query keeps in the profiler's ring buffer. The ring holds a fixed number of entries, so the size of an
 * entry is what bounds the memory a recording can pin: the text of the statement and the plan it ran with are kept in a
 * bounded form, while the figures the results report (executions, timings, per-step costs) stay the same.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class ServerQueryProfilerEntrySizeTest {
  // Deliberately looser than the limits the profiler applies: the test states the order of magnitude an entry may keep,
  // not the exact figure, so tuning a limit does not need a change here.
  private static final int TEXT_LIMIT  = 16 * 1024;
  private static final int NAME_LIMIT  = 1024;
  private static final int STEPS_LIMIT = 1024;

  @TempDir
  Path root;

  private ServerQueryProfiler profiler;

  @BeforeEach
  void startRecording() {
    profiler = new ServerQueryProfiler(FakeArcadeDBServer.create(root, new ContextConfiguration()));
    profiler.start();
  }

  @AfterEach
  void stopRecording() {
    profiler.reset();
  }

  @Test
  void aLongStatementIsKeptInABoundedForm() {
    final String longQuery = "SELECT FROM Person WHERE name = '" + "x".repeat(2_000_000) + "'";

    profiler.recordQuery("testdb", "sql", longQuery, 1_000_000L, null);
    profiler.recordQuery("testdb", "sql", longQuery, 3_000_000L, null);
    profiler.recordQuery("testdb", "sql", "SELECT FROM Person", 2_000_000L, null);

    final JSONArray queries = profiler.getResults().getJSONArray("queries");
    assertThat(queries.length()).isEqualTo(2);

    int longEntries = 0;
    for (int i = 0; i < queries.length(); i++) {
      final JSONObject query = queries.getJSONObject(i);
      if (query.getString("queryText").startsWith("SELECT FROM Person WHERE")) {
        longEntries++;
        assertThat(query.getString("queryText").length()).isLessThanOrEqualTo(TEXT_LIMIT);
        // the same statement run twice is still one row with two executions and their timings
        assertThat(query.getInt("executionCount")).isEqualTo(2);
        assertThat(query.getDouble("totalTimeMs")).isEqualTo(4.0);
        assertThat(query.getDouble("maxTimeMs")).isEqualTo(3.0);
      } else
        assertThat(query.getString("queryText")).isEqualTo("SELECT FROM Person");
    }
    assertThat(longEntries).isEqualTo(1);
  }

  @Test
  void aStatementWithinTheLimitIsKeptAsWritten() {
    final String query = "SELECT FROM Person WHERE name = '" + "y".repeat(2_000) + "'";

    profiler.recordQuery("testdb", "sql", query, 1_000_000L, null);

    final JSONObject recorded = profiler.getResults().getJSONArray("queries").getJSONObject(0);
    assertThat(recorded.getString("queryText")).isEqualTo(query);
  }

  @Test
  void theFullTextOfALongStatementIsNotRetained() throws InterruptedException {
    final WeakReference<String> text = recordLongStatement();

    assertThat(isCollected(text)).as("the ring must not keep the whole statement alive").isTrue();
    assertThat(profiler.getResults().getInt("totalQueries")).isEqualTo(1);
  }

  @Test
  void theFullPlanOfAQueryIsNotRetained() throws InterruptedException {
    final WeakReference<JSONObject> plan = recordLargePlan();

    assertThat(isCollected(plan)).as("the ring must not keep the whole plan alive").isTrue();

    final JSONObject step = findStep(profiler.getResults(), "Scan");
    assertThat(step).as("the per-step figures of the plan are still reported").isNotNull();
    assertThat(step.getInt("executionCount")).isGreaterThan(0);
    assertThat(step.getDouble("totalCostMs")).isEqualTo(step.getInt("executionCount") * 2.0);
  }

  @Test
  void aPlanWithManyStepsReportsAtMostTheFirstOnes() {
    final JSONArray steps = new JSONArray();
    for (int i = 0; i < 20_000; i++)
      steps.put(new JSONObject().put("name", "Scan").put("cost", 2_000_000L).put("description", "d".repeat(100)));

    profiler.recordQuery("testdb", "sql", "SELECT FROM Person", 1_000_000L, new JSONObject().put("steps", steps));

    final JSONObject step = findStep(profiler.getResults(), "Scan");
    assertThat(step).isNotNull();
    assertThat(step.getInt("executionCount")).isBetween(1, STEPS_LIMIT);
    assertThat(step.getInt("measuredCount")).isEqualTo(step.getInt("executionCount"));
  }

  @Test
  void aDeeplyNestedPlanIsAggregatedWithoutDeepRecursion() {
    JSONObject nested = new JSONObject().put("name", "Leaf").put("cost", 1_000_000L);
    for (int i = 0; i < 200_000; i++)
      nested = new JSONObject().put("name", "Level").put("cost", 1_000_000L).put("subSteps", new JSONArray().put(nested));

    profiler.recordQuery("testdb", "sql", "SELECT FROM Person", 1_000_000L,
        new JSONObject().put("steps", new JSONArray().put(nested)));

    final JSONObject results = profiler.getResults();
    final JSONObject level = findStep(results, "Level");
    assertThat(level).isNotNull();
    assertThat(level.getInt("executionCount")).isBetween(1, STEPS_LIMIT);
  }

  @Test
  void theStepsOfAPlanKeepTheirFiguresAcrossNesting() {
    final JSONObject plan = new JSONObject().put("steps", new JSONArray()
        .put(new JSONObject().put("name", "Parent").put("cost", -1L).put("subSteps", new JSONArray()
            .put(new JSONObject().put("name", "Child").put("cost", 4_000_000L).put("subSteps", new JSONArray()
                .put(new JSONObject().put("name", "Grandchild").put("cost", 1_000_000L))))
            .put(new JSONObject().put("name", "Child").put("cost", 2_000_000L))))
        .put(new JSONObject().put("name", "").put("cost", 3_000_000L))
        .put(new JSONObject().put("name", "NoCost")));

    profiler.recordQuery("testdb", "sql", "SELECT FROM Person", 10_000_000L, plan);

    final JSONObject results = profiler.getResults();

    final JSONObject parent = findStep(results, "Parent");
    assertThat(parent.getInt("executionCount")).isEqualTo(1);
    assertThat(parent.getInt("measuredCount")).isZero();

    final JSONObject child = findStep(results, "Child");
    assertThat(child.getInt("executionCount")).isEqualTo(2);
    assertThat(child.getInt("measuredCount")).isEqualTo(2);
    assertThat(child.getDouble("totalCostMs")).isEqualTo(6.0);
    assertThat(child.getDouble("maxCostMs")).isEqualTo(4.0);

    assertThat(findStep(results, "Grandchild").getDouble("totalCostMs")).isEqualTo(1.0);
    assertThat(findStep(results, "unknown").getDouble("totalCostMs")).isEqualTo(3.0);
    assertThat(findStep(results, "NoCost").getInt("measuredCount")).isZero();
  }

  @Test
  void longNamesAreKeptInABoundedForm() {
    final String database = "d".repeat(100_000);
    final String language = "l".repeat(100_000);
    final JSONObject plan = new JSONObject().put("steps", new JSONArray()
        .put(new JSONObject().put("name", "n".repeat(100_000)).put("cost", 1_000_000L)));

    profiler.recordQuery(database, language, "SELECT 1", 1_000_000L, plan);

    final JSONObject results = profiler.getResults();
    final JSONObject query = results.getJSONArray("queries").getJSONObject(0);
    assertThat(query.getString("database").length()).isLessThanOrEqualTo(NAME_LIMIT);
    assertThat(query.getString("language").length()).isLessThanOrEqualTo(NAME_LIMIT);
    assertThat(results.getJSONArray("profiledDatabases").getString(0).length()).isLessThanOrEqualTo(NAME_LIMIT);
    assertThat(query.getJSONArray("steps").getJSONObject(0).getString("name").length()).isLessThanOrEqualTo(NAME_LIMIT);
  }

  // Separate methods so that no local of the test still names the object when the collection is requested
  private WeakReference<String> recordLongStatement() {
    final String text = "SELECT FROM Person WHERE name = '" + "z".repeat(3_000_000) + "'";
    profiler.recordQuery("testdb", "sql", text, 1_000_000L, null);
    return new WeakReference<>(text);
  }

  private WeakReference<JSONObject> recordLargePlan() {
    final JSONArray steps = new JSONArray();
    for (int i = 0; i < 50; i++)
      steps.put(new JSONObject().put("name", "Scan").put("cost", 2_000_000L).put("description", "d".repeat(10_000)));
    final JSONObject plan = new JSONObject().put("prettyPrint", "p".repeat(500_000)).put("steps", steps);

    profiler.recordQuery("testdb", "sql", "SELECT FROM Person", 1_000_000L, plan);
    return new WeakReference<>(plan);
  }

  private static boolean isCollected(final WeakReference<?> reference) throws InterruptedException {
    for (int attempt = 0; attempt < 40 && reference.get() != null; attempt++) {
      System.gc();
      Thread.sleep(50);
    }
    return reference.get() == null;
  }

  private static JSONObject findStep(final JSONObject results, final String name) {
    final JSONArray queries = results.getJSONArray("queries");
    for (int q = 0; q < queries.length(); q++) {
      final JSONArray steps = queries.getJSONObject(q).getJSONArray("steps");
      for (int s = 0; s < steps.length(); s++)
        if (name.equals(steps.getJSONObject(s).getString("name")))
          return steps.getJSONObject(s);
    }
    return null;
  }
}
