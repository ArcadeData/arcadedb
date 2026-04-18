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
package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.monitor.QueryProfile;
import com.arcadedb.server.monitor.ServerQueryProfiler;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

class ServerQueryProfilerTest extends StaticBaseServerTest {
  private ArcadeDBServer server;

  @BeforeEach
  public void beginTest() {
    super.beginTest();

    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
    config.setValue(GlobalConfiguration.SERVER_HTTP_IO_THREADS, 2);
    config.setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 2);

    server = new ArcadeDBServer(config);
    server.start();
  }

  @AfterEach
  public void endTest() {
    if (server != null && server.isStarted())
      server.stop();

    // Clean profiler files
    FileUtils.deleteRecursively(new File("./target/profiler"));

    super.endTest();
  }

  @Test
  void startStopRecording() {
    final ServerQueryProfiler profiler = server.getQueryProfiler();
    assertThat(profiler).isNotNull();
    assertThat(profiler.isRecording()).isFalse();

    profiler.start();
    assertThat(profiler.isRecording()).isTrue();

    final JSONObject results = profiler.stop();
    assertThat(profiler.isRecording()).isFalse();
    assertThat(results).isNotNull();
    assertThat(results.has("recording")).isTrue();
    assertThat(results.getBoolean("recording")).isFalse();
    assertThat(results.has("totalQueries")).isTrue();
    assertThat(results.has("summary")).isTrue();
    assertThat(results.has("queries")).isTrue();
  }

  @Test
  void reset() {
    final ServerQueryProfiler profiler = server.getQueryProfiler();
    profiler.start();
    profiler.recordQuery("testdb", "sql", "SELECT 1", 1_000_000, null);
    profiler.reset();

    assertThat(profiler.isRecording()).isFalse();
    final JSONObject results = profiler.getResults();
    assertThat(results.getInt("totalQueries")).isEqualTo(0);
  }

  @Test
  void recordAndAggregate() {
    final ServerQueryProfiler profiler = server.getQueryProfiler();
    profiler.start();

    // Record the same query multiple times with different execution times
    profiler.recordQuery("testdb", "sql", "SELECT FROM Person", 1_000_000, null);
    profiler.recordQuery("testdb", "sql", "SELECT FROM Person", 2_000_000, null);
    profiler.recordQuery("testdb", "sql", "SELECT FROM Person", 3_000_000, null);

    // Record a different query
    profiler.recordQuery("testdb", "sql", "SELECT FROM Order", 5_000_000, null);

    final JSONObject results = profiler.stop();
    assertThat(results.getInt("totalQueries")).isEqualTo(4);

    final JSONArray queries = results.getJSONArray("queries");
    assertThat(queries.length()).isEqualTo(2);

    // Queries should be sorted by total time descending
    // SELECT FROM Order: 5ms total vs SELECT FROM Person: 6ms total
    final JSONObject first = queries.getJSONObject(0);
    assertThat(first.getString("queryText")).isEqualTo("SELECT FROM Person");
    assertThat(first.getInt("executionCount")).isEqualTo(3);

    final JSONObject second = queries.getJSONObject(1);
    assertThat(second.getString("queryText")).isEqualTo("SELECT FROM Order");
    assertThat(second.getInt("executionCount")).isEqualTo(1);
  }

  @Test
  void snapshotsCapture() {
    final ServerQueryProfiler profiler = server.getQueryProfiler();
    profiler.start();
    final JSONObject results = profiler.stop();

    final JSONObject summary = results.getJSONObject("summary");
    assertThat(summary.has("snapshotStart")).isTrue();
    assertThat(summary.has("snapshotStop")).isTrue();

    final JSONObject startSnapshot = summary.getJSONObject("snapshotStart");
    assertThat(startSnapshot.has("profiler")).isTrue();

    final JSONObject stopSnapshot = summary.getJSONObject("snapshotStop");
    assertThat(stopSnapshot.has("profiler")).isTrue();
  }

  @Test
  void persistenceAndList() {
    final ServerQueryProfiler profiler = server.getQueryProfiler();
    profiler.start();
    profiler.recordQuery("testdb", "sql", "SELECT 1", 1_000_000, null);
    profiler.stop();

    final JSONArray savedRuns = profiler.listSavedRuns();
    assertThat(savedRuns.length()).isGreaterThanOrEqualTo(1);

    final JSONObject firstRun = savedRuns.getJSONObject(0);
    assertThat(firstRun.has("fileName")).isTrue();
    assertThat(firstRun.getString("fileName")).startsWith("profiler-run-");
    assertThat(firstRun.getString("fileName")).endsWith(".json");
  }

  @Test
  void loadSavedRun() {
    final ServerQueryProfiler profiler = server.getQueryProfiler();
    profiler.start();
    profiler.recordQuery("testdb", "sql", "SELECT 1", 1_000_000, null);
    profiler.stop();

    final JSONArray savedRuns = profiler.listSavedRuns();
    assertThat(savedRuns.length()).isGreaterThanOrEqualTo(1);

    final String fileName = savedRuns.getJSONObject(0).getString("fileName");
    final JSONObject loaded = profiler.loadSavedRun(fileName);
    assertThat(loaded).isNotNull();
    assertThat(loaded.has("totalQueries")).isTrue();
    assertThat(loaded.getInt("totalQueries")).isEqualTo(1);
  }

  @Test
  void profilerWithRealQueries() {
    // Create a test database
    server.createDatabase("profiler-test-db", ComponentFile.MODE.READ_WRITE);
    final ServerDatabase db = server.getDatabase("profiler-test-db");

    // Create schema
    db.command("sql", "CREATE VERTEX TYPE Person");
    db.command("sql", "CREATE PROPERTY Person.name STRING");

    // Start profiler
    final ServerQueryProfiler profiler = server.getQueryProfiler();
    profiler.start();

    // Execute some queries — consume results to trigger recording via close()
    try (final ResultSet rs1 = db.command("sql", "INSERT INTO Person SET name = 'Alice'")) {
      while (rs1.hasNext())
        rs1.next();
    }
    try (final ResultSet rs2 = db.command("sql", "INSERT INTO Person SET name = 'Bob'")) {
      while (rs2.hasNext())
        rs2.next();
    }
    try (final ResultSet rs3 = db.query("sql", "SELECT FROM Person")) {
      while (rs3.hasNext())
        rs3.next();
    }

    final JSONObject results = profiler.stop();
    assertThat(results.getInt("totalQueries")).isGreaterThanOrEqualTo(3);

    final JSONArray queries = results.getJSONArray("queries");
    assertThat(queries.length()).isGreaterThanOrEqualTo(2);

    // Verify database stats in snapshots
    final JSONObject summary = results.getJSONObject("summary");
    if (summary.has("snapshotStart")) {
      final JSONObject startSnap = summary.getJSONObject("snapshotStart");
      if (startSnap.has("databases")) {
        final JSONObject dbStats = startSnap.getJSONObject("databases");
        assertThat(dbStats.has("profiler-test-db")).isTrue();
      }
    }

    // Clean up
    db.getEmbedded().drop();
    server.removeDatabase("profiler-test-db");
  }

  @Test
  void recordThreePhasesAndAggregate() {
    final ServerQueryProfiler profiler = server.getQueryProfiler();
    profiler.start();

    // deser=1ms, engine=2ms, ser=0.5ms twice for same query text
    profiler.recordQuery("testdb", "sql", "SELECT FROM Person", 1_000_000L, 2_000_000L, 500_000L, null);
    profiler.recordQuery("testdb", "sql", "SELECT FROM Person", 1_000_000L, 4_000_000L, 500_000L, null);

    final JSONObject results = profiler.stop();
    assertThat(results.getInt("totalQueries")).isEqualTo(2);

    final JSONObject q = results.getJSONArray("queries").getJSONObject(0);
    assertThat(q.getString("queryText")).isEqualTo("SELECT FROM Person");

    // Total time is (deser+engine+ser) per execution, summed
    assertThat(q.getDouble("totalTimeMs")).isEqualTo(9.0);

    // Phase aggregates
    assertThat(q.getDouble("deserializationTotalTimeMs")).isEqualTo(2.0);
    assertThat(q.getDouble("engineTotalTimeMs")).isEqualTo(6.0);
    assertThat(q.getDouble("serializationTotalTimeMs")).isEqualTo(1.0);
    assertThat(q.getDouble("engineAvgTimeMs")).isEqualTo(3.0);
  }

  @Test
  void legacyRecordQueryStoresInEnginePhase() {
    final ServerQueryProfiler profiler = server.getQueryProfiler();
    profiler.start();

    // Legacy 5-argument overload: time is treated as engine nanos, deser/ser stay zero.
    profiler.recordQuery("testdb", "sql", "SELECT 1", 3_000_000L, null);

    final JSONObject results = profiler.stop();
    final JSONObject q = results.getJSONArray("queries").getJSONObject(0);
    assertThat(q.getDouble("totalTimeMs")).isEqualTo(3.0);
    assertThat(q.getDouble("engineTotalTimeMs")).isEqualTo(3.0);
    assertThat(q.getDouble("deserializationTotalTimeMs")).isEqualTo(0.0);
    assertThat(q.getDouble("serializationTotalTimeMs")).isEqualTo(0.0);
  }

  @Test
  void threadLocalQueryProfileCapturesEngineFromProfilingResultSet() {
    server.createDatabase("profiler-thread-db", ComponentFile.MODE.READ_WRITE);
    final ServerDatabase db = server.getDatabase("profiler-thread-db");
    db.command("sql", "CREATE VERTEX TYPE Person");

    final ServerQueryProfiler profiler = server.getQueryProfiler();
    profiler.start();

    // Push a per-request profile on the thread. ProfilingResultSet must not auto-record
    // (the handler owns the record) but it must still populate profile.engineNanos.
    final QueryProfile profile = new QueryProfile();
    QueryProfile.pushCurrent(profile);
    // Use values large enough to survive 2-decimal-place rounding in the results JSON.
    profile.addDeserializationNanos(1_500_000L); // 1.5 ms
    try (final ResultSet rs = db.command("sql", "INSERT INTO Person SET name = 'ThreadLocal'")) {
      while (rs.hasNext())
        rs.next();
    } finally {
      profile.addSerializationNanos(800_000L); // 0.8 ms
      QueryProfile.popCurrent();
    }

    assertThat(profile.getEngineNanos()).isPositive();
    assertThat(profile.getDeserializationNanos()).isEqualTo(1_500_000L);
    assertThat(profile.getSerializationNanos()).isEqualTo(800_000L);

    // ProfilingResultSet deferred recording to the caller, so the profiler must still be empty.
    // The caller then records the full 3-phase entry.
    profiler.recordQuery(db.getName(), "sql", "INSERT INTO Person SET name = 'ThreadLocal'", profile, null);

    final JSONObject results = profiler.stop();
    assertThat(results.getInt("totalQueries")).isEqualTo(1);
    final JSONObject q = results.getJSONArray("queries").getJSONObject(0);
    // Phase aggregates are rounded to two decimals so fast queries can surface as 0.0 in ms
    // while being strictly positive in nanoseconds. The Java-side asserts above cover the nanos.
    assertThat(q.has("deserializationTotalTimeMs")).isTrue();
    assertThat(q.has("engineTotalTimeMs")).isTrue();
    assertThat(q.has("serializationTotalTimeMs")).isTrue();
    assertThat(q.getDouble("deserializationTotalTimeMs")).isGreaterThanOrEqualTo(0.0);
    assertThat(q.getDouble("engineTotalTimeMs")).isGreaterThanOrEqualTo(0.0);
    assertThat(q.getDouble("serializationTotalTimeMs")).isGreaterThanOrEqualTo(0.0);

    db.getEmbedded().drop();
    server.removeDatabase("profiler-thread-db");
  }

  @Test
  void normalizeQuery() {
    assertThat(ServerQueryProfiler.normalizeQuery("SELECT  FROM   Person")).isEqualTo("SELECT FROM Person");
    assertThat(ServerQueryProfiler.normalizeQuery("  SELECT FROM Person  ")).isEqualTo("SELECT FROM Person");
    assertThat(ServerQueryProfiler.normalizeQuery("SELECT\nFROM\tPerson")).isEqualTo("SELECT FROM Person");
  }

  @Test
  void doubleStartIsIdempotent() {
    final ServerQueryProfiler profiler = server.getQueryProfiler();
    profiler.start();
    assertThat(profiler.isRecording()).isTrue();

    // Second start should be a no-op
    profiler.start();
    assertThat(profiler.isRecording()).isTrue();

    profiler.stop();
    assertThat(profiler.isRecording()).isFalse();
  }

  @Test
  void stopWhenNotRecordingReturnsLastResults() {
    final ServerQueryProfiler profiler = server.getQueryProfiler();
    profiler.start();
    profiler.recordQuery("testdb", "sql", "SELECT 1", 1_000_000, null);
    final JSONObject firstStop = profiler.stop();

    // Second stop should return cached results
    final JSONObject secondStop = profiler.stop();
    assertThat(secondStop).isNotNull();
    assertThat(secondStop.getInt("totalQueries")).isEqualTo(firstStop.getInt("totalQueries"));
  }

  @Test
  void autoStopTimeout() throws Exception {
    final ServerQueryProfiler profiler = server.getQueryProfiler();
    profiler.start(2); // 2-second timeout
    assertThat(profiler.isRecording()).isTrue();

    profiler.recordQuery("testdb", "sql", "SELECT 1", 500_000, null);

    // Wait for auto-stop (2s + margin)
    Thread.sleep(3000);

    assertThat(profiler.isRecording()).isFalse();
    final JSONObject results = profiler.getResults();
    assertThat(results).isNotNull();
    assertThat(results.getInt("totalQueries")).isEqualTo(1);
    assertThat(results.getInt("timeoutSeconds")).isEqualTo(2);
  }

  @Test
  void startWithCustomTimeout() {
    final ServerQueryProfiler profiler = server.getQueryProfiler();
    profiler.start(120);
    assertThat(profiler.isRecording()).isTrue();

    final JSONObject results = profiler.stop();
    assertThat(results.getInt("timeoutSeconds")).isEqualTo(120);
  }
}
