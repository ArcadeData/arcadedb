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

import com.arcadedb.Profiler;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public class ServerQueryProfiler {
  private static final int DEFAULT_TIMEOUT_SECONDS = 60;
  private static final int MAX_ENTRIES             = 10_000;
  private static final int MAX_PROFILER_FILES      = 50;

  private final ArcadeDBServer server;

  private volatile boolean recording;
  private long    startTimeMs;
  private long    stopTimeMs;
  private int     timeoutSeconds;
  private Timer   autoStopTimer;

  private final ProfiledQueryEntry[] entries = new ProfiledQueryEntry[MAX_ENTRIES];
  private final AtomicInteger        writeIndex = new AtomicInteger(0);
  private int                        totalRecorded;

  private JSONObject snapshotStartProfiler;
  private JSONObject snapshotStartDatabases;
  private JSONObject snapshotStopProfiler;
  private JSONObject snapshotStopDatabases;

  private JSONObject lastResults;

  public ServerQueryProfiler(final ArcadeDBServer server) {
    this.server = server;
  }

  public boolean isRecording() {
    return recording;
  }

  public void start() {
    start(DEFAULT_TIMEOUT_SECONDS);
  }

  public synchronized void start(final int timeoutSec) {
    if (recording)
      return;

    // Clear previous data
    Arrays.fill(entries, null);
    writeIndex.set(0);
    totalRecorded = 0;
    lastResults = null;
    stopTimeMs = 0;
    timeoutSeconds = timeoutSec > 0 ? timeoutSec : DEFAULT_TIMEOUT_SECONDS;

    // Capture start snapshots
    snapshotStartProfiler = Profiler.INSTANCE.toJSON();
    snapshotStartDatabases = captureDatabaseStats();

    startTimeMs = System.currentTimeMillis();
    recording = true;

    // Schedule auto-stop
    autoStopTimer = new Timer("ProfilerAutoStop", true);
    autoStopTimer.schedule(new TimerTask() {
      @Override
      public void run() {
        LogManager.instance().log(ServerQueryProfiler.this, Level.INFO, "Query profiler auto-stopping after %d seconds", timeoutSeconds);
        stop();
      }
    }, timeoutSeconds * 1000L);

    LogManager.instance().log(this, Level.INFO, "Query profiler started (timeout: %ds)", timeoutSeconds);
  }

  public synchronized JSONObject stop() {
    if (!recording)
      return lastResults;

    recording = false;
    cancelAutoStopTimer();
    stopTimeMs = System.currentTimeMillis();

    // Capture stop snapshots
    snapshotStopProfiler = Profiler.INSTANCE.toJSON();
    snapshotStopDatabases = captureDatabaseStats();

    LogManager.instance().log(this, Level.INFO, "Query profiler stopped (%d queries recorded)", totalRecorded);

    lastResults = buildResults();
    persistResults(lastResults);
    return lastResults;
  }

  public synchronized void reset() {
    recording = false;
    cancelAutoStopTimer();
    Arrays.fill(entries, null);
    writeIndex.set(0);
    totalRecorded = 0;
    startTimeMs = 0;
    stopTimeMs = 0;
    snapshotStartProfiler = null;
    snapshotStartDatabases = null;
    snapshotStopProfiler = null;
    snapshotStopDatabases = null;
    lastResults = null;

    LogManager.instance().log(this, Level.INFO, "Query profiler reset");
  }

  public void recordQuery(final String database, final String language, final String queryText,
      final long executionTimeNanos, final JSONObject executionPlan) {
    if (!recording)
      return;

    final ProfiledQueryEntry entry = new ProfiledQueryEntry(database, language, queryText,
        System.currentTimeMillis(), executionTimeNanos, executionPlan);

    final int idx = writeIndex.getAndIncrement() % MAX_ENTRIES;
    entries[idx] = entry;
    totalRecorded++;
  }

  public synchronized JSONObject getResults() {
    if (lastResults != null)
      return lastResults;
    return buildResults();
  }

  public JSONArray listSavedRuns() {
    final JSONArray result = new JSONArray();
    final File dir = getProfilerDirectory();
    if (!dir.exists())
      return result;

    final File[] files = dir.listFiles((f) -> f.getName().startsWith("profiler-run-") && f.getName().endsWith(".json"));
    if (files == null)
      return result;

    Arrays.sort(files, Comparator.comparing(File::getName).reversed());

    for (final File f : files) {
      final JSONObject entry = new JSONObject();
      entry.put("fileName", f.getName());
      entry.put("size", f.length());
      entry.put("lastModified", f.lastModified());
      result.put(entry);
    }
    return result;
  }

  public JSONObject loadSavedRun(final String fileName) {
    if (fileName.contains("..") || fileName.contains("/") || fileName.contains("\\"))
      throw new IllegalArgumentException("Invalid file name: " + fileName);

    final File file = new File(getProfilerDirectory(), fileName);
    if (!file.exists())
      throw new IllegalArgumentException("Profiler run file not found: " + fileName);

    try {
      final String content = Files.readString(file.toPath());
      return new JSONObject(content);
    } catch (final IOException e) {
      throw new RuntimeException("Error reading profiler run file: " + fileName, e);
    }
  }

  private JSONObject buildResults() {
    final JSONObject result = new JSONObject();
    result.put("recording", recording);
    result.put("startTime", startTimeMs);
    result.put("stopTime", stopTimeMs > 0 ? stopTimeMs : System.currentTimeMillis());
    result.put("durationMs", (stopTimeMs > 0 ? stopTimeMs : System.currentTimeMillis()) - startTimeMs);
    result.put("timeoutSeconds", timeoutSeconds);
    result.put("totalQueries", totalRecorded);

    // Summary with snapshots
    final JSONObject summary = new JSONObject();
    if (snapshotStartProfiler != null) {
      final JSONObject startSnapshot = new JSONObject();
      startSnapshot.put("profiler", snapshotStartProfiler);
      if (snapshotStartDatabases != null)
        startSnapshot.put("databases", snapshotStartDatabases);
      summary.put("snapshotStart", startSnapshot);
    }
    if (snapshotStopProfiler != null) {
      final JSONObject stopSnapshot = new JSONObject();
      stopSnapshot.put("profiler", snapshotStopProfiler);
      if (snapshotStopDatabases != null)
        stopSnapshot.put("databases", snapshotStopDatabases);
      summary.put("snapshotStop", stopSnapshot);
    }
    result.put("summary", summary);

    // Aggregate queries
    result.put("queries", aggregateQueries());

    return result;
  }

  private JSONArray aggregateQueries() {
    final Map<String, List<ProfiledQueryEntry>> groups = new HashMap<>();

    final int count = Math.min(totalRecorded, MAX_ENTRIES);
    for (int i = 0; i < count; i++) {
      final ProfiledQueryEntry entry = entries[i];
      if (entry == null)
        continue;
      final String key = entry.database + "|" + entry.language + "|" + normalizeQuery(entry.queryText);
      groups.computeIfAbsent(key, k -> new ArrayList<>()).add(entry);
    }

    final JSONArray queriesArray = new JSONArray();
    for (final Map.Entry<String, List<ProfiledQueryEntry>> group : groups.entrySet()) {
      final List<ProfiledQueryEntry> entries = group.getValue();
      final ProfiledQueryEntry first = entries.getFirst();

      final JSONObject queryObj = new JSONObject();
      queryObj.put("queryText", first.queryText);
      queryObj.put("language", first.language);
      queryObj.put("database", first.database);
      queryObj.put("executionCount", entries.size());

      // Compute time stats
      final long[] times = new long[entries.size()];
      for (int i = 0; i < entries.size(); i++)
        times[i] = entries.get(i).executionTimeNanos;
      Arrays.sort(times);

      final double totalMs = sumNanos(times) / 1_000_000.0;
      queryObj.put("totalTimeMs", round(totalMs));
      queryObj.put("minTimeMs", round(times[0] / 1_000_000.0));
      queryObj.put("maxTimeMs", round(times[times.length - 1] / 1_000_000.0));
      queryObj.put("avgTimeMs", round(totalMs / times.length));
      queryObj.put("p99TimeMs", round(percentile(times, 99) / 1_000_000.0));

      // Aggregate execution plan steps
      queryObj.put("steps", aggregateSteps(entries));

      queriesArray.put(queryObj);
    }

    // Sort by totalTimeMs descending
    final List<JSONObject> sorted = new ArrayList<>();
    for (int i = 0; i < queriesArray.length(); i++)
      sorted.add(queriesArray.getJSONObject(i));
    sorted.sort((a, b) -> Double.compare(b.getDouble("totalTimeMs"), a.getDouble("totalTimeMs")));

    final JSONArray sortedArray = new JSONArray();
    for (final JSONObject obj : sorted)
      sortedArray.put(obj);
    return sortedArray;
  }

  private JSONArray aggregateSteps(final List<ProfiledQueryEntry> entries) {
    final Map<String, List<Long>> stepCosts = new HashMap<>();

    for (final ProfiledQueryEntry entry : entries) {
      if (entry.executionPlan == null)
        continue;

      // Extract steps from execution plan
      if (entry.executionPlan.has("steps")) {
        final Object stepsObj = entry.executionPlan.get("steps");
        if (stepsObj instanceof JSONArray stepsArray) {
          extractStepCosts(stepsArray, stepCosts);
        }
      }
    }

    final JSONArray result = new JSONArray();
    for (final Map.Entry<String, List<Long>> stepEntry : stepCosts.entrySet()) {
      final List<Long> costs = stepEntry.getValue();
      final long[] costArray = costs.stream().mapToLong(Long::longValue).toArray();
      Arrays.sort(costArray);

      final JSONObject stepObj = new JSONObject();
      stepObj.put("name", stepEntry.getKey());
      stepObj.put("executionCount", costArray.length);

      final double totalCost = sumNanos(costArray) / 1_000_000.0;
      stepObj.put("totalCostMs", round(totalCost));
      stepObj.put("minCostMs", round(costArray[0] / 1_000_000.0));
      stepObj.put("maxCostMs", round(costArray[costArray.length - 1] / 1_000_000.0));
      stepObj.put("avgCostMs", round(totalCost / costArray.length));
      stepObj.put("p99CostMs", round(percentile(costArray, 99) / 1_000_000.0));

      result.put(stepObj);
    }
    return result;
  }

  private void extractStepCosts(final JSONArray stepsArray, final Map<String, List<Long>> stepCosts) {
    for (int i = 0; i < stepsArray.length(); i++) {
      final JSONObject step = stepsArray.getJSONObject(i);
      final String name = step.getString("name", "unknown");
      final long cost = step.getLong("cost", 0);
      stepCosts.computeIfAbsent(name, k -> new ArrayList<>()).add(cost);

      // Recurse into sub-steps
      if (step.has("subSteps")) {
        final Object sub = step.get("subSteps");
        if (sub instanceof JSONArray subArray)
          extractStepCosts(subArray, stepCosts);
      }
    }
  }

  private JSONObject captureDatabaseStats() {
    final JSONObject dbStats = new JSONObject();
    try {
      for (final String dbName : server.getDatabaseNames()) {
        try {
          final Map<String, Object> stats = server.getDatabase(dbName).getStats();
          dbStats.put(dbName, new JSONObject(stats));
        } catch (final Exception e) {
          // Skip databases that can't be accessed
        }
      }
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Error capturing database stats", e);
    }
    return dbStats;
  }

  private void persistResults(final JSONObject results) {
    try {
      final File dir = getProfilerDirectory();
      if (!dir.exists())
        if (!dir.mkdirs())
          return;

      final String timestamp = new SimpleDateFormat("yyyyMMdd-HHmmss").format(new Date());
      final File file = new File(dir, "profiler-run-" + timestamp + ".json");
      Files.writeString(file.toPath(), results.toString(2));

      // Enforce retention
      cleanOldFiles(dir);

      LogManager.instance().log(this, Level.INFO, "Profiler results saved to %s", file.getName());
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.WARNING, "Error saving profiler results to disk", e);
    }
  }

  private void cleanOldFiles(final File dir) {
    final File[] files = dir.listFiles((f) -> f.getName().startsWith("profiler-run-") && f.getName().endsWith(".json"));
    if (files == null || files.length <= MAX_PROFILER_FILES)
      return;

    Arrays.sort(files, Comparator.comparing(File::getName));
    final int toDelete = files.length - MAX_PROFILER_FILES;
    for (int i = 0; i < toDelete; i++)
      files[i].delete();
  }

  private void cancelAutoStopTimer() {
    if (autoStopTimer != null) {
      autoStopTimer.cancel();
      autoStopTimer = null;
    }
  }

  private File getProfilerDirectory() {
    return new File(server.getRootPath() + File.separator + "profiler");
  }

  public static String normalizeQuery(final String query) {
    return query.replaceAll("\\s+", " ").trim();
  }

  private static long sumNanos(final long[] values) {
    long sum = 0;
    for (final long v : values)
      sum += v;
    return sum;
  }

  private static long percentile(final long[] sortedValues, final int percentile) {
    if (sortedValues.length == 1)
      return sortedValues[0];
    final int index = (int) Math.ceil(percentile / 100.0 * sortedValues.length) - 1;
    return sortedValues[Math.min(index, sortedValues.length - 1)];
  }

  private static double round(final double value) {
    return Math.round(value * 100.0) / 100.0;
  }
}
