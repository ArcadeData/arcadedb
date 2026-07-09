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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.logging.Level;
import java.util.regex.Pattern;

public class ServerQueryProfiler {
  private static final int     DEFAULT_TIMEOUT_SECONDS = 60;
  private static final int     MAX_ENTRIES             = 10_000;
  private static final int     MAX_PROFILER_FILES      = 50;
  private static final Pattern WHITESPACE              = Pattern.compile("\\s+");

  private final ArcadeDBServer server;

  private volatile boolean recording;
  private long    startTimeMs;
  private long    stopTimeMs;
  private int     timeoutSeconds;
  private Timer   autoStopTimer;

  // The recording hot path (recordQuery) is intentionally unsynchronized and can run concurrently on
  // many Undertow workers. Visibility to the synchronized readers (stop/buildResults) is established
  // through the AtomicReferenceArray element writes and the AtomicLong counter. writeIndex is a long
  // so the ring-buffer slot (floorMod) never goes negative even after 2^31 records, and it doubles as
  // the exact grand total of recorded queries (one atomic increment per record, no lost updates).
  private final AtomicReferenceArray<ProfiledQueryEntry> entries    = new AtomicReferenceArray<>(MAX_ENTRIES);
  private final AtomicLong                               writeIndex = new AtomicLong(0);

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
    clearEntries();
    writeIndex.set(0);
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

    LogManager.instance().log(this, Level.INFO, "Query profiler stopped (%d queries recorded)", writeIndex.get());

    lastResults = buildResults();
    persistResults(lastResults);
    return lastResults;
  }

  public synchronized void reset() {
    recording = false;
    cancelAutoStopTimer();
    clearEntries();
    writeIndex.set(0);
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
    recordQuery(database, language, queryText, 0L, executionTimeNanos, 0L, executionPlan);
  }

  public void recordQuery(final String database, final String language, final String queryText,
      final long deserializationNanos, final long engineNanos, final long serializationNanos,
      final JSONObject executionPlan) {
    if (!recording)
      return;

    final ProfiledQueryEntry entry = new ProfiledQueryEntry(database, language, queryText,
        System.currentTimeMillis(), deserializationNanos, engineNanos, serializationNanos, executionPlan);

    // floorMod on the long sequence stays non-negative even after the counter passes Integer.MAX_VALUE.
    final int idx = (int) Math.floorMod(writeIndex.getAndIncrement(), MAX_ENTRIES);
    entries.set(idx, entry);
  }

  public void recordQuery(final String database, final String language, final String queryText,
      final QueryProfile profile, final JSONObject executionPlan) {
    if (profile == null) {
      recordQuery(database, language, queryText, 0L, 0L, 0L, executionPlan);
      return;
    }
    recordQuery(database, language, queryText, profile.getDeserializationNanos(), profile.getEngineNanos(),
        profile.getSerializationNanos(), executionPlan);
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

    final File[] files = dir.listFiles(f -> f.getName().startsWith("profiler-run-") && f.getName().endsWith(".json"));
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
    result.put("totalQueries", writeIndex.get());

    // Names of databases that have at least one recorded query in this window.
    // The Studio summary cards use this to scope DB-specific deltas (queries, commands,
    // writeTx, records, ...) to only the databases that were actually exercised, instead
    // of summing across every database registered with the server.
    final JSONArray profiledDbs = new JSONArray();
    for (final String name : collectProfiledDatabaseNames())
      profiledDbs.put(name);
    result.put("profiledDatabases", profiledDbs);

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

  private Set<String> collectProfiledDatabaseNames() {
    final LinkedHashSet<String> names = new LinkedHashSet<>();
    final int count = (int) Math.min(writeIndex.get(), MAX_ENTRIES);
    for (int i = 0; i < count; i++) {
      final ProfiledQueryEntry entry = entries.get(i);
      if (entry != null && entry.database != null)
        names.add(entry.database);
    }
    return names;
  }

  private JSONArray aggregateQueries() {
    final Map<String, List<ProfiledQueryEntry>> groups = new HashMap<>();

    final int count = (int) Math.min(writeIndex.get(), MAX_ENTRIES);
    for (int i = 0; i < count; i++) {
      final ProfiledQueryEntry entry = entries.get(i);
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

      // Compute per-phase and total time stats
      final long[] deserTimes = new long[entries.size()];
      final long[] engineTimes = new long[entries.size()];
      final long[] serTimes = new long[entries.size()];
      final long[] totalTimes = new long[entries.size()];
      for (int i = 0; i < entries.size(); i++) {
        final ProfiledQueryEntry e = entries.get(i);
        deserTimes[i] = e.deserializationNanos;
        engineTimes[i] = e.engineNanos;
        serTimes[i] = e.serializationNanos;
        totalTimes[i] = e.getTotalNanos();
      }
      Arrays.sort(deserTimes);
      Arrays.sort(engineTimes);
      Arrays.sort(serTimes);
      Arrays.sort(totalTimes);

      // Keep totalTimeMs/engineTimeMs backward compatible: "totalTimeMs" is now the end-to-end time
      // (deser+engine+ser) so the Studio query table can sort by slowest request, while the engine
      // breakdown stays available for users who only want the query-engine cost.
      putTimeStats(queryObj, "", totalTimes);
      putTimeStats(queryObj, "engine", engineTimes);
      putTimeStats(queryObj, "deserialization", deserTimes);
      putTimeStats(queryObj, "serialization", serTimes);

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
    final File[] files = dir.listFiles(f -> f.getName().startsWith("profiler-run-") && f.getName().endsWith(".json"));
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

  private static void putTimeStats(final JSONObject queryObj, final String prefix, final long[] sortedNanos) {
    // When prefix is empty we expose totalTimeMs/minTimeMs/... (end-to-end cost across all phases).
    // When prefix is a phase name (engine/deserialization/serialization) we emit engineTimeMs, etc.
    final String totalKey = prefix.isEmpty() ? "totalTimeMs" : prefix + "TotalTimeMs";
    final String minKey = prefix.isEmpty() ? "minTimeMs" : prefix + "MinTimeMs";
    final String maxKey = prefix.isEmpty() ? "maxTimeMs" : prefix + "MaxTimeMs";
    final String avgKey = prefix.isEmpty() ? "avgTimeMs" : prefix + "AvgTimeMs";
    final String p99Key = prefix.isEmpty() ? "p99TimeMs" : prefix + "P99TimeMs";

    final double totalMs = sumNanos(sortedNanos) / 1_000_000.0;
    queryObj.put(totalKey, round(totalMs));
    queryObj.put(minKey, round(sortedNanos[0] / 1_000_000.0));
    queryObj.put(maxKey, round(sortedNanos[sortedNanos.length - 1] / 1_000_000.0));
    queryObj.put(avgKey, round(totalMs / sortedNanos.length));
    queryObj.put(p99Key, round(percentile(sortedNanos, 99) / 1_000_000.0));
  }

  public static String normalizeQuery(final String query) {
    return WHITESPACE.matcher(query).replaceAll(" ").trim();
  }

  private void clearEntries() {
    for (int i = 0; i < MAX_ENTRIES; i++)
      entries.set(i, null);
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
