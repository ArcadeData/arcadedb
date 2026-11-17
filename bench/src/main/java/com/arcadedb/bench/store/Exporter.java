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
package com.arcadedb.bench.store;

import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;

/**
 * Reads the results store and writes the static series the dashboard draws: {@code index.json}, {@code latest.json} and one
 * {@code series/<workload>/<case>__<scale>__<metric>.json} per series. A series is one (workload, case, scale, metric); inside it
 * every (machineId, variant) pair is its own line, so results of different machines are never joined. The complete tree is
 * built in a sibling directory of {@code outDir} (same filesystem) and then swapped in with two renames: outDir to
 * {@code <outDir>.old-<n>}, the new directory to outDir. If the second rename fails the first one is undone, so the last good tree
 * survives; series that no longer exist are gone after a re-run. Files in {@code outDir} that the exporter does not own are copied
 * into the new tree first. Leftovers of a crashed run are cleaned at the start (a lone {@code .old-*} is restored when outDir is
 * missing, since that is the last good tree).
 */
public final class Exporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(Exporter.class);

  private static final String SERIES = "series";
  private static final String EXPORT = ".export-";
  private static final String OLD    = ".old-";

  /** Test seam: runs between the two renames of the swap; an exception it throws simulates a failing second rename. */
  static volatile Runnable swapFaultHook;

  private Exporter() {
  }

  public static void export(final String url, final String user, final String password, final Path outDir) {
    final Instant generatedAt = Instant.now();
    final JSONObject index = new JSONObject().put("generatedAt", generatedAt.toString());
    final JSONArray latestSeries = new JSONArray();
    final Path target = outDir.toAbsolutePath().normalize();
    final Path tmp = target.resolveSibling(target.getFileName() + EXPORT + System.nanoTime());
    recoverAndClean(target);
    try {
      Files.createDirectories(tmp.resolve(SERIES));
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    try {
      // workload -> "case\0scale" -> metric -> index entry
      final Map<String, Map<String, JSONObject>> workloads = new TreeMap<>();
      final Set<String> usedFiles = new HashSet<>();

      try (final RemoteDatabase db = ResultsStore.open(url, user, password)) {
        final Map<String, RunMeta> runs = loadRuns(db);
        for (final String[] key : loadSeriesKeys(db)) {
          final String workload = key[0];
          final String caseName = key[1];
          final String scale = key[2];
          final String metric = key[3];
          final Series s = loadSeries(db, workload, caseName, scale, metric, runs);
          if (s.lines.isEmpty())
            continue;

          final String dir = sanitize(workload);
          final String base = dir + "/" + sanitize(caseName) + "__" + sanitizeKeepingCase(scale) + "__" + sanitize(metric);
          String candidate = base;
          for (int n = 2; !usedFiles.add(candidate.toLowerCase()); n++)
            candidate = base + "-" + n;
          final String file = SERIES + "/" + candidate + ".json";
          write(tmp.resolve(file), s.toJSON(metric).toString());

          final JSONObject entry = new JSONObject().put("metric", metric).put("unit", (Object) s.unit)
              .put("better", (Object) s.better).put("file", file);
          workloads.computeIfAbsent(workload, w -> new TreeMap<>()).computeIfAbsent(caseName + "\0" + scale, k -> new JSONObject()
              .put("case", caseName).put("scale", scale).put("metrics", new JSONArray())).getJSONArray("metrics").put(entry);
          s.addLatest(latestSeries, workload, caseName, scale, metric);
        }
      }

      final JSONArray workloadArray = new JSONArray();
      for (final Map.Entry<String, Map<String, JSONObject>> w : workloads.entrySet()) {
        final JSONArray cases = new JSONArray();
        for (final JSONObject c : w.getValue().values())
          cases.put(c);
        workloadArray.put(new JSONObject().put("workload", w.getKey()).put("cases", cases));
      }
      index.put("workloads", workloadArray);
      write(tmp.resolve("index.json"), index.toString());
      write(tmp.resolve("latest.json"),
          new JSONObject().put("generatedAt", generatedAt.toString()).put("series", latestSeries).toString());
      copyUnowned(target, tmp);
      swapIn(tmp, target);
    } finally {
      deleteRecursively(tmp);
    }
  }

  /** Name of a path segment: lower case, and only {@code [a-z0-9_-]} survives, everything else (dots, slashes) becomes an underscore. */
  static String sanitize(final String name) {
    return sanitizeKeepingCase(name).toLowerCase(Locale.ROOT);
  }

  /** Same as {@link #sanitize} but the case is kept, for the scale (XS, S, M, L). */
  static String sanitizeKeepingCase(final String name) {
    final StringBuilder b = new StringBuilder(name.length());
    for (int i = 0; i < name.length(); i++) {
      final char c = name.charAt(i);
      final boolean ok = c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_';
      b.append(ok ? c : '_');
    }
    return b.length() == 0 ? "_" : b.toString();
  }

  /** The public charts show scheduled measurements only; manual runs stay in the store. */
  private static final Set<String> PUBLISHED_TRIGGERS = Set.of("nightly", "release");

  /** What a point needs to know about its run: the release tag, the trigger and the bench commit that produced it. */
  private record RunMeta(String tag, String trigger, String benchCommit) {
  }

  private static Map<String, RunMeta> loadRuns(final RemoteDatabase db) {
    final Map<String, RunMeta> runs = new HashMap<>();
    try (final ResultSet rs = db.query("sql",
        "SELECT runId, arcadedb.tag AS tag, `trigger` AS trig, config.benchCommit AS benchCommit FROM Run")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        final String tag = r.getProperty("tag");
        final String runId = r.getProperty("runId");
        if (runId != null)
          runs.put(runId, new RunMeta(tag != null && !tag.isEmpty() ? tag : null, r.getProperty("trig"),
              r.getProperty("benchCommit")));
      }
    }
    return runs;
  }

  private static List<String[]> loadSeriesKeys(final RemoteDatabase db) {
    final List<String[]> keys = new ArrayList<>();
    try (final ResultSet rs = db.query("sql",
        "SELECT workload, `case` AS caseName, scale, metric FROM Measurement GROUP BY workload, `case`, scale, metric")) {
      while (rs.hasNext()) {
        final Result r = rs.next();
        final String workload = r.getProperty("workload");
        final String caseName = r.getProperty("caseName");
        final String scale = r.getProperty("scale");
        final String metric = r.getProperty("metric");
        if (workload != null && caseName != null && scale != null && metric != null)
          keys.add(new String[] { workload, caseName, scale, metric });
      }
    }
    keys.sort(Comparator.<String[], String>comparing(k -> k[0]).thenComparing(k -> k[1]).thenComparing(k -> k[2])
        .thenComparing(k -> k[3]));
    return keys;
  }

  /**
   * One series. The rows of the metric are its points; the {@code _status} rows of the same case (written for a run that
   * produced no metric at all, for example a timeout) join it as points without a median that carry their status, unless the
   * run has a real point of that metric already. Only runs with a published trigger count.
   */
  private static Series loadSeries(final RemoteDatabase db, final String workload, final String caseName, final String scale,
      final String metric, final Map<String, RunMeta> runs) {
    final Series s = new Series();
    final Map<String, Object> params = Map.of("w", workload, "c", caseName, "s", scale, "m", metric, "st", Uploader.STATUS_METRIC);
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = db.query("sql",
        "SELECT runId, machineId, variant, startedAt, `commit` AS commitId, metric, median, status, unit, better FROM Measurement"
            + " WHERE workload = :w AND `case` = :c AND scale = :s AND (metric = :m OR metric = :st) ORDER BY startedAt", params)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    final Set<String> real = new HashSet<>();
    for (final Result r : rows)
      if (metric.equals(r.getProperty("metric")))
        real.add(r.getProperty("runId") + "\0" + r.getProperty("machineId") + "\0" + r.getProperty("variant"));

    for (final Result r : rows) {
      final String runId = r.getProperty("runId");
      final String machineId = r.getProperty("machineId");
      final String variant = r.getProperty("variant");
      final boolean isReal = metric.equals(r.getProperty("metric"));
      if (!isReal && real.contains(runId + "\0" + machineId + "\0" + variant))
        continue;
      final RunMeta meta = runs.get(runId);
      if (meta == null || !PUBLISHED_TRIGGERS.contains(meta.trigger))
        continue;
      final String status = r.getProperty("status");
      final Number median = r.getProperty("median");
      final boolean ok = "ok".equals(status);
      final JSONObject point = new JSONObject().put("t", toIso(r.getProperty("startedAt")))
          .put("commit", (Object) r.getProperty("commitId")).put("tag", (Object) meta.tag)
          .put("benchCommit", (Object) meta.benchCommit)
          .put("median", ok && median != null ? (Object) median.doubleValue() : null).put("status", (Object) status)
          .put("runId", runId);
      final String unit = r.getProperty("unit");
      final String better = r.getProperty("better");
      if (s.unit == null && unit != null)
        s.unit = unit;
      if (s.better == null && better != null)
        s.better = better;
      if (machineId == null || variant == null)
        continue;
      s.lines.computeIfAbsent(machineId + "\0" + variant, k -> new Line(machineId, variant)).points.add(point);
    }
    return s;
  }

  private static String toIso(final Object value) {
    return switch (value) {
      case null -> "";
      case Instant i -> i.toString();
      case LocalDateTime l -> l.toInstant(ZoneOffset.UTC).toString();
      case Date d -> d.toInstant().toString();
      case Number n -> Instant.ofEpochMilli(n.longValue()).toString();
      default -> value.toString();
    };
  }

  private static final class Line {
    final String           machineId;
    final String           variant;
    final List<JSONObject> points = new ArrayList<>();

    Line(final String machineId, final String variant) {
      this.machineId = machineId;
      this.variant = variant;
    }
  }

  private static final class Series {
    String                  unit;
    String                  better;
    final Map<String, Line> lines = new TreeMap<>();

    JSONObject toJSON(final String metric) {
      final JSONArray lineArray = new JSONArray();
      for (final Line l : lines.values()) {
        final JSONArray points = new JSONArray();
        for (final JSONObject p : l.points)
          points.put(p);
        lineArray.put(new JSONObject().put("machineId", l.machineId).put("variant", l.variant).put("points", points));
      }
      return new JSONObject().put("metric", metric).put("unit", (Object) unit).put("better", (Object) better).put("lines", lineArray);
    }

    void addLatest(final JSONArray out, final String workload, final String caseName, final String scale, final String metric) {
      for (final Line l : lines.values()) {
        final int n = l.points.size();
        if (n == 0)
          continue;
        JSONObject previousRelease = null;
        for (int i = n - 2; i >= 0 && previousRelease == null; i--)
          if (!l.points.get(i).isNull("tag"))
            previousRelease = l.points.get(i);
        out.put(new JSONObject().put("workload", workload).put("case", caseName).put("scale", scale).put("metric", metric)
            .put("machineId", l.machineId).put("variant", l.variant).put("last", l.points.get(n - 1))
            .put("previous", (Object) (n > 1 ? l.points.get(n - 2) : null)).put("previousRelease", (Object) previousRelease));
      }
    }
  }

  private static void write(final Path file, final String content) {
    try {
      Files.createDirectories(file.getParent());
      Files.writeString(file, content, StandardCharsets.UTF_8);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Files of outDir that the exporter does not produce travel into the new tree. */
  private static void copyUnowned(final Path target, final Path tmp) {
    if (!Files.isDirectory(target))
      return;
    try (final Stream<Path> children = Files.list(target)) {
      for (final Path child : (Iterable<Path>) children::iterator) {
        final String name = child.getFileName().toString();
        if (name.equals(SERIES) || name.equals("index.json") || name.equals("latest.json"))
          continue;
        try (final Stream<Path> walk = Files.walk(child)) {
          for (final Path from : (Iterable<Path>) walk::iterator)
            Files.copy(from, tmp.resolve(target.relativize(from).toString()), StandardCopyOption.COPY_ATTRIBUTES);
        }
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** outDir becomes {@code tmp}; the previous tree is kept aside until the new one is in place and restored if that fails. */
  private static void swapIn(final Path tmp, final Path target) {
    Path old = null;
    try {
      if (Files.exists(target)) {
        old = target.resolveSibling(target.getFileName() + OLD + System.nanoTime());
        Files.move(target, old, StandardCopyOption.ATOMIC_MOVE);
      }
      try {
        final Runnable hook = swapFaultHook;
        if (hook != null)
          hook.run();
        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE);
      } catch (final IOException | RuntimeException e) {
        if (old != null)
          try {
            Files.move(old, target, StandardCopyOption.ATOMIC_MOVE);
          } catch (final IOException restore) {
            e.addSuppressed(restore);
          }
        throw e;
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    if (old != null)
      deleteRecursively(old);
  }

  /** Removes the {@code .export-*} and {@code .old-*} siblings a crashed run left; a lone {@code .old-*} without outDir is restored. */
  private static void recoverAndClean(final Path target) {
    final Path parent = target.getParent();
    if (parent == null || !Files.isDirectory(parent))
      return;
    final String name = target.getFileName().toString();
    final List<Path> olds = new ArrayList<>();
    final List<Path> leftovers = new ArrayList<>();
    try (final Stream<Path> siblings = Files.list(parent)) {
      for (final Path p : (Iterable<Path>) siblings::iterator) {
        final String n = p.getFileName().toString();
        if (n.startsWith(name + EXPORT))
          leftovers.add(p);
        else if (n.startsWith(name + OLD))
          olds.add(p);
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    olds.sort(Comparator.comparingLong(p -> nanosOf(p, name)));
    if (!Files.exists(target) && !olds.isEmpty()) {
      final Path last = olds.removeLast();
      try {
        Files.move(last, target, StandardCopyOption.ATOMIC_MOVE);
      } catch (final IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    leftovers.addAll(olds);
    leftovers.forEach(Exporter::deleteRecursively);
  }

  /** The numeric suffix of {@code <name>.old-<nanos>}; unparsable suffixes sort first. */
  private static long nanosOf(final Path p, final String name) {
    try {
      return Long.parseLong(p.getFileName().toString().substring(name.length() + OLD.length()));
    } catch (final NumberFormatException e) {
      return Long.MIN_VALUE;
    }
  }

  private static void deleteRecursively(final Path dir) {
    if (!Files.exists(dir))
      return;
    try (final Stream<Path> walk = Files.walk(dir)) {
      walk.sorted(Comparator.reverseOrder()).forEach(p -> {
        try {
          Files.delete(p);
        } catch (final IOException e) {
          LOGGER.warn("cannot delete {}: {}", p, e.getMessage());
        }
      });
    } catch (final IOException e) {
      LOGGER.warn("cannot delete {}: {}", dir, e.getMessage());
    }
  }
}
