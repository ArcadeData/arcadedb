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
package com.arcadedb.bench.gate;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Reads the published raw run files into lines of points. It does not go through {@code RunResult.fromJSON}, which rejects the older
 * file shape (no {@code schemaVersion}, {@code config} null, no {@code commitDate}, metrics without {@code values}).
 */
public final class RawData {
  private static final String NIGHTLY = "nightly";
  private static final String RELEASE = "release";

  private static final Pattern RUN_ID_TIME = Pattern.compile("^(\\d{4}-\\d{2}-\\d{2})T(\\d{2})-(\\d{2})-(\\d{2})Z");

  private static final Comparator<Point> BY_TIME = Comparator.comparing(Point::t).thenComparing(Point::runAt);

  private RawData() {
  }

  public static Map<LineKey, List<Point>> read(final Path dataDir) {
    return read(dataDir, System.err);
  }

  /** Reads every run file; a file that cannot be read is skipped with one warning line on {@code warn}, never aborting the rest. */
  public static Map<LineKey, List<Point>> read(final Path dataDir, final PrintStream warn) {
    final Map<LineKey, List<Point>> lines = new TreeMap<>();
    final List<Path> files;
    try (final Stream<Path> walk = Files.walk(dataDir)) {
      files = walk.filter(p -> Files.isRegularFile(p) && p.getFileName().toString().endsWith(".json")).sorted().toList();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    for (final Path file : files) {
      final Map<LineKey, List<Point>> fileLines = new TreeMap<>();
      try {
        readFile(file, fileLines);
      } catch (final RuntimeException | IOException e) {
        warn.println("WARNING: skipping unreadable run file " + file + ": " + e);
        continue;
      }
      for (final Map.Entry<LineKey, List<Point>> e : fileLines.entrySet())
        lines.computeIfAbsent(e.getKey(), k -> new ArrayList<>()).addAll(e.getValue());
    }
    for (final List<Point> points : lines.values())
      points.sort(BY_TIME);
    return lines;
  }

  private static void readFile(final Path file, final Map<LineKey, List<Point>> lines) throws IOException {
    final JSONObject run = new JSONObject(Files.readString(file, StandardCharsets.UTF_8));
    final String trigger = run.getString("trigger", "");
    final JSONObject arcade = objectOrEmpty(run, "arcadedb");
    final String commit = arcade.getString("commit", "");
    final String tag = nullIfEmpty(arcade.getString("tag", null));
    final String ref;
    if (NIGHTLY.equals(trigger)) {
      if (commit.isEmpty())
        throw new IllegalArgumentException("nightly run without arcadedb.commit");
      ref = commit;
    }
    else if (RELEASE.equals(trigger) && tag != null)
      ref = tag;
    else
      return;

    final String fileName = file.getFileName().toString();
    final String runId = run.getString("runId", fileName.substring(0, fileName.length() - ".json".length()));
    final Instant runAt = parseInstant(run.getString("startedAt", null), runId);
    final String commitDate = nullIfEmpty(arcade.getString("commitDate", null));
    final Instant commitInstant = commitDate == null ? null : parseInstant(commitDate, null);
    final Instant t = commitInstant != null ? commitInstant : runAt;
    final String benchCommit = nullIfEmpty(objectOrEmpty(run, "config").getString("benchCommit", null));
    final String machine = objectOrEmpty(run, "machine").getString("id", "");
    final String variant = arcade.getString("variant", "");

    final JSONArray results = run.getJSONArray("results", new JSONArray());
    for (int i = 0; i < results.length(); i++) {
      final JSONObject r = results.getJSONObject(i);
      final String workload = r.getString("workload", "");
      final String caseName = r.getString("case", "");
      final String scale = r.getString("scale", "");
      final String status = r.getString("status", "");
      final boolean hostNotQuiet = objectOrEmpty(r, "notes").has("hostNotQuiet");

      add(lines, new LineKey(machine, variant, scale, workload, caseName, LineKey.STATUS),
          new Point(trigger, ref, commit, tag, t, runAt, runId, benchCommit, status, Double.NaN, new double[0], null, null,
              hostNotQuiet));

      final JSONArray metrics = r.getJSONArray("metrics", new JSONArray());
      for (int m = 0; m < metrics.length(); m++) {
        final JSONObject metric = metrics.getJSONObject(m);
        final Object rawMedian = metric.has("median") ? metric.get("median") : null;
        final double median = rawMedian instanceof Number n ? n.doubleValue() : Double.NaN;
        if (Double.isNaN(median) || Double.isInfinite(median))
          continue;
        final JSONArray vals = metric.getJSONArray("values", new JSONArray());
        final double[] values = new double[vals.length()];
        for (int v = 0; v < values.length; v++)
          values[v] = vals.getDouble(v);
        add(lines, new LineKey(machine, variant, scale, workload, caseName, metric.getString("name", "")),
            new Point(trigger, ref, commit, tag, t, runAt, runId, benchCommit, status, median, values,
                metric.getString("unit", null), metric.getString("better", null), hostNotQuiet));
      }
    }
  }

  private static void add(final Map<LineKey, List<Point>> lines, final LineKey key, final Point point) {
    lines.computeIfAbsent(key, k -> new ArrayList<>()).add(point);
  }

  private static JSONObject objectOrEmpty(final JSONObject parent, final String name) {
    final Object v = parent.has(name) ? parent.get(name) : null;
    return v instanceof JSONObject o ? o : new JSONObject();
  }

  private static String nullIfEmpty(final String s) {
    return s == null || s.isEmpty() ? null : s;
  }

  /** Parses an ISO instant; without one, takes the time from the run id prefix (yyyy-MM-ddTHH-mm-ssZ). */
  private static Instant parseInstant(final String text, final String runId) {
    if (text != null && !text.isEmpty())
      try {
        return Instant.parse(text);
      } catch (final DateTimeParseException e) {
        // fall through to the run id
      }
    if (runId != null) {
      final Matcher m = RUN_ID_TIME.matcher(runId);
      if (m.find())
        return Instant.parse(m.group(1) + "T" + m.group(2) + ":" + m.group(3) + ":" + m.group(4) + "Z");
    }
    return Instant.EPOCH;
  }
}
