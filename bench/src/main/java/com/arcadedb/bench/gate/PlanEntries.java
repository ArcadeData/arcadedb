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

import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The marker entry of one line: the baseline it was reported against ({@code base*}), and its newest measured run ({@code last*},
 * {@code streak}, {@code recovered}). The entry holds everything the issue texts show, so they render from the marker alone.
 */
final class PlanEntries {

  private PlanEntries() {
  }

  /** The entry of a line reported worse by its verdict; {@code added} when it joins an issue already open for its run. */
  static JSONObject reported(final Verdict v, final boolean added) {
    final Point base = v.baseline();
    final JSONObject entry = new JSONObject();
    entry.put("baseRef", base.ref());
    putOrRemove(entry, "baseRunId", base.runId());
    putOrRemove(entry, "baseBench", base.benchCommit());
    entry.put("baseStatus", base.status());
    final JSONArray values = new JSONArray();
    if (!Double.isNaN(base.median())) {
      entry.put("baseMedian", base.median());
      for (final double value : base.values())
        values.put(value);
    }
    entry.put("baseValues", values);
    putOrRemove(entry, "unit", v.newest().unit());
    putOrRemove(entry, "better", v.newest().better());
    if (added)
      entry.put("added", true);
    setLast(entry, v.newest(), Double.isNaN(v.newest().median()) ? Double.NaN : v.change(), v.band(), false, 0);
    return entry;
  }

  /** Follows a tracked line to its newest run: recovered when that run is not worse than the recorded baseline. */
  static void follow(final JSONObject entry, final Point newest, final boolean statusLine) {
    final Point recorded = recordedBaseline(entry, newest.trigger());
    final boolean judged = statusLine || newest.valid();
    final boolean recovered = judged && Rule.compare(recorded, newest) != Verdict.Kind.WORSE;
    final boolean metric = !Double.isNaN(recorded.median()) && !Double.isNaN(newest.median());
    setLast(entry, newest, metric ? Rule.change(recorded, newest) : Double.NaN, metric ? Rule.band(recorded, newest) : Double.NaN, recovered,
        recovered ? entry.getInt("streak", 0) + 1 : 0);
  }

  /** Whether every line of the marker has recovered on enough consecutive runs to close its issue. */
  static boolean allRecovered(final Marker marker) {
    final int needed = IssueText.runsToClose(marker.getTrigger());
    for (final String id : marker.lineIds())
      if (marker.line(id).getInt("streak", 0) < needed)
        return false;
    return !marker.lineIds().isEmpty();
  }

  /**
   * The start of the event's range: the baseline ref most of its lines share, ties broken by the newest baseline point. A line whose own
   * baseline differs shows it in its row.
   */
  static String eventBaseRef(final List<Verdict> worse) {
    final Map<String, Integer> counts = new HashMap<>();
    final Map<String, Point> newestOfRef = new HashMap<>();
    final Comparator<Point> byTime = Comparator.comparing(Point::t).thenComparing(Point::runAt);
    for (final Verdict v : worse) {
      final Point base = v.baseline();
      counts.merge(base.ref(), 1, Integer::sum);
      newestOfRef.merge(base.ref(), base, (a, b) -> byTime.compare(a, b) >= 0 ? a : b);
    }
    String best = null;
    for (final Map.Entry<String, Integer> e : counts.entrySet()) {
      if (best == null)
        best = e.getKey();
      else {
        final int c = Integer.compare(e.getValue(), counts.get(best));
        final int t = byTime.compare(newestOfRef.get(e.getKey()), newestOfRef.get(best));
        if (c > 0 || (c == 0 && (t > 0 || (t == 0 && e.getKey().compareTo(best) > 0))))
          best = e.getKey();
      }
    }
    return best;
  }

  /** The baseline recorded in the entry, rebuilt as a point: a status point when it has no median. */
  static Point recordedBaseline(final JSONObject entry, final String trigger) {
    final boolean metric = entry.has("baseMedian");
    final JSONArray array = entry.getJSONArray("baseValues", new JSONArray());
    final double[] values = new double[metric ? array.length() : 0];
    for (int i = 0; i < values.length; i++)
      values[i] = array.getDouble(i);
    return new Point(trigger, entry.getString("baseRef", ""), null, null, Instant.EPOCH, Instant.EPOCH, entry.getString("baseRunId", null),
        entry.getString("baseBench", null), entry.getString("baseStatus", ""), metric ? entry.getDouble("baseMedian") : Double.NaN, values,
        entry.getString("unit", null), entry.getString("better", null));
  }

  private static void setLast(final JSONObject entry, final Point newest, final double change, final double band, final boolean recovered,
      final int streak) {
    entry.put("lastRun", newest.ref());
    putOrRemove(entry, "lastRunId", newest.runId());
    putOrRemove(entry, "lastBench", newest.benchCommit());
    entry.put("lastStatus", newest.status());
    if (Double.isNaN(newest.median()))
      entry.remove("lastMedian");
    else
      entry.put("lastMedian", newest.median());
    if (Double.isNaN(change)) {
      entry.remove("lastChange");
      entry.remove("lastBand");
    } else {
      entry.put("lastChange", change);
      entry.put("lastBand", band);
    }
    entry.put("streak", streak);
    entry.put("recovered", recovered);
  }

  private static void putOrRemove(final JSONObject entry, final String name, final String value) {
    if (value == null)
      entry.remove(name);
    else
      entry.put(name, value);
  }
}
