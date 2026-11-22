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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/** Judges the newest point of each trigger of each line against its baseline. */
final class Judge {

  static final String[] TRIGGERS = { "nightly", "release" };

  private Judge() {
  }

  /** One verdict per (line, trigger) with a newest point, in line key order then nightly before release. */
  static List<Verdict> judge(final Map<LineKey, List<Point>> lines) {
    final List<Verdict> out = new ArrayList<>();
    for (final LineKey key : new TreeSet<>(lines.keySet())) {
      final List<Point> points = lines.get(key);
      final boolean statusLine = LineKey.STATUS.equals(key.metric());
      for (final String trigger : TRIGGERS) {
        final Point newest = newest(points, trigger);
        if (newest == null)
          continue;
        final Point baseline = baselineOf(points, newest, statusLine);
        if (baseline == null || (!statusLine && !newest.valid()))
          out.add(new Verdict(key, trigger, Verdict.Kind.NOT_JUDGED, newest, baseline, 0, 0));
        else
          out.add(new Verdict(key, trigger, Rule.compare(baseline, newest), newest, baseline, Rule.change(baseline, newest),
              statusLine ? 0 : Rule.band(baseline, newest)));
      }
    }
    return out;
  }

  /** The last point of the trigger, or null. */
  static Point newest(final List<Point> points, final String trigger) {
    for (int i = points.size() - 1; i >= 0; i--)
      if (trigger.equals(points.get(i).trigger()))
        return points.get(i);
    return null;
  }

  /**
   * The runs of the trigger measured after {@code lastRun}, oldest first in the judge's order, one point per ref (its last measurement):
   * every run a tracked line has to be followed through, so a worse run between two invocations still resets its streak. Empty when the
   * newest run is {@code lastRun}; only the newest run when {@code lastRun} is not among the points.
   */
  static List<Point> runsAfter(final List<Point> points, final String trigger, final String lastRun) {
    int from = -1;
    for (int i = points.size() - 1; i >= 0; i--)
      if (trigger.equals(points.get(i).trigger()) && points.get(i).ref().equals(lastRun)) {
        from = i;
        break;
      }
    if (from < 0) {
      final Point newest = newest(points, trigger);
      return newest == null ? List.of() : List.of(newest);
    }
    final LinkedHashMap<String, Point> byRef = new LinkedHashMap<>();
    for (int i = from + 1; i < points.size(); i++) {
      final Point p = points.get(i);
      if (!trigger.equals(p.trigger()) || p.ref().equals(lastRun))
        continue;
      byRef.remove(p.ref());
      byRef.put(p.ref(), p);
    }
    return new ArrayList<>(byRef.values());
  }

  /**
   * The nearest earlier point of the same trigger and a different ref: the first valid one for a metric line, whatever its status
   * for the status line. Null if none.
   */
  static Point baselineOf(final List<Point> points, final Point newest, final boolean statusLine) {
    final int idx = points.lastIndexOf(newest);
    for (int i = (idx < 0 ? points.size() : idx) - 1; i >= 0; i--) {
      final Point p = points.get(i);
      if (!p.trigger().equals(newest.trigger()) || p.ref().equals(newest.ref()))
        continue;
      if (statusLine || p.valid())
        return p;
    }
    return null;
  }
}
