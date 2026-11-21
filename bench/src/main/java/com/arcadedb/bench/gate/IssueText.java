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

import com.arcadedb.serializer.json.JSONObject;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * The texts of a regression issue: its title, its body and the comment of each run. Each is a pure function of the marker (and of the
 * bench-change answers), so re-rendering the same state gives the same bytes.
 */
public final class IssueText {
  static final String NIGHTLY = "nightly";

  private static final String RUN_MARKER_PREFIX = "<!-- perf-regression-run ";
  private static final String DASHBOARD         = "https://perf.arcadedb.com/workload.html?w=";
  private static final String RAW               = "https://github.com/ArcadeData/arcadedb-perf/blob/main/data/";
  private static final String COMPARE           = "https://github.com/ArcadeData/arcadedb/compare/";

  static final String BENCH_CHANGED = "the bench changed between these runs: check it first";
  static final String BENCH_UNKNOWN = "bench change unknown";

  private IssueText() {
  }

  /** Recovered runs a line needs before the issue closes: 2 nightly runs, 1 later release. */
  static int runsToClose(final String trigger) {
    return NIGHTLY.equals(trigger) ? 2 : 1;
  }

  public static String title(final Marker m) {
    final int n = m.lineIds().size();
    if (NIGHTLY.equals(m.getTrigger()))
      return "Performance regression: " + n + " lines worse between " + shortRef(m.getTrigger(), m.getBaseRef()) + ".." + shortRef(
          m.getTrigger(), m.getRef());
    return "Performance regression: " + n + " lines worse in release " + m.getRef() + " vs " + m.getBaseRef();
  }

  public static String body(final Marker m, final BenchChanges bench) {
    final boolean nightly = NIGHTLY.equals(m.getTrigger());
    final String base = m.getBaseRef();
    final String ref = m.getRef();
    final StringBuilder sb = new StringBuilder();
    if (nightly)
      sb.append("Lines worse in the nightly run of `").append(shortRef(m.getTrigger(), ref)).append("` than in the previous nightly run of `")
          .append(shortRef(m.getTrigger(), base)).append("`");
    else
      sb.append("Lines worse in release `").append(ref).append("` than in release `").append(base).append("`");
    sb.append(": [").append(shortRef(m.getTrigger(), base)).append("...").append(shortRef(m.getTrigger(), ref)).append("](").append(COMPARE)
        .append(base).append("...").append(ref).append(")\n\n");

    sb.append("| workload | case | scale | variant | metric | before | after | change | band |\n");
    sb.append("|---|---|---|---|---|---|---|---|---|\n");
    boolean anyChanged = false;
    boolean anyUnknown = false;
    for (final String id : sortedIds(m)) {
      final JSONObject e = m.line(id);
      final String[] parts = parts(id);
      sb.append("| [").append(cell(parts[3])).append("](").append(DASHBOARD).append(parts[3]).append(") | ").append(cell(parts[4]))
          .append(" | ").append(cell(parts[2])).append(" | ").append(cell(parts[1])).append(" | ").append(cell(parts[5])).append(" | ");
      sb.append(value(e, "base"));
      final String lineBase = e.getString("baseRef", null);
      if (lineBase != null && !lineBase.equals(base))
        sb.append(" at `").append(shortRef(m.getTrigger(), lineBase)).append('`');
      sb.append(rawLink(e.getString("baseRunId", null))).append(" | ").append(value(e, "last"))
          .append(rawLink(e.getString("lastRunId", null))).append(" | ").append(change(e)).append(" | ").append(band(e)).append(" |\n");

      final Boolean changed = bench.changed(e.getString("baseBench", null), e.getString("lastBench", null));
      if (changed == null)
        anyUnknown = true;
      else if (changed)
        anyChanged = true;
    }
    if (anyChanged)
      sb.append("\n**Note:** ").append(BENCH_CHANGED).append('\n');
    else if (anyUnknown)
      sb.append("\n**Note:** ").append(BENCH_UNKNOWN).append('\n');

    sb.append("\nA line recovers when its newest ").append(nightly ? "nightly run" : "release")
        .append(" is back within the band of the baseline above, or its status is `ok` again. This issue closes itself when every line has recovered ")
        .append(nightly ? "on 2 consecutive nightly runs" : "on 1 later release").append(".\n\n");
    sb.append("If the change is expected, close this issue to accept the regression: its lines are not reported again against the same baseline.\n\n");
    sb.append(m.render()).append('\n');
    return sb.toString();
  }

  public static String runComment(final String runKey, final List<String> rows) {
    return runComment(runKey, rows, List.of(), null);
  }

  /** The comment of one run: its key marker, a heading, the rows, the improvements if any, and an optional closing line. */
  static String runComment(final String runKey, final List<String> rows, final List<String> improvements, final String closing) {
    final int colon = runKey.indexOf(':');
    final String trigger = colon < 0 ? runKey : runKey.substring(0, colon);
    final String ref = colon < 0 ? "" : runKey.substring(colon + 1);
    final StringBuilder sb = new StringBuilder(RUN_MARKER_PREFIX).append(runKey).append(" -->\n");
    sb.append(NIGHTLY.equals(trigger) ? "**Nightly run `" + shortRef(trigger, ref) + "`**" : "**Release `" + ref + "`**").append("\n\n");
    for (final String row : rows)
      sb.append(row).append('\n');
    if (!improvements.isEmpty()) {
      sb.append("\nImprovements in this run:\n\n");
      for (final String row : improvements)
        sb.append(row).append('\n');
    }
    if (closing != null)
      sb.append('\n').append(closing).append('\n');
    return sb.toString();
  }

  /** The closing line of an issue whose lines all recovered. */
  static String closing(final String trigger) {
    return NIGHTLY.equals(trigger) ?
        "Every line has recovered on 2 consecutive nightly runs: closing this issue." :
        "Every line has recovered on a later release: closing this issue.";
  }

  /**
   * The comment rows of the issue's lines last measured at {@code ref}: for the issue's own run only the lines added to it after it was
   * opened (the others are in the body), for a later run each line's state against its recorded baseline.
   */
  static List<String> runRows(final Marker m, final String ref) {
    final List<String> rows = new ArrayList<>();
    final boolean ownRun = ref.equals(m.getRef());
    for (final String id : sortedIds(m)) {
      final JSONObject e = m.line(id);
      if (!ref.equals(e.getString("lastRun", null)))
        continue;
      final String state;
      if (ownRun) {
        if (!e.getBoolean("added", false))
          continue;
        state = "worse, added to this issue";
      } else if (e.getBoolean("recovered", false))
        state = "recovered (" + e.getInt("streak", 0) + " of " + runsToClose(m.getTrigger()) + ")";
      else if (e.has("baseMedian") && !"ok".equals(e.getString("lastStatus", null)))
        state = "not judged, status `" + e.getString("lastStatus", "") + "`";
      else
        state = "still worse";
      rows.add("- `" + id + "`: " + state + ": " + value(e, "base") + " -> " + value(e, "last") + changeSuffix(e));
    }
    return rows;
  }

  /** One improvement row, from the line's own verdict. */
  static String improvementRow(final Verdict v) {
    final Point b = v.baseline();
    final Point n = v.newest();
    if (Double.isNaN(n.median()))
      return "- `" + v.key().id() + "`: `" + b.status() + "` -> `" + n.status() + "`";
    return "- `" + v.key().id() + "`: " + number(b.median()) + unit(b.unit()) + " -> " + number(n.median()) + unit(n.unit()) + " (" + percent(
        -v.change()) + " better)";
  }

  static String shortRef(final String trigger, final String ref) {
    if (ref == null)
      return "?";
    return NIGHTLY.equals(trigger) && ref.length() > 7 ? ref.substring(0, 7) : ref;
  }

  private static List<String> sortedIds(final Marker m) {
    final List<String> ids = new ArrayList<>(m.lineIds());
    Collections.sort(ids);
    return ids;
  }

  /** machine, variant, scale, workload, case, metric of a line id; a case holding '/' keeps it. */
  private static String[] parts(final String id) {
    final String[] p = id.split("/");
    if (p.length < 6)
      return new String[] { "", "", "", "", id, "" };
    final String caseName = String.join("/", List.of(p).subList(4, p.length - 1));
    return new String[] { p[0], p[1], p[2], p[3], caseName, p[p.length - 1] };
  }

  private static String cell(final String text) {
    return text.replace("|", "\\|");
  }

  /** The base or last value of an entry: its median with its unit, or its status for a status line. */
  private static String value(final JSONObject e, final String prefix) {
    if (e.has(prefix + "Median"))
      return number(e.getDouble(prefix + "Median")) + unit(e.getString("unit", null));
    return "`" + e.getString(prefix + "Status", "?") + "`";
  }

  private static String change(final JSONObject e) {
    if (!e.has("lastChange"))
      return "status";
    final double c = e.getDouble("lastChange");
    return c > 0 ? percent(c) + " worse" : c < 0 ? percent(-c) + " better" : "0.0%";
  }

  private static String band(final JSONObject e) {
    return e.has("lastBand") ? percent(e.getDouble("lastBand")) : "";
  }

  private static String changeSuffix(final JSONObject e) {
    return e.has("lastChange") ? " (" + change(e) + ", band " + band(e) + ")" : "";
  }

  private static String rawLink(final String runId) {
    if (runId == null || runId.length() < 7)
      return "";
    return " ([raw](" + RAW + runId.substring(0, 4) + "/" + runId.substring(5, 7) + "/" + runId + ".json))";
  }

  private static String unit(final String unit) {
    return unit == null || unit.isEmpty() ? "" : " " + unit;
  }

  private static String percent(final double ratio) {
    return String.format(Locale.ROOT, "%.1f%%", ratio * 100);
  }

  static String number(final double v) {
    if (Double.isNaN(v) || Double.isInfinite(v))
      return String.valueOf(v);
    if (v == Math.rint(v) && Math.abs(v) < 1e15)
      return Long.toString((long) v);
    return new BigDecimal(v).round(new MathContext(4)).stripTrailingZeros().toPlainString();
  }
}
