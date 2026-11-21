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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

class PlannerTest {
  @TempDir
  Path dir;

  private static final String A = "aaaaaaaaaa";
  private static final String B = "bbbbbbbbbb";
  private static final String C = "cccccccccc";
  private static final String D = "dddddddddd";
  private static final String E = "eeeeeeeeee";

  private static final LineKey TIME   = new LineKey("perf-01", "jvm", "M", "vector", "search", "time");
  private static final LineKey TIME2  = new LineKey("perf-01", "jvm", "M", "vector", "build", "time");
  private static final LineKey NATIVE = new LineKey("perf-01", "native", "M", "vector", "search", "time");

  private static final BenchChanges NO_BENCH = BenchChanges.read(Path.of("does-not-exist.json"));

  private static int seq = 0;

  private static Point point(final String trigger, final String ref, final int day, final double median, final String bench) {
    final Instant t = Instant.parse("2026-11-01T00:00:00Z").plusSeconds(day * 86400L);
    final boolean nightly = "nightly".equals(trigger);
    final int n = seq++;
    return new Point(trigger, ref, nightly ? ref : null, nightly ? null : ref, t, t.plusSeconds(n),
        String.format("2026-11-%02dT00-00-%02dZ-%d", day, n % 60, n), bench, "ok", median, new double[] { median * 0.99, median, median * 1.01 },
        "ms", "lower");
  }

  private static Point n(final String ref, final int day, final double median) {
    return point("nightly", ref, day, median, "x");
  }

  private static Point r(final String tag, final int day, final double median) {
    return point("release", tag, day, median, "x");
  }

  @SuppressWarnings("unchecked")
  private static Map<LineKey, List<Point>> data(final Object... kv) {
    final Map<LineKey, List<Point>> m = new TreeMap<>();
    for (int i = 0; i < kv.length; i += 2)
      m.put((LineKey) kv[i], new ArrayList<>((List<Point>) kv[i + 1]));
    return m;
  }

  private static List<String> ops(final JSONObject plan) {
    final List<String> ops = new ArrayList<>();
    final JSONArray actions = plan.getJSONArray("actions");
    for (int i = 0; i < actions.length(); i++)
      ops.add(actions.getJSONObject(i).getString("op"));
    return ops;
  }

  private static JSONObject action(final JSONObject plan, final String op) {
    final JSONArray actions = plan.getJSONArray("actions");
    for (int i = 0; i < actions.length(); i++)
      if (op.equals(actions.getJSONObject(i).getString("op")))
        return actions.getJSONObject(i);
    throw new AssertionError("no " + op + " in " + plan);
  }

  /** The marker the plan wrote into the issue body, parsed back as the workflow would read it. */
  private static Marker markerOf(final JSONObject action) {
    final Marker m = Marker.parse(action.getString("body"));
    assertThat(m).isNotNull();
    return m;
  }

  private static TrackedIssue open(final int number, final Marker marker) {
    return new TrackedIssue(number, true, false, marker);
  }

  private static JSONObject plan(final Map<LineKey, List<Point>> lines, final TrackedIssue... issues) {
    return Planner.plan(lines, List.of(issues), NO_BENCH);
  }

  @Test
  void newEventOpensOneIssue() {
    final JSONObject plan = plan(data(TIME, List.of(n(A, 1, 100), n(B, 2, 150)), TIME2, List.of(n(A, 1, 100), n(B, 2, 160))));

    assertThat(plan.getString("label")).isEqualTo("performance-regression");
    assertThat(ops(plan)).containsExactly("create");
    final JSONObject create = action(plan, "create");
    assertThat(create.getString("key")).isEqualTo("nightly:" + B);
    assertThat(create.getString("title")).isEqualTo("Performance regression: 2 lines worse between aaaaaaa..bbbbbbb");
    assertThat(create.getString("body")).contains("<!-- perf-regression {").contains("compare/" + A + "..." + B);
    final Marker marker = markerOf(create);
    assertThat(marker.getTrigger()).isEqualTo("nightly");
    assertThat(marker.getRef()).isEqualTo(B);
    assertThat(marker.getBaseRef()).isEqualTo(A);
    assertThat(marker.lineIds()).containsExactlyInAnyOrder(TIME.id(), TIME2.id());
    assertThat(marker.line(TIME.id()).getString("lastRun")).isEqualTo(B);
    assertThat(marker.line(TIME.id()).getInt("streak")).isEqualTo(0);
  }

  @Test
  void rerunWithNoNewDataProducesAnEmptyPlan() {
    final Map<LineKey, List<Point>> lines = data(TIME, List.of(n(A, 1, 100), n(B, 2, 150)), TIME2, List.of(n(A, 1, 100), n(B, 2, 160)));
    final JSONObject first = plan(lines);

    final JSONObject second = plan(lines, open(7, markerOf(action(first, "create"))));

    assertThat(ops(second)).isEmpty();
  }

  @Test
  void planIsDeterministic() {
    final Map<LineKey, List<Point>> lines = data(TIME, List.of(n(A, 1, 100), n(B, 2, 150)), TIME2, List.of(n(A, 1, 100), n(B, 2, 160)));
    assertThat(plan(lines).toString()).isEqualTo(plan(lines).toString());
  }

  @Test
  void nightlyFilesArrivingSeparatelyJoinOneIssueAndOneComment() {
    final List<Point> time = List.of(n(A, 1, 100), n(B, 2, 150));
    final List<Point> time2 = List.of(n(A, 1, 100), n(B, 2, 160));
    final JSONObject first = plan(data(TIME, time, TIME2, time2));
    assertThat(ops(first)).containsExactly("create");

    final Map<LineKey, List<Point>> withNative = data(TIME, time, TIME2, time2, NATIVE, List.of(n(A, 1, 200), n(B, 2, 300)));
    final JSONObject second = plan(withNative, open(7, markerOf(action(first, "create"))));
    assertThat(ops(second)).containsExactly("update", "comment");
    final JSONObject update = action(second, "update");
    assertThat(update.getInt("issue")).isEqualTo(7);
    assertThat(update.getString("title")).isEqualTo("Performance regression: 3 lines worse between aaaaaaa..bbbbbbb");
    final JSONObject comment = action(second, "comment");
    assertThat(comment.getString("runKey")).isEqualTo("nightly:" + B);
    assertThat(comment.getString("body")).startsWith("<!-- perf-regression-run nightly:" + B + " -->").contains(NATIVE.id())
        .doesNotContain(TIME.id());

    final JSONObject third = plan(withNative, open(7, markerOf(update)));
    assertThat(ops(third)).isEmpty();
  }

  @Test
  void trackedLinesOfOneRunArrivingSeparatelyRenderOneWholeComment() {
    final List<Point> time = new ArrayList<>(List.of(n(A, 1, 100), n(B, 2, 150)));
    final List<Point> nat = new ArrayList<>(List.of(n(A, 1, 200), n(B, 2, 300)));
    final JSONObject first = plan(data(TIME, time, NATIVE, nat));

    time.add(n(C, 3, 150));
    final JSONObject jvmFile = plan(data(TIME, time, NATIVE, nat), open(7, markerOf(action(first, "create"))));
    assertThat(ops(jvmFile)).containsExactly("update", "comment");
    assertThat(action(jvmFile, "comment").getString("body")).contains(TIME.id()).doesNotContain(NATIVE.id());

    nat.add(n(C, 3, 300));
    final JSONObject nativeFile = plan(data(TIME, time, NATIVE, nat), open(7, markerOf(action(jvmFile, "update"))));
    assertThat(ops(nativeFile)).containsExactly("update", "comment");
    final JSONObject comment = action(nativeFile, "comment");
    assertThat(comment.getString("runKey")).isEqualTo("nightly:" + C);
    assertThat(comment.getString("body")).contains(TIME.id()).contains(NATIVE.id());

    assertThat(ops(plan(data(TIME, time, NATIVE, nat), open(7, markerOf(action(nativeFile, "update")))))).isEmpty();
  }

  @Test
  void stillWorseIsCommented() {
    final JSONObject first = plan(data(TIME, List.of(n(A, 1, 100), n(B, 2, 150))));

    final JSONObject second = plan(data(TIME, List.of(n(A, 1, 100), n(B, 2, 150), n(C, 3, 152))), open(7, markerOf(action(first, "create"))));

    assertThat(ops(second)).containsExactly("update", "comment");
    final JSONObject line = markerOf(action(second, "update")).line(TIME.id());
    assertThat(line.getString("lastRun")).isEqualTo(C);
    assertThat(line.getInt("streak")).isEqualTo(0);
    assertThat(line.getString("baseRef")).isEqualTo(A);
    final JSONObject comment = action(second, "comment");
    assertThat(comment.getString("runKey")).isEqualTo("nightly:" + C);
    assertThat(comment.getString("body")).contains("still worse");
  }

  @Test
  void recoveredTwiceClosesANightlyIssue() {
    final List<Point> points = new ArrayList<>(List.of(n(A, 1, 100), n(B, 2, 150)));
    final JSONObject first = plan(data(TIME, points));

    points.add(n(C, 3, 101));
    final JSONObject second = plan(data(TIME, points), open(7, markerOf(action(first, "create"))));
    assertThat(ops(second)).containsExactly("update", "comment");
    assertThat(action(second, "comment").getString("body")).contains("recovered");
    assertThat(markerOf(action(second, "update")).line(TIME.id()).getInt("streak")).isEqualTo(1);

    points.add(n(D, 4, 100));
    final JSONObject third = plan(data(TIME, points), open(7, markerOf(action(second, "update"))));
    assertThat(ops(third)).containsExactly("update", "close");
    final JSONObject close = action(third, "close");
    assertThat(close.getInt("issue")).isEqualTo(7);
    assertThat(close.getString("body")).startsWith("<!-- perf-regression-run nightly:" + D + " -->").contains("recovered");
  }

  @Test
  void recoveryStreakResetsWhenWorseAgain() {
    final List<Point> points = new ArrayList<>(List.of(n(A, 1, 100), n(B, 2, 150)));
    Marker marker = markerOf(action(plan(data(TIME, points)), "create"));

    points.add(n(C, 3, 100));
    marker = markerOf(action(plan(data(TIME, points), open(7, marker)), "update"));
    points.add(n(D, 4, 150));
    final JSONObject worseAgain = plan(data(TIME, points), open(7, marker));
    assertThat(action(worseAgain, "comment").getString("body")).contains("still worse");
    marker = markerOf(action(worseAgain, "update"));
    assertThat(marker.line(TIME.id()).getInt("streak")).isEqualTo(0);

    points.add(n(E, 5, 100));
    final JSONObject last = plan(data(TIME, points), open(7, marker));
    assertThat(ops(last)).containsExactly("update", "comment");
  }

  @Test
  void oneRecoveredReleaseClosesAReleaseIssue() {
    final List<Point> points = new ArrayList<>(List.of(r("26.10.2", 1, 100), r("26.11.1", 2, 150)));
    final JSONObject first = plan(data(TIME, points));
    final JSONObject create = action(first, "create");
    assertThat(create.getString("key")).isEqualTo("release:26.11.1");
    assertThat(create.getString("title")).isEqualTo("Performance regression: 1 lines worse in release 26.11.1 vs 26.10.2");

    points.add(r("26.12.1", 3, 100));
    final JSONObject second = plan(data(TIME, points), open(7, markerOf(create)));
    assertThat(ops(second)).containsExactly("update", "close");
    assertThat(action(second, "close").getString("body")).startsWith("<!-- perf-regression-run release:26.12.1 -->");
  }

  @Test
  void maintainerCloseMeansAccepted() {
    final Map<LineKey, List<Point>> lines = data(TIME, List.of(n(A, 1, 100), n(B, 2, 150)));
    final Marker marker = markerOf(action(plan(lines), "create"));

    assertThat(ops(plan(lines, new TrackedIssue(7, false, true, marker)))).isEmpty();
    // the same issue closed by the bot is not an acceptance: the regression is an event again
    assertThat(ops(plan(lines, new TrackedIssue(7, false, false, marker)))).containsExactly("create");
  }

  @Test
  void reopenedIssueIsTrackedAgain() {
    final Marker marker = markerOf(action(plan(data(TIME, List.of(n(A, 1, 100), n(B, 2, 150)))), "create"));
    final Map<LineKey, List<Point>> later = data(TIME, List.of(n(A, 1, 100), n(B, 2, 150), n(C, 3, 150)));

    final JSONObject plan = plan(later, new TrackedIssue(7, true, false, marker));

    assertThat(ops(plan)).containsExactly("update", "comment");
    assertThat(action(plan, "comment").getString("body")).contains("still worse");
  }

  @Test
  void releaseRemeasuredWithReplaceDoesNotCommentTwice() {
    final Point base = r("26.10.2", 1, 100);
    final Point reported = r("26.11.1", 2, 150);
    final JSONObject first = plan(data(TIME, List.of(base, reported)));
    final Marker marker = markerOf(action(first, "create"));

    final Point remeasured = r("26.11.1", 2, 150);
    assertThat(remeasured.runId()).isNotEqualTo(reported.runId());
    final JSONObject second = plan(data(TIME, List.of(base, reported, remeasured)), open(7, marker));

    assertThat(ops(second)).isEmpty();
  }

  @Test
  void improvementsGoInTheComment() {
    final List<Point> time = new ArrayList<>(List.of(n(A, 1, 100), n(B, 2, 150)));
    final List<Point> time2 = new ArrayList<>(List.of(n(A, 1, 100), n(B, 2, 100)));
    final JSONObject first = plan(data(TIME, time, TIME2, time2));
    assertThat(markerOf(action(first, "create")).lineIds()).containsExactly(TIME.id());

    time.add(n(C, 3, 100));
    time2.add(n(C, 3, 70));
    final JSONObject second = plan(data(TIME, time, TIME2, time2), open(7, markerOf(action(first, "create"))));
    assertThat(ops(second)).containsExactly("update", "comment");
    final String body = action(second, "comment").getString("body");
    assertThat(body).contains("recovered").contains("Improvements");
    assertThat(body.substring(body.indexOf("Improvements"))).contains(TIME2.id());

    // improvements alone neither open an issue nor comment
    assertThat(ops(plan(data(TIME2, List.of(n(A, 1, 100), n(B, 2, 70)))))).isEmpty();
  }

  @Test
  void benchChangeNote() throws IOException {
    final Map<LineKey, List<Point>> lines = data(TIME, List.of(point("nightly", A, 1, 100, "x1"), point("nightly", B, 2, 150, "y1")));
    final Path file = dir.resolve("bench-changes.json");

    Files.writeString(file, "{\"x1..y1\": true}", StandardCharsets.UTF_8);
    assertThat(action(Planner.plan(lines, List.of(), BenchChanges.read(file)), "create").getString("body"))
        .contains("the bench changed between these runs: check it first");

    Files.writeString(file, "{\"x1..y1\": false}", StandardCharsets.UTF_8);
    assertThat(action(Planner.plan(lines, List.of(), BenchChanges.read(file)), "create").getString("body"))
        .doesNotContain("the bench changed between these runs").doesNotContain("bench change unknown");

    assertThat(action(Planner.plan(lines, List.of(), BenchChanges.read(dir.resolve("missing.json"))), "create").getString("body"))
        .contains("bench change unknown");
  }

  @Test
  void benchPairsNeeded() {
    final Map<LineKey, List<Point>> lines = data(TIME, List.of(point("nightly", A, 1, 100, "x1"), point("nightly", B, 2, 150, "y1")), TIME2,
        List.of(point("nightly", A, 1, 100, "x1"), point("nightly", B, 2, 100, "x1")), NATIVE,
        List.of(point("nightly", A, 1, 100, null), point("nightly", B, 2, 100, "y1")));

    assertThat(BenchChanges.pairsNeeded(Judge.judge(lines))).containsExactly("x1..y1");
  }

  @Test
  void trackedLinePairIsNeededToo() {
    final List<Point> points = new ArrayList<>(List.of(point("nightly", A, 1, 100, "x1"), point("nightly", B, 2, 150, "y1")));
    final Marker marker = markerOf(action(plan(data(TIME, points)), "create"));
    points.add(point("nightly", C, 3, 150, "z1"));

    assertThat(BenchChanges.pairsNeeded(data(TIME, points), List.of(open(7, marker)))).containsExactly("x1..z1", "y1..z1");
  }
}
