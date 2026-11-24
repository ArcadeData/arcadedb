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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URISyntaxException;
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
  private static final LineKey STATUS = new LineKey("perf-01", "jvm", "M", "vector", "search", LineKey.STATUS);

  private static final BenchChanges NO_BENCH = BenchChanges.read(Path.of("does-not-exist.json"));

  private static int seq = 0;

  private static Point point(final String trigger, final String ref, final int day, final double median, final String bench) {
    return point(trigger, ref, day, "ok", median, bench);
  }

  private static Point point(final String trigger, final String ref, final int day, final String status, final double median,
      final String bench) {
    final Instant t = Instant.parse("2026-11-01T00:00:00Z").plusSeconds(day * 86400L);
    final boolean nightly = "nightly".equals(trigger);
    final boolean statusPoint = Double.isNaN(median);
    final int n = seq++;
    return new Point(trigger, ref, nightly ? ref : null, nightly ? null : ref, t, t.plusSeconds(n),
        String.format("2026-11-%02dT00-00-%02dZ-%d", day, n % 60, n), bench, status, median,
        statusPoint ? new double[0] : new double[] { median * 0.99, median, median * 1.01 }, statusPoint ? null : "ms",
        statusPoint ? null : "lower");
  }

  /** A point of the case's status line. */
  private static Point s(final String ref, final int day, final String status) {
    return point("nightly", ref, day, status, Double.NaN, "x");
  }

  /** A metric point of a run whose case failed. */
  private static Point failed(final String ref, final int day, final double median) {
    return point("nightly", ref, day, "failed", median, "x");
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

  private static JSONObject action(final JSONObject plan, final String op, final int issue) {
    final JSONArray actions = plan.getJSONArray("actions");
    for (int i = 0; i < actions.length(); i++)
      if (op.equals(actions.getJSONObject(i).getString("op")) && actions.getJSONObject(i).getInt("issue", -1) == issue)
        return actions.getJSONObject(i);
    throw new AssertionError("no " + op + " for #" + issue + " in " + plan);
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

  /** The marker a close wrote into the issue body, in the same update that closed it. */
  private static Marker closedMarkerOf(final JSONObject plan) {
    final Marker m = Marker.parse(action(plan, "close").getString("issueBody"));
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
    assertThat(create.getString("key")).isEqualTo("nightly:" + B + ":M");
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
    assertThat(ops(second)).containsExactly("comment", "update");
    final JSONObject update = action(second, "update");
    assertThat(update.getInt("issue")).isEqualTo(7);
    assertThat(update.getString("title")).isEqualTo("Performance regression: 3 lines worse between aaaaaaa..bbbbbbb");
    final JSONObject comment = action(second, "comment");
    assertThat(comment.getString("runKey")).isEqualTo("nightly:" + B + ":M");
    assertThat(comment.getString("body")).startsWith("<!-- perf-regression-run nightly:" + B + ":M -->").contains(NATIVE.id())
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
    assertThat(ops(jvmFile)).containsExactly("comment", "update");
    assertThat(action(jvmFile, "comment").getString("body")).contains(TIME.id()).doesNotContain(NATIVE.id());

    nat.add(n(C, 3, 300));
    final JSONObject nativeFile = plan(data(TIME, time, NATIVE, nat), open(7, markerOf(action(jvmFile, "update"))));
    assertThat(ops(nativeFile)).containsExactly("comment", "update");
    final JSONObject comment = action(nativeFile, "comment");
    assertThat(comment.getString("runKey")).isEqualTo("nightly:" + C + ":M");
    assertThat(comment.getString("body")).contains(TIME.id()).contains(NATIVE.id());

    assertThat(ops(plan(data(TIME, time, NATIVE, nat), open(7, markerOf(action(nativeFile, "update")))))).isEmpty();
  }

  @Test
  void stillWorseIsCommented() {
    final JSONObject first = plan(data(TIME, List.of(n(A, 1, 100), n(B, 2, 150))));

    final JSONObject second = plan(data(TIME, List.of(n(A, 1, 100), n(B, 2, 150), n(C, 3, 152))), open(7, markerOf(action(first, "create"))));

    assertThat(ops(second)).containsExactly("comment", "update");
    final JSONObject line = markerOf(action(second, "update")).line(TIME.id());
    assertThat(line.getString("lastRun")).isEqualTo(C);
    assertThat(line.getInt("streak")).isEqualTo(0);
    assertThat(line.getString("baseRef")).isEqualTo(A);
    final JSONObject comment = action(second, "comment");
    assertThat(comment.getString("runKey")).isEqualTo("nightly:" + C + ":M");
    assertThat(comment.getString("body")).contains("still worse");
  }

  @Test
  void recoveredTwiceClosesANightlyIssue() {
    final List<Point> points = new ArrayList<>(List.of(n(A, 1, 100), n(B, 2, 150)));
    final JSONObject first = plan(data(TIME, points));

    points.add(n(C, 3, 101));
    final JSONObject second = plan(data(TIME, points), open(7, markerOf(action(first, "create"))));
    assertThat(ops(second)).containsExactly("comment", "update");
    assertThat(action(second, "comment").getString("body")).contains("recovered");
    assertThat(markerOf(action(second, "update")).line(TIME.id()).getInt("streak")).isEqualTo(1);

    points.add(n(D, 4, 100));
    final JSONObject third = plan(data(TIME, points), open(7, markerOf(action(second, "update"))));
    assertThat(ops(third)).containsExactly("close");
    final JSONObject close = action(third, "close");
    assertThat(close.getInt("issue")).isEqualTo(7);
    assertThat(close.getString("body")).startsWith("<!-- perf-regression-run nightly:" + D + ":M -->").contains("recovered");
    // the marker that completes the streaks is written by the close itself: never a closed issue with a stale marker
    final Marker closed = closedMarkerOf(third);
    assertThat(closed.line(TIME.id()).getInt("streak")).isEqualTo(2);
    assertThat(close.getString("issueBody")).isEqualTo(IssueText.body(closed, NO_BENCH));
    assertThat(close.getString("title")).isEqualTo(IssueText.title(closed));
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
    assertThat(ops(last)).containsExactly("comment", "update");
  }

  @Test
  void aWorseRunBetweenTwoInvocationsResetsTheStreak() {
    final List<Point> points = new ArrayList<>(List.of(n(A, 1, 100), n(B, 2, 150)));
    Marker marker = markerOf(action(plan(data(TIME, points)), "create"));
    points.add(n(C, 3, 101));
    marker = markerOf(action(plan(data(TIME, points), open(7, marker)), "update"));
    assertThat(marker.line(TIME.id()).getInt("streak")).isEqualTo(1);

    // the gate did not run while D (worse) and E (recovered) were published: E alone is 1 recovered run, not the 2nd in a row
    points.add(n(D, 4, 150));
    points.add(n(E, 5, 100));
    final JSONObject plan = plan(data(TIME, points), open(7, marker));

    assertThat(ops(plan)).containsExactly("comment", "update");
    assertThat(action(plan, "comment").getString("runKey")).isEqualTo("nightly:" + E + ":M");
    final JSONObject entry = markerOf(action(plan, "update")).line(TIME.id());
    assertThat(entry.getString("lastRun")).isEqualTo(E);
    assertThat(entry.getInt("streak")).isEqualTo(1);
  }

  @Test
  void twoRecoveredRunsBetweenTwoInvocationsCloseANightlyIssue() {
    final List<Point> points = new ArrayList<>(List.of(n(A, 1, 100), n(B, 2, 150)));
    final Marker marker = markerOf(action(plan(data(TIME, points)), "create"));

    points.add(n(D, 4, 100));
    points.add(n(E, 5, 101));
    final JSONObject plan = plan(data(TIME, points), open(7, marker));

    assertThat(ops(plan)).containsExactly("close");
    assertThat(action(plan, "close").getString("body")).startsWith("<!-- perf-regression-run nightly:" + E + ":M -->").contains("recovered (2 of 2)");
  }

  @Test
  void oneRecoveredReleaseClosesAReleaseIssue() {
    final List<Point> points = new ArrayList<>(List.of(r("26.10.2", 1, 100), r("26.11.1", 2, 150)));
    final JSONObject first = plan(data(TIME, points));
    final JSONObject create = action(first, "create");
    assertThat(create.getString("key")).isEqualTo("release:26.11.1");
    assertThat(create.getString("title")).isEqualTo("Performance regression: 1 line worse in release 26.11.1 vs 26.10.2");

    points.add(r("26.12.1", 3, 100));
    final JSONObject second = plan(data(TIME, points), open(7, markerOf(create)));
    assertThat(ops(second)).containsExactly("close");
    assertThat(action(second, "close").getString("body")).startsWith("<!-- perf-regression-run release:26.12.1 -->");
  }

  @Test
  void maintainerCloseMeansAccepted() {
    final Map<LineKey, List<Point>> lines = data(TIME, List.of(n(A, 1, 100), n(B, 2, 150)));
    final Marker marker = markerOf(action(plan(lines), "create"));

    assertThat(ops(plan(lines, new TrackedIssue(7, false, true, marker)))).isEmpty();
    // without the accepted issue the same data is an event
    assertThat(ops(plan(lines))).containsExactly("create");
  }

  @Test
  void aNewRegressionAfterAnAcceptedOneOpensAnIssue() {
    final List<Point> points = new ArrayList<>(List.of(n(A, 1, 100), n(B, 2, 150)));
    final Marker accepted = markerOf(action(plan(data(TIME, points)), "create"));

    points.add(n(C, 3, 150));
    points.add(n(D, 4, 200));
    final JSONObject plan = plan(data(TIME, points), new TrackedIssue(7, false, true, accepted));

    assertThat(ops(plan)).containsExactly("create");
    assertThat(action(plan, "create").getString("key")).isEqualTo("nightly:" + D + ":M");
    assertThat(markerOf(action(plan, "create")).line(TIME.id()).getString("baseRef")).isEqualTo(C);
  }

  @Test
  void rerunAfterABotCloseIsEmpty() {
    final List<Point> points = new ArrayList<>(List.of(n(A, 1, 100), n(B, 2, 150)));
    Marker marker = markerOf(action(plan(data(TIME, points)), "create"));
    points.add(n(C, 3, 95));
    marker = markerOf(action(plan(data(TIME, points), open(7, marker)), "update"));
    assertThat(marker.line(TIME.id()).getInt("streak")).isEqualTo(1);
    points.add(n(D, 4, 108));
    final JSONObject closing = plan(data(TIME, points), open(7, marker));
    assertThat(ops(closing)).containsExactly("close");
    final Marker closed = closedMarkerOf(closing);

    // D is +13.7% on C, but it was already reported (as recovered) in the issue the bot closed
    assertThat(ops(plan(data(TIME, points), new TrackedIssue(7, false, false, closed)))).isEmpty();

    points.add(n(E, 5, 125));
    final JSONObject next = plan(data(TIME, points), new TrackedIssue(7, false, false, closed));
    assertThat(ops(next)).containsExactly("create");
    assertThat(action(next, "create").getString("key")).isEqualTo("nightly:" + E + ":M");
  }

  @Test
  void failedCaseOpensOneStatusLine() {
    final JSONObject plan = plan(data(STATUS, List.of(s(A, 1, "ok"), s(B, 2, "failed")), TIME, List.of(n(A, 1, 100), failed(B, 2, 300))));

    assertThat(ops(plan)).containsExactly("create");
    assertThat(markerOf(action(plan, "create")).lineIds()).containsExactly(STATUS.id());
    assertThat(action(plan, "create").getString("title")).isEqualTo("Performance regression: 1 line worse between aaaaaaa..bbbbbbb");
  }

  @Test
  void trackedStatusLineRecoversAndCloses() {
    final List<Point> status = new ArrayList<>(List.of(s(A, 1, "ok"), s(B, 2, "failed")));
    final JSONObject first = plan(data(STATUS, status));

    status.add(s(C, 3, "ok"));
    final JSONObject second = plan(data(STATUS, status), open(7, markerOf(action(first, "create"))));
    assertThat(ops(second)).containsExactly("comment", "update");
    assertThat(action(second, "comment").getString("body")).contains("recovered");

    status.add(s(D, 4, "ok"));
    final JSONObject third = plan(data(STATUS, status), open(7, markerOf(action(second, "update"))));
    assertThat(ops(third)).containsExactly("close");
  }

  @Test
  void trackedMetricOfAFailedRunIsNotJudged() {
    final List<Point> points = new ArrayList<>(List.of(n(A, 1, 100), n(B, 2, 150)));
    Marker marker = markerOf(action(plan(data(TIME, points)), "create"));
    points.add(n(C, 3, 100));
    marker = markerOf(action(plan(data(TIME, points), open(7, marker)), "update"));
    assertThat(marker.line(TIME.id()).getInt("streak")).isEqualTo(1);

    points.add(failed(D, 4, 100));
    final JSONObject plan = plan(data(TIME, points), open(7, marker));

    assertThat(ops(plan)).containsExactly("comment", "update");
    assertThat(action(plan, "comment").getString("body")).contains("not judged");
    assertThat(markerOf(action(plan, "update")).line(TIME.id()).getInt("streak")).isEqualTo(0);
  }

  @Test
  void reopenedIssueIsTrackedAgain() {
    final Marker marker = markerOf(action(plan(data(TIME, List.of(n(A, 1, 100), n(B, 2, 150)))), "create"));
    final Map<LineKey, List<Point>> later = data(TIME, List.of(n(A, 1, 100), n(B, 2, 150), n(C, 3, 150)));

    final JSONObject plan = plan(later, new TrackedIssue(7, true, false, marker));

    assertThat(ops(plan)).containsExactly("comment", "update");
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
    assertThat(ops(second)).containsExactly("comment", "update");
    final String body = action(second, "comment").getString("body");
    assertThat(body).contains("recovered").contains("Improvements");
    assertThat(body.substring(body.indexOf("Improvements"))).contains(TIME2.id());

    // improvements alone neither open an issue nor comment
    assertThat(ops(plan(data(TIME2, List.of(n(A, 1, 100), n(B, 2, 70)))))).isEmpty();
  }

  @Test
  void aLineTrackedByAnotherIssueIsNotAnImprovement() {
    final List<Point> time = new ArrayList<>(List.of(n(A, 1, 100), n(B, 2, 150)));
    final List<Point> time2 = new ArrayList<>(List.of(n(A, 1, 100), n(B, 2, 100)));
    final JSONObject first = plan(data(TIME, time, TIME2, time2));

    time.add(n(C, 3, 150));
    time2.add(n(C, 3, 150));
    final JSONObject second = plan(data(TIME, time, TIME2, time2), open(7, markerOf(action(first, "create"))));
    assertThat(ops(second)).containsExactly("create", "comment", "update");
    final Marker seven = markerOf(action(second, "update"));
    final Marker eight = markerOf(action(second, "create"));
    assertThat(eight.lineIds()).containsExactly(TIME2.id());

    time.add(n(D, 4, 150));
    time2.add(n(D, 4, 100));
    final JSONObject third = plan(data(TIME, time, TIME2, time2), open(7, seven), open(8, eight));
    assertThat(action(third, "comment", 8).getString("body")).contains(TIME2.id()).contains("recovered");
    assertThat(action(third, "comment", 7).getString("body")).contains("still worse").doesNotContain(TIME2.id()).doesNotContain("Improvements");
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
  void invalidBenchChangesFileIsAllUnknown() throws IOException {
    final Path file = dir.resolve("bench-changes.json");
    Files.writeString(file, "{not json", StandardCharsets.UTF_8);
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

    final BenchChanges bench = BenchChanges.read(file, new PrintStream(bytes, true, StandardCharsets.UTF_8));

    assertThat(bench.changed("x1", "y1")).isNull();
    assertThat(bytes.toString(StandardCharsets.UTF_8)).contains("bench-changes.json");
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

    // the body shows the recorded regression (x1..y1), the verdict of the newest run is y1..z1
    assertThat(BenchChanges.pairsNeeded(data(TIME, points), List.of(open(7, marker)))).containsExactly("x1..y1", "y1..z1");
  }

  /** A point shaped like a real published one: 40-character commits, a full run id and a bench commit. */
  private static Point real(final String ref, final int day, final String variant, final double median) {
    final Instant t = Instant.parse("2026-11-01T03:00:00Z").plusSeconds(day * 86400L);
    return new Point("nightly", ref, ref, null, t, t, String.format("2026-11-%02dT03-00-00.123Z-%s-%s", day, ref.substring(0, 10), variant),
        ("bench" + ref).substring(0, 40), "ok", median, new double[] { median * 0.97, median, median * 1.03 }, "ms", "lower");
  }

  @Test
  void threeHundredWorseLinesFitInABodyAndAComment() {
    final String a = "1a2b3c4d5e".repeat(4);
    final String b = "2b3c4d5e6f".repeat(4);
    final String c = "3c4d5e6f7a".repeat(4);
    final Map<LineKey, List<Point>> lines = new TreeMap<>();
    for (int i = 0; i < 300; i++) {
      final String variant = i % 2 == 0 ? "jvm" : "native";
      final LineKey key = new LineKey("perf-01", variant, "L", "workload-" + (i % 10), "case-number-" + i, "latency_p95_ms");
      final double base = 100.123456 + i * 0.987654;
      lines.put(key, new ArrayList<>(List.of(real(a, 1, variant, base), real(b, 2, variant, base * 1.5 + i * 0.001234))));
    }

    final JSONObject create = action(plan(lines), "create");
    final String body = create.getString("body");
    assertThat(body.length()).isLessThanOrEqualTo(65536);
    assertThat(body).contains("and 260 more lines: see the dashboard");
    final Marker marker = markerOf(create);
    assertThat(marker.lineIds()).hasSize(300);
    assertThat(Marker.parse(marker.render()).render()).isEqualTo(marker.render());

    for (final Map.Entry<LineKey, List<Point>> e : lines.entrySet())
      e.getValue().add(real(c, 3, e.getKey().variant(), e.getValue().get(1).median() * 1.01));
    final JSONObject followed = plan(lines, open(7, marker));
    final String comment = action(followed, "comment", 7).getString("body");
    assertThat(comment.length()).isLessThanOrEqualTo(65536);
    assertThat(comment).contains("and 260 more lines: see the dashboard");
    final String update = action(followed, "update", 7).getString("body");
    assertThat(update.length()).isLessThanOrEqualTo(65536);
    assertThat(markerOf(action(followed, "update", 7)).lineIds()).hasSize(300);
  }

  @Test
  void aMarkerTooLargeForAnIssueBodyIsReportedNotTruncated() {
    final String a = "1a2b3c4d5e".repeat(4);
    final String b = "2b3c4d5e6f".repeat(4);
    final Map<LineKey, List<Point>> lines = new TreeMap<>();
    for (int i = 0; i < 1000; i++) {
      final String variant = i % 2 == 0 ? "jvm" : "native";
      final LineKey key = new LineKey("perf-01", variant, "L", "workload-" + (i % 10), "case-number-" + i, "latency_p95_ms");
      final double base = 100.123456 + i * 0.987654;
      lines.put(key, new ArrayList<>(List.of(real(a, 1, variant, base), real(b, 2, variant, base * 1.5 + i * 0.001234))));
    }

    final JSONObject plan = plan(lines);

    final JSONObject create = action(plan, "create");
    final String body = create.getString("body");
    // the marker is the issue's state: it is never cut, the action is flagged instead so the workflow refuses it
    assertThat(body.length()).isGreaterThan(IssueText.MAX_TEXT);
    assertThat(markerOf(create).lineIds()).hasSize(1000);
    assertThat(create.getInt("oversized")).isEqualTo(body.length());
    assertThat(Planner.oversized(plan)).singleElement().asString()
        .isEqualTo("create nightly:" + b + ":L: body is " + body.length() + " characters, over GitHub's limit of 65536: the workflow will refuse it");
  }

  @Test
  void aPlanThatFitsHasNoOversizedAction() {
    final JSONObject plan = plan(data(TIME, List.of(n(A, 1, 100), n(B, 2, 150))));

    assertThat(action(plan, "create").has("oversized")).isFalse();
    assertThat(Planner.oversized(plan)).isEmpty();
  }

  @Test
  void bodyKeepsTheRegressedNumbersAfterAFollow() {
    final Point regressed = n(B, 2, 150);
    final List<Point> points = new ArrayList<>(List.of(n(A, 1, 100), regressed));
    final JSONObject first = plan(data(TIME, points));

    points.add(n(C, 3, 101));
    final String body = action(plan(data(TIME, points), open(7, markerOf(action(first, "create")))), "update").getString("body");

    String row = null;
    for (final String line : body.split("\n"))
      if (line.startsWith("| ["))
        row = line;
    assertThat(row).contains("150 ms").contains("50.0% worse").contains("/" + regressed.runId() + ".json").doesNotContain("101 ms");
  }

  @Test
  void aCloseThatFailedIsPlannedAgainWithoutNewData() {
    final List<Point> points = new ArrayList<>(List.of(n(A, 1, 100), n(B, 2, 150)));
    Marker marker = markerOf(action(plan(data(TIME, points)), "create"));
    points.add(n(C, 3, 101));
    marker = markerOf(action(plan(data(TIME, points), open(7, marker)), "update"));
    points.add(n(D, 4, 100));
    final JSONObject closing = plan(data(TIME, points), open(7, marker));
    assertThat(ops(closing)).containsExactly("close");

    // the issue is still open with every line recovered in its marker (the close did not happen after the marker was written): the
    // close is planned again, with no body to write as the marker is already there
    final JSONObject retry = plan(data(TIME, points), open(7, closedMarkerOf(closing)));

    assertThat(ops(retry)).containsExactly("close");
    assertThat(action(retry, "close").getString("body")).isEqualTo(action(closing, "close").getString("body"));
    assertThat(action(retry, "close").has("issueBody")).isFalse();
    // once closed by the bot, nothing more
    assertThat(ops(plan(data(TIME, points), new TrackedIssue(7, false, false, closedMarkerOf(closing))))).isEmpty();
  }

  @Test
  void aReopenedRecoveredIssueIsNotClosedAgainWithoutNewRecoveredRuns() {
    final List<Point> points = new ArrayList<>(List.of(n(A, 1, 100), n(B, 2, 150)));
    Marker marker = markerOf(action(plan(data(TIME, points)), "create"));
    points.add(n(C, 3, 101));
    marker = markerOf(action(plan(data(TIME, points), open(7, marker)), "update"));
    points.add(n(D, 4, 100));
    final Marker closed = closedMarkerOf(plan(data(TIME, points), open(7, marker)));
    assertThat(PlanEntries.allRecovered(closed)).isTrue();

    // the bot closed it, a maintainer reopened it: tracked again from its marker, with the recovery streaks started over
    final JSONObject reopened = plan(data(TIME, points), new TrackedIssue(7, true, false, closed, true));

    assertThat(ops(reopened)).containsExactly("update");
    final Marker restarted = markerOf(action(reopened, "update"));
    assertThat(restarted.line(TIME.id()).getInt("streak")).isEqualTo(0);
    assertThat(restarted.line(TIME.id()).getString("lastRun")).isEqualTo(D);
    // GitHub keeps "reopened" while the issue is open: the next run plans nothing more
    assertThat(ops(plan(data(TIME, points), new TrackedIssue(7, true, false, restarted, true)))).isEmpty();

    points.add(n(E, 5, 100));
    final Marker once = markerOf(action(plan(data(TIME, points), new TrackedIssue(7, true, false, restarted, true)), "update"));
    assertThat(once.line(TIME.id()).getInt("streak")).isEqualTo(1);
    points.add(point("nightly", "ffffffffff", 6, 100, "x"));
    assertThat(ops(plan(data(TIME, points), new TrackedIssue(7, true, false, once, true)))).containsExactly("close");
  }

  @Test
  void aReleaseRegressionAcceptedLongAgoStaysAccepted() {
    // the planner sees no age: a maintainer close is honoured for as long as the newest release is the one it accepted
    final Map<LineKey, List<Point>> lines = data(TIME, List.of(r("26.10.2", 1, 100), r("26.11.1", 2, 150)), TIME2,
        List.of(r("26.10.2", 1, 100), r("26.11.1", 2, 100)));
    final Marker accepted = markerOf(action(plan(lines), "create"));

    assertThat(ops(plan(lines, new TrackedIssue(3, false, true, accepted)))).isEmpty();
    assertThat(ops(plan(lines, new TrackedIssue(3, false, true, accepted), new TrackedIssue(9, false, false, accepted)))).isEmpty();
  }

  @Test
  void improvementsAreTheIssueWorkloadsOnlyAndCapped() {
    final Map<LineKey, List<Point>> lines = data(TIME, List.of(n(A, 1, 100), n(B, 2, 150)));
    final List<LineKey> better = new ArrayList<>();
    for (int i = 0; i < 25; i++) {
      final LineKey key = new LineKey("perf-01", "jvm", "M", "vector", String.format("case-%02d", i), "time");
      better.add(key);
      lines.put(key, new ArrayList<>(List.of(n(A, 1, 100), n(B, 2, 100))));
    }
    final LineKey otherWorkload = new LineKey("perf-01", "jvm", "M", "fulltext", "search", "time");
    lines.put(otherWorkload, new ArrayList<>(List.of(n(A, 1, 100), n(B, 2, 100))));
    final Marker marker = markerOf(action(plan(lines), "create"));

    lines.get(TIME).add(n(C, 3, 150));
    for (int i = 0; i < 25; i++)
      lines.get(better.get(i)).add(n(C, 3, 50 + i));
    lines.get(otherWorkload).add(n(C, 3, 10));
    final String body = action(plan(lines, open(7, marker)), "comment").getString("body");

    final String improvements = body.substring(body.indexOf("Improvements"));
    assertThat(improvements).doesNotContain("fulltext");
    assertThat(improvements.lines().filter(l -> l.startsWith("- ")).toList()).hasSize(20);
    assertThat(improvements).contains("case-00").contains("case-19").doesNotContain("case-20");
    assertThat(improvements.indexOf("case-00")).isLessThan(improvements.indexOf("case-01"));
  }

  /** A line measured at scale S, the weeknight lane. */
  private static final LineKey TIME_S = new LineKey("perf-01", "jvm", "S", "vector", "search", "time");

  @Test
  void sameCommitSAndMNightlyAreTwoEvents() {
    // Friday's S run and Saturday's M run measure the same commit B; both are worse than A
    final List<Point> s = new ArrayList<>(List.of(n(A, 1, 100), n(B, 5, 150)));
    final List<Point> m = new ArrayList<>(List.of(n(A, 0, 100), n(B, 6, 150)));
    final JSONObject friday = plan(data(TIME_S, s));
    assertThat(ops(friday)).containsExactly("create");
    assertThat(action(friday, "create").getString("key")).isEqualTo("nightly:" + B + ":S");
    assertThat(action(friday, "create").getString("title")).isEqualTo("Performance regression: 1 line worse between aaaaaaa..bbbbbbb (scale S)");
    final Marker sMarker = markerOf(action(friday, "create"));
    assertThat(sMarker.getScale()).isEqualTo("S");

    // the M results of the same commit are their own event, not lines added to the S issue
    final JSONObject saturday = plan(data(TIME_S, s, TIME, m), open(1, sMarker));
    assertThat(ops(saturday)).containsExactly("create");
    assertThat(action(saturday, "create").getString("key")).isEqualTo("nightly:" + B + ":M");
    assertThat(action(saturday, "create").getString("title")).isEqualTo("Performance regression: 1 line worse between aaaaaaa..bbbbbbb");
    final Marker mMarker = markerOf(action(saturday, "create"));
    assertThat(mMarker.getScale()).isEqualTo("M");
    assertThat(mMarker.lineIds()).containsExactly(TIME.id());

    // the next runs of both scales: one comment per issue, each keyed by its own scale
    s.add(n(C, 8, 150));
    m.add(n(C, 13, 150));
    final LineKey improvedS = new LineKey("perf-01", "jvm", "S", "vector", "build", "time");
    final List<Point> improved = List.of(n(A, 1, 100), n(B, 5, 100), n(C, 8, 50));
    final JSONObject next = plan(data(TIME_S, s, TIME, m, improvedS, improved), open(1, sMarker), open(2, mMarker));
    assertThat(ops(next)).containsExactly("comment", "update", "comment", "update");
    assertThat(action(next, "comment", 1).getString("runKey")).isEqualTo("nightly:" + C + ":S");
    assertThat(action(next, "comment", 1).getString("body")).startsWith("<!-- perf-regression-run nightly:" + C + ":S -->")
        .contains("`" + C.substring(0, 7) + "`").contains(TIME_S.id()).doesNotContain(TIME.id()).contains("Improvements");
    assertThat(action(next, "comment", 2).getString("runKey")).isEqualTo("nightly:" + C + ":M");
    // an S line that improved at the commit is an improvement of the S issue only
    assertThat(action(next, "comment", 2).getString("body")).contains(TIME.id()).doesNotContain(TIME_S.id()).doesNotContain("Improvements");
  }

  @Test
  void ldbcLinesJoinTheMEventOfTheirCommit() {
    // LDBC runs only with the Saturday M run, at a dataset scale of its own
    final LineKey ldbc = new LineKey("perf-01", "jvm", "datagen-7_5-fb", "graph-analytics", "olap-bfs", "processing_s");
    final Map<LineKey, List<Point>> lines = data(TIME, List.of(n(A, 1, 100), n(B, 2, 150)), ldbc, List.of(n(A, 1, 100), n(B, 2, 150)));

    final JSONObject plan = plan(lines);

    assertThat(ops(plan)).containsExactly("create");
    assertThat(action(plan, "create").getString("key")).isEqualTo("nightly:" + B + ":M");
    final Marker marker = markerOf(action(plan, "create"));
    assertThat(marker.getScale()).isEqualTo("M");
    assertThat(marker.lineIds()).containsExactlyInAnyOrder(TIME.id(), ldbc.id());
    assertThat(Planner.eventScale(ldbc)).isEqualTo("M");
    assertThat(Planner.eventScale(new LineKey("perf-01", "jvm", "XS", "w", "c", "m"))).isEqualTo("XS");
  }

  /** The marker of #13093 as the phase 3 gate wrote it, copied from the issue: a nightly marker without a scale. */
  private static String legacyBody() throws IOException, URISyntaxException {
    return "Lines worse in the nightly run of `6a1f81a`...\n\n" + Files.readString(
        Path.of(PlannerTest.class.getResource("/gate/legacy-marker-13093.txt").toURI()), StandardCharsets.UTF_8);
  }

  @Test
  void legacyMarkerWithoutScaleStillMatchesItsMRun() throws IOException, URISyntaxException {
    final String r0 = "0f8f74e80352166a1557b3d3f0342fdf1e4b4cf3";
    final String r1 = "6a1f81ae232b81ffe5268f55bfe06c00d29947e0";
    final String r2 = "d6b48767cb84c05f9b03e52fbb23345b4fd42288";
    final Marker legacy = Marker.parse(legacyBody());
    assertThat(legacy).isNotNull();
    assertThat(legacy.getScale()).isEqualTo("M");

    // a tracked line already followed to its newest run, and an M line of the issue's own run that nobody tracks yet
    final LineKey tracked = new LineKey("perf-01", "jvm", "M", "facet", "facet-query-after-load", "latency_p50_ms");
    final LineKey joining = new LineKey("perf-01", "jvm", "M", "ingestion", "tx-insert-1t", "latency_p50_ms");
    final Map<LineKey, List<Point>> lines = data(tracked, List.of(real(r0, 21, "jvm", 141.368585), real(r1, 22, "jvm", 157.632112),
        real(r2, 23, "jvm", 161.891162)), joining, List.of(real(r0, 21, "jvm", 100), real(r1, 22, "jvm", 150)));

    final JSONObject plan = plan(lines, open(13093, legacy));

    // no new issue: the line joins the open issue of its run, and the comment is keyed by the M run
    assertThat(ops(plan)).containsExactly("comment", "update");
    final JSONObject comment = action(plan, "comment", 13093);
    assertThat(comment.getString("runKey")).isEqualTo("nightly:" + r1 + ":M");
    assertThat(comment.getString("body")).contains(joining.id()).doesNotContain(tracked.id());
    final Marker updated = markerOf(action(plan, "update", 13093));
    assertThat(updated.line(joining.id()).getBoolean("added", false)).isTrue();
    assertThat(updated.getScale()).isEqualTo("M");

    // no second comment once the update landed
    assertThat(ops(plan(lines, open(13093, updated)))).isEmpty();
  }
}
