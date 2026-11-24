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

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

class JudgeTest {

  private static final LineKey STATUS = new LineKey("m", "jvm", "s", "w", "c", LineKey.STATUS);
  private static final LineKey RECALL = new LineKey("m", "jvm", "s", "w", "c", "recall");
  private static final LineKey TIME = new LineKey("m", "jvm", "s", "w", "c", "time");

  private static int clock = 0;

  private static Point at(final String trigger, final String ref, final int day, final String status, final double median) {
    final Instant t = Instant.parse("2026-11-01T00:00:00Z").plusSeconds(day * 86400L);
    final boolean nightly = "nightly".equals(trigger);
    final boolean isStatus = Double.isNaN(median);
    return new Point(trigger, ref, nightly ? ref : null, nightly ? null : ref, t, t.plusSeconds(clock++), "run" + clock, "b", status, median,
        isStatus ? new double[0] : new double[] { median, median }, "ms", "lower");
  }

  /** A nightly point of a case during which the host was not quiet (it slept or was suspended). */
  private static Point notQuiet(final String ref, final int day, final String status, final double median) {
    final Point p = nightly(ref, day, status, median);
    return new Point(p.trigger(), p.ref(), p.commit(), p.tag(), p.t(), p.runAt(), p.runId(), p.benchCommit(), p.status(), p.median(),
        p.values(), p.unit(), p.better(), true);
  }

  private static Point nightly(final String ref, final int day, final String status, final double median) {
    return at("nightly", ref, day, status, median);
  }

  private static Point release(final String ref, final int day, final double median) {
    return at("release", ref, day, "ok", median);
  }

  private static Verdict only(final List<Verdict> verdicts, final LineKey key) {
    final List<Verdict> found = verdicts.stream().filter(v -> v.key().equals(key)).toList();
    assertThat(found).hasSize(1);
    return found.get(0);
  }

  private static Map<LineKey, List<Point>> lines(final Object... kv) {
    final Map<LineKey, List<Point>> m = new TreeMap<>();
    for (int i = 0; i < kv.length; i += 2)
      m.put((LineKey) kv[i], new ArrayList<>((List<Point>) kv[i + 1]));
    return m;
  }

  @Test
  void statusFlipIsOneWorseLine() {
    final List<Verdict> v = Judge.judge(lines(STATUS, List.of(nightly("a", 1, "ok", Double.NaN), nightly("b", 2, "failed", Double.NaN)), TIME,
        List.of(nightly("a", 1, "ok", 100), nightly("b", 2, "failed", 100)), RECALL, List.of(nightly("a", 1, "ok", 1), nightly("b", 2, "failed", 1))));
    assertThat(v.stream().filter(x -> x.kind() == Verdict.Kind.WORSE).toList()).hasSize(1);
    assertThat(only(v, STATUS).kind()).isEqualTo(Verdict.Kind.WORSE);
  }

  @Test
  void hostNotQuietNewestIsNotJudged() {
    final List<Verdict> v = Judge.judge(lines(TIME, List.of(nightly("a", 1, "ok", 100), nightly("b", 2, "ok", 100), notQuiet("c", 3, "ok", 300)),
        STATUS, List.of(nightly("a", 1, "ok", Double.NaN), nightly("b", 2, "ok", Double.NaN), notQuiet("c", 3, "timeout", Double.NaN)),
        RECALL, List.of(nightly("a", 1, "ok", 1), notQuiet("c", 3, "ok", 0.2))));

    // the line is judged on its newest quiet point
    assertThat(only(v, TIME).newest().ref()).isEqualTo("b");
    assertThat(only(v, TIME).kind()).isEqualTo(Verdict.Kind.SAME);
    assertThat(only(v, STATUS).newest().ref()).isEqualTo("b");
    assertThat(only(v, STATUS).kind()).isEqualTo(Verdict.Kind.SAME);
    // its only quiet point has no baseline: nothing is judged
    assertThat(only(v, RECALL).newest().ref()).isEqualTo("a");
    assertThat(only(v, RECALL).kind()).isEqualTo(Verdict.Kind.NOT_JUDGED);
    // a line whose every point is not quiet has no verdict at all
    assertThat(Judge.judge(lines(TIME, List.of(notQuiet("a", 1, "ok", 100), notQuiet("b", 2, "ok", 300))))).isEmpty();
  }

  @Test
  void hostNotQuietIsNeverABaseline() {
    final List<Verdict> v = Judge.judge(lines(TIME, List.of(nightly("a", 1, "ok", 100), notQuiet("b", 2, "ok", 300), nightly("c", 3, "ok", 150)),
        STATUS, List.of(nightly("a", 1, "ok", Double.NaN), notQuiet("b", 2, "timeout", Double.NaN), nightly("c", 3, "ok", Double.NaN))));

    assertThat(only(v, TIME).baseline().ref()).isEqualTo("a");
    assertThat(only(v, TIME).kind()).isEqualTo(Verdict.Kind.WORSE);
    assertThat(only(v, STATUS).baseline().ref()).isEqualTo("a");
    assertThat(only(v, STATUS).kind()).isEqualTo(Verdict.Kind.SAME);
    // a tracked line is not followed to a run during which the host was not quiet
    assertThat(Judge.runsAfter(List.of(nightly("a", 1, "ok", 100), notQuiet("b", 2, "ok", 300)), "nightly", "a")).isEmpty();
    assertThat(Judge.runsAfter(List.of(nightly("a", 1, "ok", 100), notQuiet("b", 2, "ok", 300)), "nightly", "z")).extracting(Point::ref)
        .containsExactly("a");
  }

  @Test
  void failedCaseMetricsAreNotJudged() {
    final List<Verdict> v = Judge.judge(lines(RECALL, List.of(nightly("a", 1, "ok", 0.99), nightly("b", 2, "failed", 0.2))));
    assertThat(only(v, RECALL).kind()).isEqualTo(Verdict.Kind.NOT_JUDGED);
  }

  @Test
  void alreadyFailingIsNotReportedAgain() {
    final List<Verdict> v = Judge.judge(lines(STATUS,
        List.of(nightly("a", 1, "ok", Double.NaN), nightly("b", 2, "failed", Double.NaN), nightly("c", 3, "failed", Double.NaN))));
    assertThat(only(v, STATUS).kind()).isNotEqualTo(Verdict.Kind.WORSE);
  }

  @Test
  void recoveredStatusIsBetter() {
    final List<Verdict> v = Judge.judge(lines(STATUS, List.of(nightly("a", 1, "failed", Double.NaN), nightly("b", 2, "ok", Double.NaN))));
    assertThat(only(v, STATUS).kind()).isEqualTo(Verdict.Kind.BETTER);
  }

  @Test
  void invalidBaselineIsSkipped() {
    final Point p100 = nightly("a", 1, "ok", 100);
    final List<Verdict> v = Judge.judge(lines(TIME, List.of(p100, nightly("b", 2, "failed", 5), nightly("c", 3, "ok", 150))));
    final Verdict verdict = only(v, TIME);
    assertThat(verdict.baseline()).isSameAs(p100);
    assertThat(verdict.kind()).isEqualTo(Verdict.Kind.WORSE);
  }

  @Test
  void judgesEachTriggerSeparately() {
    final Point r0 = release("26.10.2", 0, 300);
    final Point n1 = nightly("a", 1, "ok", 100);
    final Point n2 = nightly("b", 2, "ok", 105);
    final Point r1 = release("26.11.1", 0, 300);
    final List<Verdict> v = Judge.judge(lines(TIME, List.of(r0, n1, n2, r1)));
    assertThat(v).hasSize(2);
    assertThat(v.get(0).trigger()).isEqualTo("nightly");
    assertThat(v.get(0).baseline()).isSameAs(n1);
    assertThat(v.get(0).kind()).isEqualTo(Verdict.Kind.SAME);
    assertThat(v.get(1).trigger()).isEqualTo("release");
    assertThat(v.get(1).baseline()).isSameAs(r0);
    assertThat(v.get(1).kind()).isEqualTo(Verdict.Kind.SAME);
  }

  @Test
  void releaseBaselineIsThePreviousDifferentTag() {
    final Point first = release("26.10.2", 1, 100);
    final List<Verdict> v = Judge.judge(lines(TIME, List.of(first, release("26.11.1", 2, 100), release("26.11.1", 2, 130))));
    final Verdict verdict = only(v, TIME);
    assertThat(verdict.baseline()).isSameAs(first);
    assertThat(verdict.kind()).isEqualTo(Verdict.Kind.WORSE);
  }

  @Test
  void sameCommitRerunIsNotItsOwnBaseline() {
    final Point b = nightly("b", 1, "ok", 100);
    final List<Verdict> v = Judge.judge(lines(TIME, List.of(b, nightly("a", 2, "ok", 100), nightly("a", 2, "ok", 100))));
    assertThat(only(v, TIME).baseline()).isSameAs(b);
  }

  @Test
  void noBaselineIsNotJudged() {
    final List<Verdict> v = Judge.judge(lines(TIME, List.of(nightly("a", 1, "ok", 100))));
    assertThat(only(v, TIME).kind()).isEqualTo(Verdict.Kind.NOT_JUDGED);
    assertThat(only(v, TIME).baseline()).isNull();
  }

  @Test
  void verdictsFollowLineKeyOrder() {
    final List<Verdict> v = Judge.judge(lines(TIME, List.of(nightly("a", 1, "ok", 1)), RECALL, List.of(nightly("a", 1, "ok", 1))));
    assertThat(v.get(0).key()).isEqualTo(RECALL);
    assertThat(v.get(1).key()).isEqualTo(TIME);
  }

  @Test
  void runsAfterTheLastFollowedRunAreEveryRunOfTheTriggerOncePerRef() {
    final Point a = nightly("a", 1, "ok", 100);
    final Point b = nightly("b", 2, "ok", 150);
    final Point r = release("26.11.1", 2, 100);
    final Point c = nightly("c", 3, "ok", 120);
    final Point cAgain = nightly("c", 3, "ok", 101);
    final Point d = nightly("d", 4, "ok", 100);
    final List<Point> points = List.of(a, b, r, c, cAgain, d);

    assertThat(Judge.runsAfter(points, "nightly", "b")).containsExactly(cAgain, d);
    assertThat(Judge.runsAfter(points, "nightly", "d")).isEmpty();
    assertThat(Judge.runsAfter(points, "release", "26.11.1")).isEmpty();
    // a last run no longer in the data: the newest run only, as before
    assertThat(Judge.runsAfter(points, "nightly", "gone")).containsExactly(d);
  }
}
