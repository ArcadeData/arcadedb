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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class IssueTextTest {
  private static final String A = "aaaaaaaaaa";
  private static final String B = "bbbbbbbbbb";
  private static final String Z = "zzzzzzzzzz";

  private static final String TIME_ID   = "perf-01/jvm/M/vector/search/time";
  private static final String RECALL_ID = "perf-01/jvm/M/vector/search/recall";

  private static final BenchChanges NO_BENCH = BenchChanges.read(Path.of("does-not-exist.json"));

  /** A metric entry reported at {@code B} and not followed since. */
  private static JSONObject entry(final String baseRef, final String baseRunId, final double before, final double after) {
    final double change = (after - before) / before;
    return new JSONObject().put("baseRef", baseRef).put("baseMedian", before).put("baseSpread", 0.0).put("baseStatus", "ok").put("unit", "ms")
        .put("better", "lower").put("baseRunId", baseRunId).put("worseRunId", "2026-11-21T01-00-00Z-2-jvm").put("worseMedian", after)
        .put("worseStatus", "ok").put("worseChange", change).put("worseBand", 0.1).put("lastRun", B).put("lastMedian", after)
        .put("lastStatus", "ok").put("lastChange", change).put("lastBand", 0.1).put("streak", 0).put("recovered", false);
  }

  /** A status entry that went from ok to {@code status} at {@code B}. */
  private static JSONObject statusEntry(final String status) {
    return new JSONObject().put("baseRef", A).put("baseStatus", "ok").put("baseRunId", "2026-11-20T01-00-00Z-1-jvm")
        .put("worseRunId", "2026-11-21T01-00-00Z-2-jvm").put("worseStatus", status).put("lastRun", B).put("lastStatus", status)
        .put("streak", 0).put("recovered", false);
  }

  private static String id(final String workload, final String caseName, final String metric) {
    return "perf-01/jvm/M/" + workload + "/" + caseName + "/" + metric;
  }

  private static List<String> tableRows(final String text) {
    final List<String> rows = new ArrayList<>();
    for (final String line : text.split("\n"))
      if (line.startsWith("| ["))
        rows.add(line);
    return rows;
  }

  private static Marker sample() {
    final Marker marker = Marker.create("nightly", B, A);
    marker.putLine(TIME_ID, entry(A, "2026-11-20T01-00-00Z-1-jvm", 100, 150));
    return marker;
  }

  private static String rowOf(final String body, final String metric) {
    for (final String line : body.split("\n"))
      if (line.startsWith("|") && line.contains("| " + metric + " |"))
        return line;
    throw new AssertionError("no row for " + metric + " in\n" + body);
  }

  @Test
  void bodyHasTheTableAndLinks() {
    final String body = IssueText.body(sample(), NO_BENCH);

    assertThat(body).contains("| workload | case | scale | variant | metric | before | after | change | band |");
    assertThat(body).contains("https://perf.arcadedb.com/workload.html?w=vector");
    assertThat(body).contains("https://github.com/ArcadeData/arcadedb-perf/blob/main/data/2026/11/2026-11-20T01-00-00Z-1-jvm.json");
    assertThat(body).contains("https://github.com/ArcadeData/arcadedb-perf/blob/main/data/2026/11/2026-11-21T01-00-00Z-2-jvm.json");
    assertThat(body).contains("https://github.com/ArcadeData/arcadedb/compare/" + A + "..." + B);
    final String row = rowOf(body, "time");
    assertThat(row).contains("| search |").contains("| M |").contains("| jvm |").contains("100 ms").contains("150 ms").contains("50.0%")
        .contains("10.0%");
    assertThat(body).contains(sample().render());
  }

  @Test
  void bodyIsAPureFunctionOfTheMarker() {
    assertThat(IssueText.body(sample(), NO_BENCH)).isEqualTo(IssueText.body(Marker.parse(sample().render()), NO_BENCH));
  }

  @Test
  void rowWithItsOwnOlderBaselineShowsIt() {
    final Marker marker = sample();
    marker.putLine(RECALL_ID, entry(Z, "2026-11-18T01-00-00Z-0-jvm", 10, 20));

    final String body = IssueText.body(marker, NO_BENCH);

    assertThat(rowOf(body, "recall")).contains("zzzzzzz");
    assertThat(rowOf(body, "time")).doesNotContain("aaaaaaa");
  }

  @Test
  void howToAccept() {
    assertThat(IssueText.body(sample(), NO_BENCH)).contains("close this issue to accept the regression");
  }

  @Test
  void titles() {
    assertThat(IssueText.title(sample())).isEqualTo("Performance regression: 1 line worse between aaaaaaa..bbbbbbb");
    final Marker oneRelease = Marker.create("release", "26.11.1", "26.10.2");
    oneRelease.putLine(TIME_ID, entry("26.10.2", "2026-10-02T01-00-00Z-r", 100, 150));
    assertThat(IssueText.title(oneRelease)).isEqualTo("Performance regression: 1 line worse in release 26.11.1 vs 26.10.2");
    final Marker release = Marker.create("release", "26.11.1", "26.10.2");
    release.putLine(TIME_ID, entry("26.10.2", "2026-10-02T01-00-00Z-r", 100, 150));
    release.putLine(RECALL_ID, entry("26.10.2", "2026-10-02T01-00-00Z-r", 100, 150));
    assertThat(IssueText.title(release)).isEqualTo("Performance regression: 2 lines worse in release 26.11.1 vs 26.10.2");
  }

  @Test
  void runCommentStartsWithTheRunMarker() {
    final String comment = IssueText.runComment("nightly:" + B, List.of("- one", "- two"));
    assertThat(comment).startsWith("<!-- perf-regression-run nightly:" + B + " -->").contains("- one\n- two");
  }

  @Test
  void bodyRowsAreOrderedBySeverity() {
    final Marker marker = Marker.create("nightly", B, A);
    marker.putLine(id("vector", "a-small", "time"), entry(A, "2026-11-20T01-00-00Z-1-jvm", 100, 120));
    marker.putLine(id("vector", "b-large", "time"), entry(A, "2026-11-20T01-00-00Z-1-jvm", 100, 180));
    marker.putLine(id("vector", "c-same", "time"), entry(A, "2026-11-20T01-00-00Z-1-jvm", 100, 120));
    marker.putLine(id("vector", "d-failed", LineKey.STATUS), statusEntry("failed"));

    final List<String> rows = tableRows(IssueText.body(marker, NO_BENCH));

    assertThat(rows).hasSize(4);
    assertThat(rows.get(0)).contains("| d-failed |").contains("`failed`");
    assertThat(rows.get(1)).contains("| b-large |");
    assertThat(rows.get(2)).contains("| a-small |");
    assertThat(rows.get(3)).contains("| c-same |");
  }

  @Test
  void bodyTableIsCappedWithALinkPerWorkload() {
    final Marker marker = Marker.create("nightly", B, A);
    for (int i = 0; i < 45; i++)
      marker.putLine(id(i < 42 ? "vector" : "fulltext", String.format("case-%02d", i), "time"),
          entry(A, "2026-11-20T01-00-00Z-1-jvm", 100, 200 - i));

    final String body = IssueText.body(marker, NO_BENCH);

    final List<String> rows = tableRows(body);
    assertThat(rows).hasSize(40);
    assertThat(rows.getFirst()).contains("| case-00 |");
    assertThat(rows.getLast()).contains("| case-39 |");
    assertThat(body).contains("and 5 more lines: see the dashboard: [fulltext](https://perf.arcadedb.com/workload.html?w=fulltext), "
        + "[vector](https://perf.arcadedb.com/workload.html?w=vector)");
    assertThat(Marker.parse(body).lineIds()).hasSize(45);
  }

  @Test
  void bodyShowsTheRegressionNotTheLastRun() {
    final Marker marker = sample();
    marker.line(TIME_ID).put("lastRun", Z).put("lastMedian", 101.0).put("lastChange", 0.01).put("streak", 1).put("recovered", true);

    final String row = rowOf(IssueText.body(marker, NO_BENCH), "time");

    assertThat(row).contains("150 ms").contains("50.0% worse").contains("2026-11-21T01-00-00Z-2-jvm.json").doesNotContain("101");
  }

  @Test
  void commentRowsAreCapped() {
    final Marker marker = Marker.create("nightly", A, Z);
    for (int i = 0; i < 45; i++)
      marker.putLine(id("vector", String.format("case-%02d", i), "time"), entry(Z, "2026-11-20T01-00-00Z-1-jvm", 100, 200 - i));

    final List<String> rows = IssueText.runRows(marker, B);

    assertThat(rows).hasSize(41);
    assertThat(rows.getFirst()).contains("case-00");
    assertThat(rows.get(39)).contains("case-39");
    assertThat(rows.getLast()).startsWith("\nand 5 more lines: see the dashboard: [vector](https://perf.arcadedb.com/workload.html?w=vector)");
  }

  @Test
  void changeAndBandGetTwoDecimalsOnlyWhenTheyAreClose() {
    final Marker marker = sample();
    marker.putLine(RECALL_ID, entry(A, "2026-11-20T01-00-00Z-1-jvm", 100, 110.04));

    final String body = IssueText.body(marker, NO_BENCH);

    assertThat(rowOf(body, "time")).contains("| 50.0% worse | 10.0% |");
    assertThat(rowOf(body, "recall")).contains("| 10.04% worse | 10.00% |");
    final List<String> rows = IssueText.runRows(marker, B);
    assertThat(rows).isEmpty();
    final Marker later = Marker.create("nightly", A, Z);
    later.putLine(RECALL_ID, entry(Z, "2026-11-20T01-00-00Z-1-jvm", 100, 110.04));
    assertThat(IssueText.runRows(later, B)).singleElement().asString().endsWith("(10.04% worse, band 10.00%)");
  }

  @Test
  void bytesAreShownInMiB() {
    final Marker marker = Marker.create("nightly", B, A);
    marker.putLine("perf-01/jvm/M/vector/build/peak_heap", entry(A, "2026-11-20T01-00-00Z-1-jvm", 104857600, 157286400).put("unit", "bytes"));

    assertThat(rowOf(IssueText.body(marker, NO_BENCH), "peak_heap")).contains("100.0 MiB").contains("150.0 MiB").doesNotContain("bytes");
  }
}
