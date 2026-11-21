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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class IssueTextTest {
  private static final String A = "aaaaaaaaaa";
  private static final String B = "bbbbbbbbbb";
  private static final String Z = "zzzzzzzzzz";

  private static final String TIME_ID   = "perf-01/jvm/M/vector/search/time";
  private static final String RECALL_ID = "perf-01/jvm/M/vector/search/recall";

  private static final BenchChanges NO_BENCH = BenchChanges.read(Path.of("does-not-exist.json"));

  private static JSONObject entry(final String baseRef, final String baseRunId, final double before, final double after) {
    return new JSONObject().put("baseRef", baseRef).put("baseMedian", before).put("baseValues", new JSONArray().put(before))
        .put("baseStatus", "ok").put("unit", "ms").put("better", "lower").put("baseRunId", baseRunId).put("lastRun", B)
        .put("lastRunId", "2026-11-21T01-00-00Z-2-jvm").put("lastMedian", after).put("lastStatus", "ok")
        .put("lastChange", (after - before) / before).put("lastBand", 0.1).put("streak", 0).put("recovered", false);
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
    assertThat(IssueText.title(sample())).isEqualTo("Performance regression: 1 lines worse between aaaaaaa..bbbbbbb");
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
}
