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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MarkerTest {
  @TempDir
  Path dir;

  static final String BENCH = "0123456789abcdef0123456789abcdef01234567";

  private static Marker sample() {
    final Marker marker = Marker.create("nightly", "abc1234", "def5678");
    final JSONObject entry = new JSONObject().put("baseRef", "def5678").put("baseRunId", "2026-11-20T03-00-00Z-def5678-jvm")
        .put("baseBench", BENCH).put("baseMedian", 12.5).put("baseSpread", 0.08).put("baseStatus", "ok").put("unit", "ms")
        .put("better", "lower").put("worseRunId", "2026-11-21T03-00-00Z-abc1234-jvm").put("worseBench", BENCH).put("worseStatus", "ok")
        .put("worseMedian", 20.0).put("worseChange", 0.6).put("worseBand", 0.1).put("lastRun", "abc1234").put("lastStatus", "ok")
        .put("lastMedian", 20.0).put("lastChange", 0.6).put("lastBand", 0.1).put("streak", 1).put("recovered", false);
    marker.putLine("perf-01/jvm/M/ingestion/bulk-import/time", entry);
    marker.putLine("perf-01/jvm/M/ingestion/bulk-import/_status", new JSONObject().put("baseRef", "def5678").put("baseStatus", "ok")
        .put("baseRunId", "2026-11-20T03-00-00Z-def5678-jvm").put("worseRunId", "2026-11-21T03-00-00Z-abc1234-jvm")
        .put("worseStatus", "failed").put("lastRun", "abc1234").put("lastStatus", "failed").put("streak", 0).put("recovered", false));
    return marker;
  }

  @Test
  void roundTripsThroughTheBody() {
    final Marker marker = sample();
    final String body = "Some text\n\n" + marker.render() + "\n\nmore text";

    final Marker parsed = Marker.parse(body);

    assertThat(parsed).isNotNull();
    assertThat(parsed.getTrigger()).isEqualTo("nightly");
    assertThat(parsed.getRef()).isEqualTo("abc1234");
    assertThat(parsed.getBaseRef()).isEqualTo("def5678");
    assertThat(parsed.lineIds()).containsExactlyInAnyOrder("perf-01/jvm/M/ingestion/bulk-import/time",
        "perf-01/jvm/M/ingestion/bulk-import/_status");
    final JSONObject time = parsed.line("perf-01/jvm/M/ingestion/bulk-import/time");
    assertThat(time.getDouble("baseMedian")).isEqualTo(12.5);
    assertThat(time.getDouble("baseSpread")).isEqualTo(0.08);
    assertThat(time.getString("lastRun")).isEqualTo("abc1234");
    assertThat(time.getString("worseBench")).isEqualTo(BENCH);
    assertThat(time.getInt("streak")).isEqualTo(1);
    final JSONObject status = parsed.line("perf-01/jvm/M/ingestion/bulk-import/_status");
    assertThat(status.getString("baseStatus")).isEqualTo("ok");
    assertThat(status.getString("worseStatus")).isEqualTo("failed");
    assertThat(status.getInt("streak", -1)).isEqualTo(0);
    assertThat(status.getBoolean("recovered", true)).isFalse();
    assertThat(parsed.render()).isEqualTo(marker.render());
  }

  @Test
  void renderKeepsEachLongValueOnce() {
    final String rendered = sample().render();

    assertThat(rendered.split(BENCH, -1)).hasSize(2);
    assertThat(rendered.split("2026-11-21T03-00-00Z-abc1234-jvm", -1)).hasSize(2);
    assertThat(rendered).doesNotContain("baseValues").doesNotContain("lastRunId").doesNotContain("lastBench");
  }

  @Test
  void renderRefusesAFieldItDoesNotKnow() {
    final Marker marker = sample();
    marker.line("perf-01/jvm/M/ingestion/bulk-import/time").put("baseValues", new JSONArray().put(12.0));

    assertThatThrownBy(marker::render).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("baseValues");
  }

  @Test
  void renderIsDeterministicAndNeverEmitsACommentTerminator() {
    final Marker marker = sample();
    marker.putLine("a/b/c/d/e/f", new JSONObject().put("baseRef", "def5678").put("baseMedian", 1.0).put("baseSpread", 0.0)
        .put("baseStatus", "x --> y > z").put("lastRun", "abc1234").put("streak", 0));

    final String rendered = marker.render();
    final String json = rendered.substring("<!-- perf-regression ".length(), rendered.length() - " -->".length());

    assertThat(json).doesNotContain("-->").doesNotContain(">");
    assertThat(rendered).startsWith("<!-- perf-regression {").endsWith("} -->");
    assertThat(Marker.parse(rendered).line("a/b/c/d/e/f").getString("baseStatus")).isEqualTo("x --> y > z");
    assertThat(json.indexOf("a/b/c/d/e/f")).isLessThan(json.indexOf("perf-01"));
  }

  @Test
  void parseReturnsNullForUnusableMarkers() {
    assertThat(Marker.parse(null)).isNull();
    assertThat(Marker.parse("no marker here")).isNull();
    assertThat(Marker.parse("<!-- perf-regression {\"v\":1,\"trigger\":\"nightly\" -->")).isNull();
    assertThat(Marker.parse("<!-- perf-regression {\"v\":2,\"trigger\":\"nightly\",\"ref\":\"a\"} -->")).isNull();
    assertThat(Marker.parse("<!-- perf-regression {\"v\":1,\"ref\":\"a\"} -->")).isNull();
    assertThat(Marker.parse("<!-- perf-regression {\"v\":1,\"trigger\":\"nightly\"} -->")).isNull();
  }

  @Test
  void entryOfTheWrongShapeMakesTheMarkerUnusable() {
    final Marker wrongStreak = sample();
    wrongStreak.line("perf-01/jvm/M/ingestion/bulk-import/time").put("streak", "two");
    assertThat(Marker.parse(wrongStreak.render())).isNull();

    final Marker noMedian = sample();
    noMedian.line("perf-01/jvm/M/ingestion/bulk-import/time").remove("baseMedian");
    assertThat(Marker.parse(noMedian.render())).isNull();

    final Marker statusWithMedian = sample();
    statusWithMedian.line("perf-01/jvm/M/ingestion/bulk-import/_status").put("baseMedian", 1.0);
    assertThat(Marker.parse(statusWithMedian.render())).isNull();

    final Marker textSpread = sample();
    textSpread.line("perf-01/jvm/M/ingestion/bulk-import/time").put("baseSpread", "0.08");
    assertThat(Marker.parse(textSpread.render())).isNull();

    final Marker noSpread = sample();
    noSpread.line("perf-01/jvm/M/ingestion/bulk-import/time").remove("baseSpread");
    assertThat(Marker.parse(noSpread.render())).isNull();

    final Marker statusWithSpread = sample();
    statusWithSpread.line("perf-01/jvm/M/ingestion/bulk-import/_status").put("baseSpread", 0.0);
    assertThat(Marker.parse(statusWithSpread.render())).isNull();

    final String rendered0 = sample().render();
    assertThat(Marker.parse(rendered0.replace("\"lines\":{\"", "\"lines\":{\"x/y/z\":{\"zz\":1},\""))).isNull();
    final String badIndex = rendered0.replaceFirst("(\"[a-z]{2}\"):0([,}])", "$1:99$2");
    assertThat(badIndex).isNotEqualTo(rendered0);
    assertThat(Marker.parse(badIndex)).isNull();

    final Marker noLastRun = sample();
    noLastRun.line("perf-01/jvm/M/ingestion/bulk-import/time").remove("lastRun");
    assertThat(Marker.parse(noLastRun.render())).isNull();

    final Marker lineNotAnObject = sample();
    final String rendered = lineNotAnObject.render().replace("\"lines\":{", "\"lines\":{\"x/y\":3,");
    assertThat(Marker.parse(rendered)).isNull();

    assertThat(Marker.parse(sample().render())).isNotNull();
  }

  @Test
  void aWronglyTypedMarkerIsSkippedWithAWarning() throws IOException {
    final Marker bad = sample();
    bad.line("perf-01/jvm/M/ingestion/bulk-import/time").put("streak", "two");
    final JSONArray issues = new JSONArray();
    issues.put(new JSONObject().put("number", 1).put("state", "open").put("body", bad.render()));
    issues.put(new JSONObject().put("number", 2).put("state", "open").put("body", sample().render()));
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

    final List<TrackedIssue> read = TrackedIssue.readAll(issuesFile(issues.toString()), new PrintStream(bytes, true, StandardCharsets.UTF_8));

    assertThat(read).hasSize(1);
    assertThat(read.get(0).number()).isEqualTo(2);
    assertThat(bytes.toString(StandardCharsets.UTF_8)).contains("#1").contains("invalid marker");
  }

  private Path issuesFile(final String json) throws IOException {
    final Path file = dir.resolve("issues.json");
    Files.writeString(file, json, StandardCharsets.UTF_8);
    return file;
  }

  @Test
  void unreadableMarkerIsIgnored() throws IOException {
    final JSONArray issues = new JSONArray();
    issues.put(new JSONObject().put("number", 1).put("state", "open").put("closedBy", JSONObject.NULL).put("body", "x\n" + sample().render()));
    issues.put(new JSONObject().put("number", 2).put("state", "open").put("body", "<!-- perf-regression {\"v\":1,\"trigger\":\"nightly\",\"re -->"));
    issues.put(new JSONObject().put("number", 3).put("state", "open").put("body", "just text"));

    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    final List<TrackedIssue> read = TrackedIssue.readAll(issuesFile(issues.toString()), new PrintStream(bytes, true, StandardCharsets.UTF_8));

    assertThat(read).hasSize(1);
    assertThat(read.get(0).number()).isEqualTo(1);
    assertThat(read.get(0).open()).isTrue();
    final String warnings = bytes.toString(StandardCharsets.UTF_8);
    assertThat(warnings.lines().toList()).hasSize(2);
    assertThat(warnings).contains("#2").contains("invalid marker").contains("#3").contains("no marker");
  }

  @Test
  void botCloseIsNotAcceptance() throws IOException {
    final String body = sample().render();
    final JSONArray issues = new JSONArray();
    issues.put(new JSONObject().put("number", 1).put("state", "closed").put("closedBy", "github-actions[bot]").put("body", body));
    issues.put(new JSONObject().put("number", 2).put("state", "closed").put("closedBy", "robfrank").put("body", body));
    issues.put(new JSONObject().put("number", 3).put("state", "open").put("closedBy", JSONObject.NULL).put("body", body));

    final List<TrackedIssue> read = TrackedIssue.readAll(issuesFile(issues.toString()), new PrintStream(new ByteArrayOutputStream()));

    assertThat(read).hasSize(3);
    assertThat(read.get(0).open()).isFalse();
    assertThat(read.get(0).closedByMaintainer()).isFalse();
    assertThat(read.get(1).closedByMaintainer()).isTrue();
    assertThat(read.get(2).open()).isTrue();
    assertThat(read.get(2).closedByMaintainer()).isFalse();
  }

  @Test
  void aFileThatIsNotAnArrayFails() throws IOException {
    final Path file = issuesFile("{\"number\":1}");
    final PrintStream warn = new PrintStream(new ByteArrayOutputStream());
    assertThatThrownBy(() -> TrackedIssue.readAll(file, warn)).isInstanceOf(IllegalArgumentException.class);
  }
}
