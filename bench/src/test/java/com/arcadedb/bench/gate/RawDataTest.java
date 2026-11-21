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
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RawDataTest {
  @TempDir
  Path dir;

  private static final String OLDER_SHAPE = """
      {
       "runId": "2026-11-17T03-00-00Z-1eeb9420ac-jvm",
       "source": "bench",
       "trigger": "nightly",
       "arcadedb": {
        "commit": "1eeb9420ac",
        "variant": "jvm",
        "tag": null
       },
       "machine": {
        "id": "perf-01",
        "cpu": "Intel Core i5-13500",
        "memGb": 62
       },
       "results": [
        {
         "workload": "ingestion",
         "case": "bulk-import",
         "scale": "M",
         "status": "ok",
         "metrics": [
          {
           "name": "records_per_s",
           "unit": "records/s",
           "better": "higher",
           "median": 191631.5
          }
         ]
        }
       ]
      }
      """;

  private static String run(final String runId, final String trigger, final String commit, final String tag, final String commitDate,
      final String startedAt, final String metrics) {
    return "{\"runId\":\"" + runId + "\",\"trigger\":\"" + trigger + "\",\"startedAt\":\"" + startedAt + "\",\"arcadedb\":{\"commit\":\""
        + commit + "\",\"variant\":\"jvm\",\"tag\":" + (tag == null ? "null" : "\"" + tag + "\"")
        + (commitDate == null ? "" : ",\"commitDate\":\"" + commitDate + "\"")
        + "},\"machine\":{\"id\":\"perf-01\"},\"config\":{\"benchCommit\":\"bc1\"},\"results\":[{\"workload\":\"w\",\"case\":\"c\","
        + "\"scale\":\"S\",\"status\":\"ok\",\"metrics\":[" + metrics + "]}]}";
  }

  private static String metric(final String name, final double median) {
    return "{\"name\":\"" + name + "\",\"unit\":\"ms\",\"better\":\"lower\",\"median\":" + median + ",\"values\":[" + median + "]}";
  }

  private void write(final String name, final String content) throws IOException {
    Files.writeString(dir.resolve(name), content);
  }

  @Test
  void keepsOnlyNightlyAndRelease() throws IOException {
    write("a.json", run("r-a", "nightly", "abc123", null, null, "2026-11-20T03:00:00Z", metric("m", 1)));
    write("b.json", run("r-b", "release", "def456", "26.11.1", null, "2026-11-20T04:00:00Z", metric("m", 2)));
    write("c.json", run("r-c", "manual", "ghi789", null, null, "2026-11-20T05:00:00Z", metric("m", 3)));
    write("d.json", run("r-d", "release", "jkl012", null, null, "2026-11-20T06:00:00Z", metric("m", 4)));

    final Map<LineKey, List<Point>> lines = RawData.read(dir);

    assertThat(lines.values()).allSatisfy(points -> assertThat(points).extracting(Point::trigger).isSubsetOf("nightly", "release"));
    assertThat(lines.values().stream().flatMap(List::stream).map(Point::runId)).containsOnly("r-a", "r-b");
  }

  @Test
  void emitsAStatusLineAndOneLinePerMetric() throws IOException {
    write("a.json", run("r-a", "nightly", "abc123", null, null, "2026-11-20T03:00:00Z", metric("m1", 1) + "," + metric("m2", 2)));

    final Map<LineKey, List<Point>> lines = RawData.read(dir);

    assertThat(lines).hasSize(3);
    final LineKey status = new LineKey("perf-01", "jvm", "S", "w", "c", LineKey.STATUS);
    final Point p = lines.get(status).get(0);
    assertThat(p.status()).isEqualTo("ok");
    assertThat(p.valid()).isTrue();
    assertThat(Double.isNaN(p.median())).isTrue();
    assertThat(p.values()).isEmpty();
    assertThat(status.id()).isEqualTo("perf-01/jvm/S/w/c/_status");
    assertThat(lines.get(new LineKey("perf-01", "jvm", "S", "w", "c", "m2")).get(0).median()).isEqualTo(2.0);
    assertThat(lines.get(new LineKey("perf-01", "jvm", "S", "w", "c", "m2")).get(0).benchCommit()).isEqualTo("bc1");
  }

  @Test
  void nightlyRefIsCommitReleaseRefIsTag() throws IOException {
    write("a.json", run("r-a", "nightly", "abc123", null, null, "2026-11-20T03:00:00Z", metric("m", 1)));
    write("b.json", run("r-b", "release", "def456", "26.11.1", null, "2026-11-20T04:00:00Z", metric("m", 2)));

    final List<Point> points = RawData.read(dir).get(new LineKey("perf-01", "jvm", "S", "w", "c", "m"));

    assertThat(points).extracting(Point::ref).containsExactlyInAnyOrder("abc123", "26.11.1");
  }

  @Test
  void ordersByCommitDateThenRunTime() throws IOException {
    write("a.json", run("r-a", "nightly", "abc123", null, "2026-11-19T10:00:00Z", "2026-11-20T03:00:00Z", metric("m", 1)));
    write("b.json", run("r-b", "release", "def456", "26.11.1", "2026-11-04T10:00:00Z", "2026-11-20T04:00:00Z", metric("m", 2)));
    write("c.json", run("r-c", "nightly", "fff111", null, "2026-11-19T10:00:00Z", "2026-11-20T01:00:00Z", metric("m", 3)));

    final List<Point> points = RawData.read(dir).get(new LineKey("perf-01", "jvm", "S", "w", "c", "m"));

    assertThat(points).extracting(Point::runId).containsExactly("r-b", "r-c", "r-a");
    assertThat(points.get(0).t()).isEqualTo(Instant.parse("2026-11-04T10:00:00Z"));
    assertThat(points.get(0).runAt()).isEqualTo(Instant.parse("2026-11-20T04:00:00Z"));
  }

  @Test
  void readsTheOlderFileShape() throws IOException {
    write("2026-11-17T03-00-00Z-1eeb9420ac-jvm.json", OLDER_SHAPE);

    final List<Point> points = RawData.read(dir).get(new LineKey("perf-01", "jvm", "M", "ingestion", "bulk-import", "records_per_s"));

    assertThat(points).hasSize(1);
    final Point p = points.get(0);
    assertThat(p.t()).isEqualTo(Instant.parse("2026-11-17T03:00:00Z"));
    assertThat(p.runAt()).isEqualTo(p.t());
    assertThat(p.values()).hasSize(0);
    assertThat(p.benchCommit()).isNull();
    assertThat(p.tag()).isNull();
    assertThat(p.ref()).isEqualTo("1eeb9420ac");
    assertThat(p.median()).isEqualTo(191631.5);
  }

  @Test
  void skipsAnUnreadableFileAndKeepsTheOthers() throws IOException {
    write("a.json", run("r-a", "nightly", "abc123", null, null, "2026-11-20T03:00:00Z", metric("m", 1)));
    write("bad.json", "{\"runId\":\"r-bad\",\"trigger\":\"nightly\",\"results\":[{\"workl");
    write("nocommit.json", run("r-nc", "nightly", "", null, null, "2026-11-20T05:00:00Z", metric("m", 1)));
    final ByteArrayOutputStream err = new ByteArrayOutputStream();

    final Map<LineKey, List<Point>> lines = RawData.read(dir, new PrintStream(err, true, StandardCharsets.UTF_8));

    assertThat(lines.values().stream().flatMap(List::stream).map(Point::runId)).containsOnly("r-a");
    final String warnings = err.toString(StandardCharsets.UTF_8);
    assertThat(warnings).contains("bad.json").contains("nocommit.json").doesNotContain("a.json:");
  }

  @Test
  void skipsAMetricWithNoMedian() throws IOException {
    write("a.json", run("r-a", "nightly", "abc123", null, null, "2026-11-20T03:00:00Z",
        metric("good", 1) + ",{\"name\":\"nomedian\",\"unit\":\"ms\"},{\"name\":\"nullmedian\",\"median\":null}"));

    final Map<LineKey, List<Point>> lines = RawData.read(dir);

    assertThat(lines.keySet()).extracting(LineKey::metric).containsExactlyInAnyOrder("good", LineKey.STATUS);
  }
}
