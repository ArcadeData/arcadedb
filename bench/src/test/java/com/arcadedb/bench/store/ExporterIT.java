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
package com.arcadedb.bench.store;

import com.arcadedb.bench.model.CaseResult;
import com.arcadedb.bench.model.Metric;
import com.arcadedb.bench.model.RunInfo;
import com.arcadedb.bench.model.RunResult;
import com.arcadedb.bench.server.ServerUnderTest;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteHttpComponent;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Runs {@link Exporter} against a real ArcadeDB container holding runs uploaded by {@link Uploader}.
 */
class ExporterIT {
  private static final String WRITER_PASSWORD = "writer-secret-1";
  private static final String READER_PASSWORD = "reader-secret-1";
  private static final Instant T0             = Instant.parse("2026-11-17T03:00:00Z");

  private static ServerUnderTest server;
  private static String          url;

  @TempDir
  Path out;

  @BeforeAll
  static void start() {
    server = ServerUnderTest.start(System.getProperty("arcadedb.bench.image", "arcadedata/arcadedb:latest"), "", 4, "1g", Map.of());
    url = server.httpUrl();
    ResultsStore.init(url, "root", ServerUnderTest.PASSWORD, WRITER_PASSWORD, READER_PASSWORD);
  }

  @AfterAll
  static void stop() {
    if (server != null)
      server.close();
  }

  @BeforeEach
  void clean() {
    final RemoteDatabase db = new RemoteDatabase(server.wrapper().host(), server.wrapper().httpPort(), "perf", "root",
        ServerUnderTest.PASSWORD);
    try (db) {
      db.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);
      db.command("sql", "DELETE FROM Measurement");
      db.command("sql", "DELETE FROM Run");
    }
  }

  static RunResult sample() {
    try (final InputStream in = ExporterIT.class.getResourceAsStream("/run-sample.json")) {
      final Path tmp = Files.createTempFile("run-sample", ".json");
      try {
        Files.write(tmp, in.readAllBytes());
        return RunResult.read(tmp);
      } finally {
        Files.deleteIfExists(tmp);
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** The sample run as another run: other time, variant, machine and release tag. */
  private static RunResult variant(final RunResult base, final String runId, final Instant startedAt, final String variant,
      final String machineId, final String tag, final double median) {
    return variant(base, runId, startedAt, variant, machineId, tag, median, null);
  }

  /** Same as above, with the date of the measured commit ({@code null} for none). */
  private static RunResult variant(final RunResult base, final String runId, final Instant startedAt, final String variant,
      final String machineId, final String tag, final double median, final Instant commitDate) {
    final JSONObject arcadedb = new JSONObject(base.info().arcadedb().toString()).put("variant", variant);
    if (tag != null)
      arcadedb.put("tag", tag);
    if (commitDate != null)
      arcadedb.put("commitDate", commitDate.toString());
    final JSONObject machine = new JSONObject(base.info().machine().toString()).put("id", machineId);
    final RunInfo info = new RunInfo(runId, startedAt, startedAt.plus(Duration.ofMinutes(40)), base.info().trigger(),
        base.info().source(), arcadedb, machine, base.info().config());
    final List<CaseResult> cases = new ArrayList<>(base.results());
    final CaseResult first = cases.getFirst();
    final List<Metric> metrics = new ArrayList<>(first.metrics());
    final Metric m = metrics.getFirst();
    metrics.set(0, new Metric(m.name(), m.unit(), m.better(), median, m.values()));
    cases.set(0, new CaseResult(first.workload(), first.caseName(), first.scale(), first.params(), first.reps(), first.warmup(),
        metrics, first.status(), first.error()));
    return new RunResult(info, cases);
  }

  private static void upload(final RunResult run) {
    Uploader.upload(url, "perf-writer", WRITER_PASSWORD, run);
  }

  private void export() {
    Exporter.export(url, "perf-reader", READER_PASSWORD, out);
  }

  private static JSONObject read(final Path file) {
    try {
      return new JSONObject(Files.readString(file));
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private JSONObject series(final String name) {
    return read(out.resolve("series/vector/" + name));
  }

  private static JSONObject line(final JSONObject series, final String machineId, final String variant) {
    final JSONArray lines = series.getJSONArray("lines");
    for (int i = 0; i < lines.length(); i++) {
      final JSONObject l = lines.getJSONObject(i);
      if (l.getString("machineId").equals(machineId) && l.getString("variant").equals(variant))
        return l;
    }
    return null;
  }

  @Test
  void exportsOneFilePerSeries() {
    final RunResult base = sample();
    upload(variant(base, "r1", T0, "jvm", "perf-01", null, 100.0));
    upload(variant(base, "r2", T0.plus(Duration.ofDays(1)), "jvm", "perf-01", null, 110.0));
    upload(variant(base, "r3", T0.plus(Duration.ofHours(12)), "native", "perf-01", null, 90.0));
    export();

    assertThat(out.resolve("index.json")).exists();
    assertThat(out.resolve("latest.json")).exists();
    assertThat(out.resolve("series/vector/knn-k10__M__throughput.json")).exists();
    assertThat(out.resolve("series/vector/knn-k10__M__p99.json")).exists();
    assertThat(out.resolve("series/graph/traverse__M___status.json")).exists();

    final JSONObject s = series("knn-k10__M__throughput.json");
    assertThat(s.getString("metric")).isEqualTo("throughput");
    assertThat(s.getString("unit")).isEqualTo("ops/s");
    assertThat(s.getString("better")).isEqualTo("higher");
    assertThat(s.getJSONArray("lines").length()).isEqualTo(2);
    final JSONArray jvm = line(s, "perf-01", "jvm").getJSONArray("points");
    assertThat(jvm.length()).isEqualTo(2);
    assertThat(jvm.getJSONObject(0).getString("runId")).isEqualTo("r1");
    assertThat(jvm.getJSONObject(1).getString("runId")).isEqualTo("r2");
    assertThat(jvm.getJSONObject(0).getString("t")).startsWith("2026-11-17T03:00:00");
    assertThat(jvm.getJSONObject(0).getString("t")).isLessThan(jvm.getJSONObject(1).getString("t"));
    assertThat(jvm.getJSONObject(0).getDouble("median")).isEqualTo(100.0);
    assertThat(jvm.getJSONObject(0).getString("commit")).isEqualTo("1e1ad51768");
    assertThat(jvm.getJSONObject(0).getString("status")).isEqualTo("ok");
    assertThat(line(s, "perf-01", "native").getJSONArray("points").length()).isEqualTo(1);

    final JSONObject index = read(out.resolve("index.json"));
    assertThat(index.has("generatedAt")).isTrue();
    final String text = index.toString();
    assertThat(text).contains("series/vector/knn-k10__M__throughput.json").contains("traverse");
  }

  @Test
  void failedPointExportedAsNull() {
    upload(sample());
    export();
    final JSONObject s = read(out.resolve("series/graph/traverse__M___status.json"));
    final JSONObject point = s.getJSONArray("lines").getJSONObject(0).getJSONArray("points").getJSONObject(0);
    assertThat(point.getString("status")).isEqualTo("timeout");
    assertThat(point.isNull("median")).isTrue();
  }

  /** The sample run as another run with its own trigger and bench commit. */
  private static RunResult withTrigger(final RunResult base, final String runId, final Instant startedAt, final String trigger,
      final String benchCommit, final List<CaseResult> cases) {
    final JSONObject config = new JSONObject(base.info().config().toString()).put("benchCommit", benchCommit);
    final RunInfo info = new RunInfo(runId, startedAt, startedAt.plus(Duration.ofMinutes(40)), trigger, base.info().source(),
        base.info().arcadedb(), base.info().machine(), config);
    return new RunResult(info, cases);
  }

  @Test
  void failedRunWithOnlyAStatusRowIsAFailedPointOfTheMetricSeriesAndOfLatest() {
    final RunResult base = sample();
    upload(variant(base, "r1", T0, "jvm", "perf-01", null, 100.0));
    final CaseResult first = base.results().getFirst();
    final CaseResult failed = new CaseResult(first.workload(), first.caseName(), first.scale(), first.params(), first.reps(),
        first.warmup(), List.of(), CaseResult.Status.FAILED, "boom");
    upload(withTrigger(base, "r2", T0.plus(Duration.ofDays(1)), "nightly", "abc1234", List.of(failed)));
    export();

    final JSONArray points = line(series("knn-k10__M__throughput.json"), "perf-01", "jvm").getJSONArray("points");
    assertThat(points.length()).isEqualTo(2);
    assertThat(points.getJSONObject(0).getString("runId")).isEqualTo("r1");
    assertThat(points.getJSONObject(1).getString("runId")).isEqualTo("r2");
    assertThat(points.getJSONObject(1).getString("status")).isEqualTo("failed");
    assertThat(points.getJSONObject(1).isNull("median")).isTrue();

    final JSONArray latest = read(out.resolve("latest.json")).getJSONArray("series");
    JSONObject found = null;
    for (int i = 0; i < latest.length(); i++)
      if (latest.getJSONObject(i).getString("metric").equals("throughput"))
        found = latest.getJSONObject(i);
    assertThat(found).isNotNull();
    assertThat(found.getJSONObject("last").getString("runId")).isEqualTo("r2");
    assertThat(found.getJSONObject("last").getString("status")).isEqualTo("failed");
    assertThat(found.getJSONObject("last").isNull("median")).isTrue();
  }

  @Test
  void aRunWithARealPointAndAStatusRowIsNotDuplicated() {
    final RunResult base = sample();
    // the sample has the ok knn-k10 case and the timeout traverse case: traverse's _status must not appear in knn-k10 twice
    upload(base);
    export();
    final JSONArray points = line(series("knn-k10__M__throughput.json"), "perf-01", "jvm").getJSONArray("points");
    assertThat(points.length()).isEqualTo(1);
    assertThat(points.getJSONObject(0).getString("status")).isEqualTo("ok");
  }

  @Test
  void onlyNightlyAndReleaseRunsAreExported() {
    final RunResult base = sample();
    final List<CaseResult> cases = base.results();
    upload(withTrigger(base, "r-nightly", T0, "nightly", "aaaaaaa", cases));
    upload(withTrigger(base, "r-release", T0.plus(Duration.ofDays(1)), "release", "bbbbbbb", cases));
    upload(withTrigger(base, "r-manual", T0.plus(Duration.ofDays(2)), "manual", "ccccccc", cases));
    export();
    final JSONArray points = line(series("knn-k10__M__throughput.json"), "perf-01", "jvm").getJSONArray("points");
    assertThat(points.length()).isEqualTo(2);
    assertThat(points.getJSONObject(0).getString("runId")).isEqualTo("r-nightly");
    assertThat(points.getJSONObject(1).getString("runId")).isEqualTo("r-release");
    assertThat(read(out.resolve("latest.json")).toString()).doesNotContain("r-manual");
  }

  @Test
  void aSeriesOfManualRunsOnlyIsNotExported() {
    final RunResult base = sample();
    upload(withTrigger(base, "r-manual", T0, "manual", "ccccccc", base.results()));
    export();
    assertThat(out.resolve("series/vector/knn-k10__M__throughput.json")).doesNotExist();
    assertThat(read(out.resolve("index.json")).getJSONArray("workloads").length()).isZero();
  }

  @Test
  void everyPointCarriesTheBenchCommit() {
    final RunResult base = sample();
    upload(withTrigger(base, "r1", T0, "nightly", "feedbee", base.results()));
    export();
    final JSONArray points = line(series("knn-k10__M__throughput.json"), "perf-01", "jvm").getJSONArray("points");
    assertThat(points.getJSONObject(0).getString("benchCommit")).isEqualTo("feedbee");
    final JSONArray status = read(out.resolve("series/graph/traverse__M___status.json")).getJSONArray("lines").getJSONObject(0)
        .getJSONArray("points");
    assertThat(status.getJSONObject(0).getString("benchCommit")).isEqualTo("feedbee");
  }

  @Test
  void machinesNeverShareALine() {
    final RunResult base = sample();
    upload(variant(base, "r1", T0, "jvm", "perf-01", null, 100.0));
    upload(variant(base, "r2", T0.plus(Duration.ofHours(1)), "jvm", "laptop", null, 50.0));
    export();
    final JSONObject s = series("knn-k10__M__throughput.json");
    assertThat(s.getJSONArray("lines").length()).isEqualTo(2);
    assertThat(line(s, "perf-01", "jvm").getJSONArray("points").length()).isEqualTo(1);
    assertThat(line(s, "laptop", "jvm").getJSONArray("points").length()).isEqualTo(1);
  }

  @Test
  void aPointIsPlacedAtTheDateOfItsCommitAndKeepsTheRunTime() {
    final RunResult base = sample();
    final Instant commitDate = T0.minus(Duration.ofDays(20));
    upload(variant(base, "r1", T0, "jvm", "perf-01", "26.10.2", 100.0, commitDate));
    export();
    final JSONObject p = line(series("knn-k10__M__throughput.json"), "perf-01", "jvm").getJSONArray("points").getJSONObject(0);
    assertThat(p.getString("t")).isEqualTo(commitDate.toString());
    assertThat(p.getString("runAt")).isEqualTo(T0.toString());
  }

  @Test
  void aRunWithoutACommitDateIsPlacedAtItsStart() {
    upload(variant(sample(), "r1", T0, "jvm", "perf-01", null, 100.0));
    export();
    final JSONObject p = line(series("knn-k10__M__throughput.json"), "perf-01", "jvm").getJSONArray("points").getJSONObject(0);
    assertThat(p.getString("t")).isEqualTo(T0.toString());
    assertThat(p.getString("runAt")).isEqualTo(T0.toString());
  }

  @Test
  void aReleaseMeasuredAfterANightlyIsOrderedByItsCommitDate() {
    // the 26.10.2 baseline is measured a day after the nightly of main, but its code is a month older
    final RunResult base = sample();
    upload(variant(base, "nightly", T0, "jvm", "perf-01", null, 120.0, T0.minus(Duration.ofHours(3))));
    upload(variant(base, "release", T0.plus(Duration.ofDays(1)), "jvm", "perf-01", "26.10.2", 100.0,
        T0.minus(Duration.ofDays(30))));
    export();
    final JSONArray points = line(series("knn-k10__M__throughput.json"), "perf-01", "jvm").getJSONArray("points");
    assertThat(points.getJSONObject(0).getString("runId")).isEqualTo("release");
    assertThat(points.getJSONObject(1).getString("runId")).isEqualTo("nightly");
    final JSONObject latest = latestThroughput();
    assertThat(latest.getJSONObject("last").getString("runId")).isEqualTo("nightly");
    assertThat(latest.getJSONObject("previousRelease").getString("runId")).isEqualTo("release");
  }

  @Test
  void twoRunsOfTheSameCommitAreOrderedByRunTime() {
    final RunResult base = sample();
    final Instant commitDate = T0.minus(Duration.ofDays(2));
    upload(variant(base, "second", T0.plus(Duration.ofDays(1)), "jvm", "perf-01", null, 110.0, commitDate));
    upload(variant(base, "first", T0, "jvm", "perf-01", null, 100.0, commitDate));
    export();
    final JSONArray points = line(series("knn-k10__M__throughput.json"), "perf-01", "jvm").getJSONArray("points");
    assertThat(points.getJSONObject(0).getString("runId")).isEqualTo("first");
    assertThat(points.getJSONObject(1).getString("runId")).isEqualTo("second");
  }

  @Test
  void latestCarriesPreviousAndPreviousRelease() {
    final RunResult base = sample();
    upload(variant(base, "r1", T0, "jvm", "perf-01", "26.11.1", 100.0));
    upload(variant(base, "r2", T0.plus(Duration.ofDays(1)), "jvm", "perf-01", null, 110.0));
    upload(variant(base, "r3", T0.plus(Duration.ofDays(2)), "jvm", "perf-01", null, 120.0));
    export();
    final JSONArray latest = read(out.resolve("latest.json")).getJSONArray("series");
    JSONObject found = null;
    for (int i = 0; i < latest.length(); i++) {
      final JSONObject l = latest.getJSONObject(i);
      if (l.getString("metric").equals("throughput"))
        found = l;
    }
    assertThat(found).isNotNull();
    assertThat(found.getString("machineId")).isEqualTo("perf-01");
    assertThat(found.getJSONObject("last").getString("runId")).isEqualTo("r3");
    assertThat(found.getJSONObject("previous").getString("runId")).isEqualTo("r2");
    assertThat(found.getJSONObject("previousRelease").getString("runId")).isEqualTo("r1");
    assertThat(found.getJSONObject("previousRelease").getString("tag")).isEqualTo("26.11.1");
    assertThat(found.getJSONObject("last").isNull("tag")).isTrue();
  }

  @Test
  void previousReleaseSkipsAFailedRelease() {
    final RunResult base = sample();
    final CaseResult first = base.results().getFirst();
    final CaseResult failed = new CaseResult(first.workload(), first.caseName(), first.scale(), first.params(), first.reps(),
        first.warmup(), List.of(), CaseResult.Status.FAILED, "boom");
    upload(variant(base, "r1", T0, "jvm", "perf-01", "26.11.1", 100.0));
    final RunResult failedRelease = variant(base, "r2", T0.plus(Duration.ofDays(1)), "jvm", "perf-01", "26.12.1", 0.0);
    upload(new RunResult(failedRelease.info(), List.of(failed)));
    upload(variant(base, "r3", T0.plus(Duration.ofDays(2)), "jvm", "perf-01", null, 120.0));
    export();
    final JSONObject found = latestThroughput();
    assertThat(found.getJSONObject("previous").getString("runId")).isEqualTo("r2");
    assertThat(found.getJSONObject("previous").isNull("median")).isTrue();
    assertThat(found.getJSONObject("previousRelease").getString("runId")).isEqualTo("r1");
    assertThat(found.getJSONObject("previousRelease").getString("tag")).isEqualTo("26.11.1");
  }

  @Test
  void aFailedReleaseOnlyLeavesNoPreviousRelease() {
    final RunResult base = sample();
    final CaseResult first = base.results().getFirst();
    final CaseResult failed = new CaseResult(first.workload(), first.caseName(), first.scale(), first.params(), first.reps(),
        first.warmup(), List.of(), CaseResult.Status.FAILED, "boom");
    final RunResult failedRelease = variant(base, "r1", T0, "jvm", "perf-01", "26.12.1", 0.0);
    upload(new RunResult(failedRelease.info(), List.of(failed)));
    upload(variant(base, "r2", T0.plus(Duration.ofDays(1)), "jvm", "perf-01", null, 120.0));
    export();
    assertThat(latestThroughput().isNull("previousRelease")).isTrue();
  }

  @Test
  void aStatusRowAndARealPointOfTheSameCaseAndRunAreOnePoint() {
    final RunResult base = sample();
    final CaseResult first = base.results().getFirst();
    final CaseResult failed = new CaseResult(first.workload(), first.caseName(), first.scale(), first.params(), first.reps(),
        first.warmup(), List.of(), CaseResult.Status.FAILED, "boom");
    // the same case twice in one run, once with its metrics and once as a bare status row: the series keeps the real point
    upload(new RunResult(base.info(), List.of(first, failed)));
    export();
    final JSONArray points = line(series("knn-k10__M__throughput.json"), "perf-01", "jvm").getJSONArray("points");
    assertThat(points.length()).isEqualTo(1);
    assertThat(points.getJSONObject(0).getString("status")).isEqualTo("ok");
    assertThat(points.getJSONObject(0).isNull("median")).isFalse();
  }

  private JSONObject latestThroughput() {
    final JSONArray latest = read(out.resolve("latest.json")).getJSONArray("series");
    JSONObject found = null;
    for (int i = 0; i < latest.length(); i++)
      if (latest.getJSONObject(i).getString("metric").equals("throughput"))
        found = latest.getJSONObject(i);
    assertThat(found).isNotNull();
    return found;
  }

  @Test
  void staleSeriesFilesAreRemovedAndOtherFilesKept() throws IOException {
    upload(sample());
    Files.writeString(out.resolve("keep.txt"), "x");
    Files.createDirectories(out.resolve("series/old"));
    Files.writeString(out.resolve("series/old/gone__M__x.json"), "{}");
    export();
    assertThat(out.resolve("series/old/gone__M__x.json")).doesNotExist();
    assertThat(out.resolve("keep.txt")).exists();
    try (final Stream<Path> s = Files.list(out)) {
      assertThat(s.map(p -> p.getFileName().toString()).toList()).containsExactlyInAnyOrder("index.json", "latest.json", "series",
          "keep.txt");
    }
  }

  @Test
  void unsafeNamesAreSanitized() {
    final RunResult base = sample();
    final List<CaseResult> cases = new ArrayList<>();
    final CaseResult first = base.results().getFirst();
    cases.add(new CaseResult("../Evil/x", "a b/../c", first.scale(), first.params(), first.reps(), first.warmup(),
        first.metrics(), first.status(), first.error()));
    upload(new RunResult(base.info(), cases));
    export();
    assertThat(out.resolve("series/___evil_x/a_b____c__M__throughput.json")).exists();
    try (final Stream<Path> s = Files.walk(out.resolve("series"))) {
      assertThat(s.filter(Files::isRegularFile).allMatch(p -> p.normalize().startsWith(out.resolve("series")))).isTrue();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    final String file = read(out.resolve("index.json")).getJSONArray("workloads").getJSONObject(0).getJSONArray("cases")
        .getJSONObject(0).getJSONArray("metrics").getJSONObject(0).getString("file");
    assertThat(file).matches("series/[A-Za-z0-9_-]+/[A-Za-z0-9_-]+\\.json");
    assertThat(out.resolve(file)).exists();
  }

  @Test
  void failedSwapRestoresPreviousTree() throws IOException {
    final RunResult base = sample();
    upload(variant(base, "r1", T0, "jvm", "perf-01", null, 100.0));
    export();
    final String before = Files.readString(out.resolve("series/vector/knn-k10__M__throughput.json"));
    Files.writeString(out.resolve("keep.txt"), "x");

    upload(variant(base, "r2", T0.plus(Duration.ofDays(1)), "jvm", "perf-01", null, 110.0));
    Exporter.swapFaultHook = () -> {
      throw new UncheckedIOException(new IOException("injected"));
    };
    try {
      assertThatThrownBy(this::export).isInstanceOf(UncheckedIOException.class);
    } finally {
      Exporter.swapFaultHook = null;
    }
    assertThat(Files.readString(out.resolve("series/vector/knn-k10__M__throughput.json"))).isEqualTo(before);
    assertThat(out.resolve("index.json")).exists();
    assertThat(out.resolve("keep.txt")).exists();
    try (final Stream<Path> siblings = Files.list(out.getParent())) {
      assertThat(siblings.map(p -> p.getFileName().toString())
          .filter(n -> n.startsWith(out.getFileName() + ".export-") || n.startsWith(out.getFileName() + ".old-"))).isEmpty();
    }
  }

  @Test
  void lonelyOldTreeIsRestoredByNextExport() throws IOException {
    upload(sample());
    export();
    final Path old = out.resolveSibling(out.getFileName() + ".old-1");
    Files.move(out, old);
    export();
    assertThat(out.resolve("index.json")).exists();
    assertThat(old).doesNotExist();
  }

  @Test
  void namesDifferingOnlyByCaseGetDistinctFiles() {
    final RunResult base = sample();
    final CaseResult first = base.results().getFirst();
    final List<CaseResult> cases = new ArrayList<>();
    for (final String name : List.of("A", "a"))
      cases.add(new CaseResult(first.workload(), name, first.scale(), first.params(), first.reps(), first.warmup(), first.metrics(),
          first.status(), first.error()));
    upload(new RunResult(base.info(), cases));
    export();
    final JSONArray caseArray = read(out.resolve("index.json")).getJSONArray("workloads").getJSONObject(0).getJSONArray("cases");
    final List<String> files = new ArrayList<>();
    for (int i = 0; i < caseArray.length(); i++)
      files.add(caseArray.getJSONObject(i).getJSONArray("metrics").getJSONObject(0).getString("file"));
    assertThat(files).hasSize(2).doesNotHaveDuplicates();
    assertThat(files.stream().map(String::toLowerCase).distinct().count()).isEqualTo(2);
    files.forEach(f -> assertThat(out.resolve(f)).exists());
  }

  @Test
  void rowsWithoutMachineOrVariantAreSkipped() {
    upload(sample());
    try (final RemoteDatabase db = new RemoteDatabase(server.wrapper().host(), server.wrapper().httpPort(), "perf", "root",
        ServerUnderTest.PASSWORD)) {
      db.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);
      db.command("sql", "UPDATE Measurement REMOVE machineId WHERE metric = 'p99'").close();
    }
    export();
    // a series left without any line is not written at all
    assertThat(out.resolve("series/vector/knn-k10__M__p99.json")).doesNotExist();
    assertThat(out.resolve("series/vector/knn-k10__M__throughput.json")).exists();
    assertThat(read(out.resolve("index.json")).toString()).doesNotContain("p99");
  }
}
