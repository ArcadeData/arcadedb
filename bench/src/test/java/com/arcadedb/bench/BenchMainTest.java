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
package com.arcadedb.bench;

import com.arcadedb.bench.env.MachineProbe;
import com.arcadedb.bench.model.CaseResult;
import com.arcadedb.bench.model.RunResult;
import com.arcadedb.bench.runner.Measurement;
import com.arcadedb.bench.runner.Workload;
import com.arcadedb.bench.runner.WorkloadContext;
import com.arcadedb.bench.server.ServerUnderTest;
import com.arcadedb.bench.workload.Scale;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class BenchMainTest {
  @TempDir
  Path tmp;

  private record Fake(String name, List<String> cases) implements Workload {
    @Override
    public void prepare(final WorkloadContext ctx, final String caseName) {
    }

    @Override
    public Measurement measure(final WorkloadContext ctx, final String caseName) {
      return new Measurement(List.of());
    }
  }

  private static final List<Workload> REGISTRY = List.of(new Fake("alpha", List.of("a1", "a2")), new Fake("beta", List.of("b1")));

  private int run(final ByteArrayOutputStream buf, final String... args) {
    return BenchMain.run(args, REGISTRY, (image, cpuset, mem, heap) -> null,
        new PrintStream(buf, true, StandardCharsets.UTF_8));
  }

  @Test
  void listPrintsAllWorkloads() {
    final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    assertThat(run(buf, "list")).isZero();
    final String out = buf.toString(StandardCharsets.UTF_8);
    assertThat(out).contains("alpha  a1,a2").contains("beta  b1");
  }

  @Test
  void runWritesResultFile() throws Exception {
    final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    final int code = run(buf, "run", "--workload", "all", "--scale", "XS", "--image", "img:1", "--variant", "jvm", "--reps", "1",
        "--warmup", "0", "--skip-idle-check", "--server-cpus", "none", "--client-cpus", "none", "--out", tmp.toString());
    assertThat(code).isZero();
    try (final Stream<Path> files = Files.list(tmp)) {
      final List<Path> json = files.filter(p -> p.toString().endsWith(".json")).toList();
      assertThat(json).hasSize(1);
      final RunResult result = RunResult.read(json.getFirst());
      assertThat(result.info().arcadedb().getString("variant")).isEqualTo("jvm");
      assertThat(result.info().source()).isEqualTo("bench");
      assertThat(result.results()).hasSize(3);
      assertThat(result.info().config().getString("benchCommit")).isEqualTo("unknown");
    }
  }

  @Test
  void runFileIsRewrittenAfterEachCase() throws Exception {
    final List<RunResult> seen = new ArrayList<>();
    final Workload spy = new Workload() {
      @Override
      public String name() {
        return "spy";
      }

      @Override
      public List<String> cases() {
        return List.of("s1", "s2");
      }

      @Override
      public void prepare(final WorkloadContext ctx, final String caseName) {
      }

      @Override
      public Measurement measure(final WorkloadContext ctx, final String caseName) throws Exception {
        if (caseName.equals("s2"))
          try (final Stream<Path> files = Files.list(tmp)) {
            seen.add(RunResult.read(files.filter(p -> p.toString().endsWith(".json")).toList().getFirst()));
          }
        return new Measurement(List.of());
      }
    };
    final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    final int code = BenchMain.run(new String[] { "run", "--workload", "spy", "--scale", "XS", "--image", "img:1", "--variant", "jvm",
        "--reps", "1", "--warmup", "0", "--skip-idle-check", "--server-cpus", "none", "--client-cpus", "none", "--commit",
        "abc1234567", "--out", tmp.toString() }, List.of(spy), (image, cpuset, mem, heap) -> null, new PrintStream(buf, true, StandardCharsets.UTF_8));
    assertThat(code).isZero();
    assertThat(seen).hasSize(1);
    assertThat(seen.getFirst().complete()).isFalse();
    assertThat(seen.getFirst().results()).hasSize(1);
    try (final Stream<Path> files = Files.list(tmp)) {
      final List<Path> json = files.toList();
      assertThat(json).hasSize(1);
      final RunResult last = RunResult.read(json.getFirst());
      assertThat(last.complete()).isTrue();
      assertThat(last.results()).hasSize(2);
      assertThat(last.info().runId()).isEqualTo(seen.getFirst().info().runId());
    }
  }

  @Test
  void runIdWaitsForTheFirstServerThatReportsAnImage() throws Exception {
    final Map<String, String> options = Map.of();
    final BenchMain.RunFile file = new BenchMain.RunFile(tmp, Instant.parse("2026-11-23T03:00:00Z"), options, "jvm", "img:1",
        null, null, "manual", "local", 1, 0, 30, 30, 120, 42L, "none", "none");
    final CaseResult failed = new CaseResult("alpha", "a1", "XS", new JSONObject(), 1, 0, List.of(), CaseResult.Status.FAILED,
        "server start failed");
    final List<CaseResult> results = new ArrayList<>(List.of(failed));

    // the first workload's server failed to start: nothing is written and no id is frozen on "unknown"
    file.write(results, false);
    assertThat(file.runId).isNull();
    try (final Stream<Path> files = Files.list(tmp)) {
      assertThat(files.toList()).isEmpty();
    }

    // a later server reports its image: the next write carries its revision and everything gathered so far
    file.imageInfo = new JSONObject().put("revision", "0123456789abcdef");
    results.add(failed);
    file.write(results, false);
    file.write(results, true);
    try (final Stream<Path> files = Files.list(tmp)) {
      final List<Path> json = files.toList();
      assertThat(json).hasSize(1);
      final RunResult last = RunResult.read(json.getFirst());
      assertThat(last.info().runId()).contains("0123456789").doesNotContain("unknown");
      assertThat(last.complete()).isTrue();
      assertThat(last.results()).hasSize(2);
    }
  }

  @Test
  void benchCommitFlagLandsInConfigOfRun() throws Exception {
    final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    final int code = run(buf, "run", "--workload", "all", "--scale", "XS", "--image", "img:1", "--variant", "jvm", "--reps", "1",
        "--warmup", "0", "--skip-idle-check", "--server-cpus", "none", "--client-cpus", "none", "--bench-commit", "feedface", "--out",
        tmp.toString());
    assertThat(code).isZero();
    try (final Stream<Path> files = Files.list(tmp)) {
      final RunResult result = RunResult.read(files.filter(p -> p.toString().endsWith(".json")).toList().getFirst());
      assertThat(result.info().config().getString("benchCommit")).isEqualTo("feedface");
    }
  }

  @Test
  void tagFlagLandsInArcadedbObject() throws Exception {
    final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    final int code = run(buf, "run", "--workload", "all", "--scale", "XS", "--image", "img:1", "--variant", "jvm", "--reps", "1",
        "--warmup", "0", "--skip-idle-check", "--server-cpus", "none", "--client-cpus", "none", "--tag", "26.12.1", "--out",
        tmp.toString());
    assertThat(code).isZero();
    try (final Stream<Path> files = Files.list(tmp)) {
      final RunResult result = RunResult.read(files.filter(p -> p.toString().endsWith(".json")).toList().getFirst());
      assertThat(result.info().arcadedb().getString("tag")).isEqualTo("26.12.1");
    }
  }

  @Test
  void commitDateFlagLandsInArcadedbObjectAsAUtcInstant() throws Exception {
    final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    final int code = run(buf, "run", "--workload", "all", "--scale", "XS", "--image", "img:1", "--variant", "jvm", "--reps", "1",
        "--warmup", "0", "--skip-idle-check", "--server-cpus", "none", "--client-cpus", "none", "--commit-date",
        "2026-10-28T14:05:09+02:00", "--out", tmp.toString());
    assertThat(code).isZero();
    try (final Stream<Path> files = Files.list(tmp)) {
      final RunResult result = RunResult.read(files.filter(p -> p.toString().endsWith(".json")).toList().getFirst());
      assertThat(result.info().arcadedb().getString("commitDate")).isEqualTo("2026-10-28T12:05:09Z");
    }
  }

  @Test
  void withoutCommitDateTheArcadedbObjectHasNone() throws Exception {
    final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    assertThat(run(buf, "run", "--workload", "all", "--scale", "XS", "--image", "img:1", "--variant", "jvm", "--reps", "1",
        "--warmup", "0", "--skip-idle-check", "--server-cpus", "none", "--client-cpus", "none", "--out", tmp.toString())).isZero();
    try (final Stream<Path> files = Files.list(tmp)) {
      final RunResult result = RunResult.read(files.filter(p -> p.toString().endsWith(".json")).toList().getFirst());
      assertThat(result.info().arcadedb().has("commitDate")).isFalse();
    }
  }

  @Test
  void badCommitDateExits2BeforeAnythingRuns() throws Exception {
    final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    assertThat(run(buf, "run", "--workload", "all", "--scale", "XS", "--image", "img:1", "--variant", "jvm", "--reps", "1",
        "--warmup", "0", "--skip-idle-check", "--server-cpus", "none", "--client-cpus", "none", "--commit-date", "yesterday",
        "--out", tmp.toString())).isEqualTo(2);
    try (final Stream<Path> files = Files.list(tmp)) {
      assertThat(files.filter(p -> p.toString().endsWith(".json")).toList()).isEmpty();
    }
  }

  @Test
  void commitDateFlagLandsInArcadedbObjectOfLdbcConvert() throws Exception {
    final Path summary = tmp.resolve("mode1-summary.json");
    try (final InputStream in = BenchMainTest.class.getResourceAsStream("/mode1-summary.json")) {
      Files.write(summary, in.readAllBytes());
    }
    final Path out = tmp.resolve("out");
    final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    assertThat(run(buf, "ldbc-convert", "--summary", summary.toString(), "--dataset", "d", "--commit", "abc", "--commit-date",
        "2026-10-28T12:05:09Z", "--out", out.toString())).isZero();
    try (final Stream<Path> files = Files.list(out)) {
      final RunResult result = RunResult.read(files.filter(p -> p.toString().endsWith(".json")).toList().getFirst());
      assertThat(result.info().arcadedb().getString("commitDate")).isEqualTo("2026-10-28T12:05:09Z");
    }
  }

  @Test
  void badCommitDateOfLdbcConvertExits2() throws Exception {
    final Path summary = tmp.resolve("mode1-summary.json");
    try (final InputStream in = BenchMainTest.class.getResourceAsStream("/mode1-summary.json")) {
      Files.write(summary, in.readAllBytes());
    }
    final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    assertThat(run(buf, "ldbc-convert", "--summary", summary.toString(), "--dataset", "d", "--commit", "abc", "--commit-date",
        "2026-13-01", "--out", tmp.resolve("out").toString())).isEqualTo(2);
  }

  @Test
  void ldbcConvertWritesOneLdbcFile() throws Exception {
    final Path summary = tmp.resolve("mode1-summary.json");
    try (final InputStream in = BenchMainTest.class.getResourceAsStream("/mode1-summary.json")) {
      Files.write(summary, in.readAllBytes());
    }
    final Path out = tmp.resolve("out");
    final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    final int code = run(buf, "ldbc-convert", "--summary", summary.toString(), "--dataset", "datagen-7_5-fb", "--commit",
        "0123456789abcdef", "--machine-id", "perf-01", "--trigger", "nightly", "--tag", "26.12.1", "--out", out.toString());
    assertThat(code).isZero();
    try (final Stream<Path> files = Files.list(out)) {
      final List<Path> json = files.filter(p -> p.toString().endsWith(".json")).toList();
      assertThat(json).hasSize(1);
      final RunResult result = RunResult.read(json.getFirst());
      assertThat(result.info().source()).isEqualTo("ldbc");
      assertThat(result.info().runId()).endsWith("-0123456789-jvm-ldbc");
      assertThat(result.info().arcadedb().getString("tag")).isEqualTo("26.12.1");
      assertThat(result.info().config().getString("framework")).isEqualTo("graphalytics");
      assertThat(result.info().config().getString("benchCommit")).isEqualTo("unknown");
      assertThat(result.results()).hasSize(12);
    }
  }

  @Test
  void benchCommitFlagLandsInConfigOfLdbcConvert() throws Exception {
    final Path summary = tmp.resolve("mode1-summary.json");
    try (final InputStream in = BenchMainTest.class.getResourceAsStream("/mode1-summary.json")) {
      Files.write(summary, in.readAllBytes());
    }
    final Path out = tmp.resolve("out");
    final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    assertThat(run(buf, "ldbc-convert", "--summary", summary.toString(), "--dataset", "d", "--commit", "abc", "--bench-commit",
        "feedface", "--out", out.toString())).isZero();
    try (final Stream<Path> files = Files.list(out)) {
      final RunResult result = RunResult.read(files.filter(p -> p.toString().endsWith(".json")).toList().getFirst());
      assertThat(result.info().config().getString("benchCommit")).isEqualTo("feedface");
    }
  }

  @Test
  void ldbcConvertMissingSummaryExits2() {
    final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    assertThat(run(buf, "ldbc-convert", "--summary", tmp.resolve("nope.json").toString(), "--dataset", "d", "--commit", "abc",
        "--out", tmp.toString())).isEqualTo(2);
  }

  @Test
  void unknownWorkloadExits2() {
    final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    assertThat(run(buf, "run", "--workload", "nope", "--scale", "XS", "--image", "i", "--variant", "jvm", "--skip-idle-check",
        "--out", tmp.toString())).isEqualTo(2);
  }

  @Test
  void badScaleExits2() {
    final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    assertThat(run(buf, "run", "--workload", "all", "--scale", "HUGE", "--image", "i", "--variant", "jvm", "--skip-idle-check",
        "--out", tmp.toString())).isEqualTo(2);
  }

  @Test
  void machineProbeDoesNotThrowHere() {
    final JSONObject probe = MachineProbe.probe("t");
    assertThat(probe.has("cpu")).isTrue();
  }

  // #12839: the step, prepare and case limits are separate, and the run records all three
  @Test
  void timeoutFlagsLandInConfigOfRun() throws Exception {
    final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    final int code = run(buf, "run", "--workload", "all", "--scale", "XS", "--image", "img:1", "--variant", "jvm", "--reps", "1",
        "--warmup", "0", "--skip-idle-check", "--server-cpus", "none", "--client-cpus", "none", "--timeout-min", "20",
        "--prepare-timeout-min", "45", "--case-timeout-min", "0", "--out", tmp.toString());
    assertThat(code).isZero();
    try (final Stream<Path> files = Files.list(tmp)) {
      final RunResult result = RunResult.read(files.filter(p -> p.toString().endsWith(".json")).toList().getFirst());
      assertThat(result.info().config().getInt("timeoutMin")).isEqualTo(20);
      assertThat(result.info().config().getInt("prepareTimeoutMin")).isEqualTo(45);
      assertThat(result.info().config().getInt("caseTimeoutMin")).isZero();
    }
  }

  @Test
  void prepareTimeoutDefaultsToTheStepTimeoutAndTheCaseCapTo120Minutes() throws Exception {
    final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    final int code = run(buf, "run", "--workload", "all", "--scale", "XS", "--image", "img:1", "--variant", "jvm", "--reps", "1",
        "--warmup", "0", "--skip-idle-check", "--server-cpus", "none", "--client-cpus", "none", "--timeout-min", "25", "--out",
        tmp.toString());
    assertThat(code).isZero();
    try (final Stream<Path> files = Files.list(tmp)) {
      final RunResult result = RunResult.read(files.filter(p -> p.toString().endsWith(".json")).toList().getFirst());
      assertThat(result.info().config().getInt("prepareTimeoutMin")).isEqualTo(25);
      assertThat(result.info().config().getInt("caseTimeoutMin")).isEqualTo(120);
    }
  }

  @Test
  void aNegativeCaseTimeoutExits2() {
    final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    assertThat(run(buf, "run", "--workload", "all", "--scale", "XS", "--image", "i", "--variant", "jvm", "--skip-idle-check",
        "--case-timeout-min", "-1", "--out", tmp.toString())).isEqualTo(2);
  }

  @Test
  void badParamsExits2() {
    final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    assertThat(run(buf, "run", "--workload", "all", "--scale", "XS", "--image", "i", "--variant", "jvm", "--skip-idle-check",
        "--params", "{not json", "--out", tmp.toString())).isEqualTo(2);
  }

  @Test
  void serverStartFailureIsRecordedAndRunContinues() throws Exception {
    final int[] calls = { 0 };
    final int code = BenchMain.run(new String[] { "run", "--workload", "all", "--scale", "XS", "--image", "i", "--variant", "jvm",
        "--reps", "1", "--warmup", "0", "--skip-idle-check", "--server-cpus", "none", "--client-cpus", "none", "--out",
        tmp.toString() }, REGISTRY, (image, cpuset, mem, heap) -> {
      if (calls[0]++ == 0)
        throw new IllegalStateException("boom");
      return null;
    }, new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8));
    assertThat(code).isZero();
    try (final Stream<Path> files = Files.list(tmp)) {
      final RunResult result = RunResult.read(files.filter(p -> p.toString().endsWith(".json")).findFirst().orElseThrow());
      assertThat(result.results()).hasSize(3);
      assertThat(result.results().subList(0, 2)).allSatisfy(c -> {
        assertThat(c.workload()).isEqualTo("alpha");
        assertThat(c.status()).isEqualTo(CaseResult.Status.FAILED);
        assertThat(c.error()).contains("server start failed");
      });
      assertThat(result.results().get(2).workload()).isEqualTo("beta");
      assertThat(result.results().get(2).status()).isEqualTo(CaseResult.Status.OK);
    }
  }

  /** A workload whose case list at S leaves out every case but the first. */
  private static Workload firstCaseOnlyAtS(final String name, final List<String> all) {
    return new Workload() {
      @Override
      public String name() {
        return name;
      }

      @Override
      public List<String> cases() {
        return all;
      }

      @Override
      public List<String> cases(final Scale scale) {
        return scale == Scale.S ? all.subList(0, 1) : all;
      }

      @Override
      public void prepare(final WorkloadContext ctx, final String caseName) {
      }

      @Override
      public Measurement measure(final WorkloadContext ctx, final String caseName) {
        return new Measurement(List.of());
      }
    };
  }

  @Test
  void casesAreTheOnesOfTheRunScaleWhetherTheServerStartsOrNot() throws Exception {
    final int[] calls = { 0 };
    final int code = BenchMain.run(new String[] { "run", "--workload", "all", "--scale", "S", "--image", "i", "--variant", "jvm",
            "--reps", "1", "--warmup", "0", "--skip-idle-check", "--server-cpus", "none", "--client-cpus", "none", "--out",
            tmp.toString() }, List.of(firstCaseOnlyAtS("alpha", List.of("a1", "a2")), firstCaseOnlyAtS("beta", List.of("b1", "b2"))),
        (image, cpuset, mem, heap) -> {
          if (calls[0]++ == 0)
            throw new IllegalStateException("boom");
          return null;
        }, new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8));
    assertThat(code).isZero();
    try (final Stream<Path> files = Files.list(tmp)) {
      final RunResult result = RunResult.read(files.filter(p -> p.toString().endsWith(".json")).findFirst().orElseThrow());
      // alpha's server failed to start, beta's ran: either way a case left out at S is not recorded
      assertThat(result.results()).extracting(CaseResult::caseName).containsExactly("a1", "b1");
      assertThat(result.results().get(0).status()).isEqualTo(CaseResult.Status.FAILED);
      assertThat(result.results().get(1).status()).isEqualTo(CaseResult.Status.OK);
    }
  }

  /** Counts server starts and answers each liveness check from a script, one entry per call (null = alive). */
  private static final class DyingServers implements BenchMain.ServerFactory {
    final List<String>  reasons;
    final List<Boolean> histogramsAsked = new ArrayList<>();
    int starts;
    int checks;
    int diagnosed;

    DyingServers(final String... reasons) {
      this.reasons = Arrays.asList(reasons);
    }

    @Override
    public ServerUnderTest start(final String image, final String cpuset, final int memLimitGb, final String heap) {
      starts++;
      return null;
    }

    @Override
    public String deathReason(final ServerUnderTest server) {
      final int i = checks++;
      return i < reasons.size() ? reasons.get(i) : null;
    }

    /** Numbers each server's diagnostics in the order they are taken, and records whether a class histogram was asked for. */
    @Override
    public JSONObject diagnostics(final ServerUnderTest server, final boolean classHistogram) {
      histogramsAsked.add(classHistogram);
      return new JSONObject().put("server", ++diagnosed);
    }
  }

  private int runAll(final List<Workload> registry, final BenchMain.ServerFactory servers, final Path out) {
    return BenchMain.run(new String[] { "run", "--workload", "all", "--scale", "XS", "--image", "i", "--variant", "jvm", "--reps", "1",
        "--warmup", "0", "--skip-idle-check", "--server-cpus", "none", "--client-cpus", "none", "--commit", "abc1234567", "--out",
        out.toString() }, registry, servers, new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8));
  }

  private static RunResult onlyRunFile(final Path dir) throws Exception {
    try (final Stream<Path> files = Files.list(dir)) {
      return RunResult.read(files.filter(p -> p.toString().endsWith(".json")).findFirst().orElseThrow());
    }
  }

  @Test
  void aReplacedServerKeepsItsDiagnosticsUnderANumberedKey() throws Exception {
    final List<Workload> registry = List.of(new Fake("alpha", List.of("a1", "a2", "a3")), new Fake("beta", List.of("b1")));
    // alpha's server dies before a2 and is replaced; the replacement serves a2 and a3
    final DyingServers servers = new DyingServers("OOMKilled");
    assertThat(runAll(registry, servers, tmp)).isZero();
    final JSONObject diagnostics = onlyRunFile(tmp).diagnostics();
    assertThat(diagnostics.keySet()).containsExactlyInAnyOrder("alpha#1", "alpha", "beta");
    assertThat(diagnostics.getJSONObject("alpha#1").getInt("server", 0)).isEqualTo(1);
    assertThat(diagnostics.getJSONObject("alpha").getInt("server", 0)).isEqualTo(2);
    assertThat(diagnostics.getJSONObject("beta").getInt("server", 0)).isEqualTo(3);
  }

  @Test
  void aServerNotAnsweringGetsNoClassHistogram() throws Exception {
    final List<Workload> registry = List.of(new Fake("alpha", List.of("a1", "a2")));
    // jcmd against a JVM that does not answer HTTP would most likely hang too
    final DyingServers servers = new DyingServers(ServerUnderTest.NOT_ANSWERING);
    assertThat(runAll(registry, servers, tmp)).isZero();
    // the dead server, then its healthy replacement
    assertThat(servers.histogramsAsked).containsExactly(false, true);
  }

  @Test
  void aFailedInterimWriteIsAWarningAndTheRunGoesOn() throws Exception {
    // the output path is taken by a file until the last case removes it: every interim write fails, the final one succeeds
    final Path out = tmp.resolve("out");
    Files.writeString(out, "in the way");
    final List<String> measured = new ArrayList<>();
    final Workload clearing = new Workload() {
      @Override
      public String name() {
        return "alpha";
      }

      @Override
      public List<String> cases() {
        return List.of("a1", "a2", "a3");
      }

      @Override
      public void prepare(final WorkloadContext ctx, final String caseName) {
      }

      @Override
      public Measurement measure(final WorkloadContext ctx, final String caseName) throws Exception {
        measured.add(caseName);
        if (caseName.equals("a3"))
          Files.delete(out);
        return new Measurement(List.of());
      }
    };
    assertThat(runAll(List.of(clearing), (image, cpuset, mem, heap) -> null, out)).isZero();
    assertThat(measured).containsExactly("a1", "a2", "a3");
    final RunResult result = onlyRunFile(out);
    assertThat(result.complete()).isTrue();
    assertThat(result.results()).hasSize(3);
  }

  @Test
  void aFailedFinalWriteExitsNonZero() throws Exception {
    final Path out = tmp.resolve("out");
    Files.writeString(out, "in the way");
    final List<String> measured = new ArrayList<>();
    final Workload w = new Workload() {
      @Override
      public String name() {
        return "alpha";
      }

      @Override
      public List<String> cases() {
        return List.of("a1", "a2");
      }

      @Override
      public void prepare(final WorkloadContext ctx, final String caseName) {
      }

      @Override
      public Measurement measure(final WorkloadContext ctx, final String caseName) {
        measured.add(caseName);
        return new Measurement(List.of());
      }
    };
    assertThat(runAll(List.of(w), (image, cpuset, mem, heap) -> null, out)).isEqualTo(1);
    assertThat(measured).containsExactly("a1", "a2");
  }

  @Test
  void nextWorkloadGetsAFreshServerAfterTwoDeaths() throws Exception {
    final List<Workload> registry = List.of(new Fake("alpha", List.of("a1", "a2", "a3")), new Fake("beta", List.of("b1", "b2")));
    // dead before a2 (restart), dead before a3 (second death: a3 not started); beta alive throughout
    final DyingServers servers = new DyingServers("OOMKilled", "exit code 137");
    final int code = BenchMain.run(new String[] { "run", "--workload", "all", "--scale", "XS", "--image", "i", "--variant", "jvm",
        "--reps", "1", "--warmup", "0", "--skip-idle-check", "--server-cpus", "none", "--client-cpus", "none", "--commit",
        "abc1234567", "--out", tmp.toString() }, registry, servers, new PrintStream(new ByteArrayOutputStream(), true,
        StandardCharsets.UTF_8));
    assertThat(code).isZero();
    // alpha's first server, its restart, then beta's own
    assertThat(servers.starts).isEqualTo(3);
    try (final Stream<Path> files = Files.list(tmp)) {
      final RunResult result = RunResult.read(files.filter(p -> p.toString().endsWith(".json")).findFirst().orElseThrow());
      final List<CaseResult> r = result.results();
      assertThat(r).extracting(CaseResult::caseName).containsExactly("a1", "a2", "a3", "b1", "b2");
      // the amended results replace the earlier ones in the file
      assertThat(r.get(0).notes().getString("serverDied", "")).isEqualTo("OOMKilled");
      assertThat(r.get(1).notes().getString("serverDied", "")).isEqualTo("exit code 137");
      assertThat(r.get(1).notes().getBoolean("serverRestarted", false)).isTrue();
      assertThat(r.get(2).status()).isEqualTo(CaseResult.Status.FAILED);
      assertThat(r.get(2).error()).isEqualTo("not started: server died twice");
      assertThat(r.subList(3, 5)).allSatisfy(c -> {
        assertThat(c.status()).isEqualTo(CaseResult.Status.OK);
        assertThat(c.notes().has("serverDied")).isFalse();
        assertThat(c.notes().has("serverRestarted")).isFalse();
      });
    }
  }

  @Test
  void diagnosticsLandUnderTheWorkloadName() throws Exception {
    final BenchMain.ServerFactory servers = new BenchMain.ServerFactory() {
      @Override
      public ServerUnderTest start(final String image, final String cpuset, final int memLimitGb, final String heap) {
        return null;
      }

      @Override
      public JSONObject diagnostics(final ServerUnderTest server, final boolean classHistogram) {
        return new JSONObject().put("threads", 7);
      }
    };
    final int code = BenchMain.run(new String[] { "run", "--workload", "all", "--scale", "XS", "--image", "i", "--variant", "jvm",
        "--reps", "1", "--warmup", "0", "--skip-idle-check", "--server-cpus", "none", "--client-cpus", "none", "--commit",
        "abc1234567", "--out", tmp.toString() }, REGISTRY, servers, new PrintStream(new ByteArrayOutputStream(), true,
        StandardCharsets.UTF_8));
    assertThat(code).isZero();
    try (final Stream<Path> files = Files.list(tmp)) {
      final RunResult result = RunResult.read(files.filter(p -> p.toString().endsWith(".json")).findFirst().orElseThrow());
      assertThat(result.diagnostics().keySet()).containsExactlyInAnyOrder("alpha", "beta");
      assertThat(result.diagnostics().getJSONObject("alpha").getInt("threads", 0)).isEqualTo(7);
      assertThat(result.diagnostics().getJSONObject("beta").getInt("threads", 0)).isEqualTo(7);
    }
  }

  /** A run file and the guard of workload "alpha" over it, whose thread dumps write their file name as their content. */
  private BenchMain.Guard dumpingGuard(final BenchMain.RunFile file) {
    final BenchMain.ServerFactory servers = new BenchMain.ServerFactory() {
      @Override
      public ServerUnderTest start(final String image, final String cpuset, final int memLimitGb, final String heap) {
        return null;
      }

      @Override
      public boolean threadDump(final ServerUnderTest server, final Path target) {
        try {
          Files.writeString(target, target.getFileName().toString());
        } catch (final Exception e) {
          throw new IllegalStateException(e);
        }
        return true;
      }
    };
    return new BenchMain.Guard(servers, null, "alpha", file, "i", "", 1, "1g", Scale.XS, 1L, new JSONObject());
  }

  private BenchMain.RunFile runFile(final Map<String, String> options) {
    return new BenchMain.RunFile(tmp, Instant.parse("2026-11-23T03:00:00Z"), options, "jvm", "i", null, null, "manual", "local", 1, 0,
        1, 1, 1, 1L, "", "");
  }

  @Test
  void threadDumpIsNamedAfterTheRunId() throws Exception {
    final BenchMain.RunFile file = runFile(Map.of("commit", "abc1234567"));
    assertThat(dumpingGuard(file).threadDump(2)).isTrue();
    assertThat(tmp.resolve("dumps").resolve("2026-11-23T03-00-00Z-abc1234567-jvm-2.txt")).exists();
  }

  @Test
  void dumpTakenBeforeTheRunIdIsKnownIsRenamedOnceItIs() throws Exception {
    // no --commit and no image info yet: the run id cannot be formed when the dump is taken
    final BenchMain.RunFile file = runFile(Map.of());
    assertThat(dumpingGuard(file).threadDump(1)).isTrue();
    try (final Stream<Path> dumps = Files.list(tmp.resolve("dumps"))) {
      assertThat(dumps.map(p -> p.getFileName().toString()).toList()).containsExactly("2026-11-23T03-00-00Z-pending-jvm-1.txt");
    }
    file.imageInfo = new JSONObject().put("revision", "fedcba9876");
    file.write(List.of(), false);
    try (final Stream<Path> dumps = Files.list(tmp.resolve("dumps"))) {
      assertThat(dumps.map(p -> p.getFileName().toString()).toList()).containsExactly("2026-11-23T03-00-00Z-fedcba9876-jvm-1.txt");
    }
  }

  @Test
  void initStoreWithoutPasswordVariableExits2() {
    final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    assertThat(run(buf, "init-store", "--url", "http://localhost:1", "--root-password-env", "BENCH_TEST_SURELY_UNSET_VAR",
        "--writer-password-env", "BENCH_TEST_SURELY_UNSET_VAR", "--reader-password-env", "BENCH_TEST_SURELY_UNSET_VAR")).isEqualTo(2);
  }

  @Test
  void uploadWithoutPasswordVariableExits2() {
    final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    assertThat(run(buf, "upload", "--url", "http://localhost:1", "--user", "perf-writer", "--password-env",
        "BENCH_TEST_SURELY_UNSET_VAR", tmp.toString())).isEqualTo(2);
  }

  @Test
  void uploadWithoutPathExits2() {
    final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    assertThat(run(buf, "upload", "--url", "http://localhost:1", "--user", "perf-writer", "--password-env", "PATH")).isEqualTo(2);
  }

  @Test
  void replaceReleaseIsAFlagThatTakesNoValue() {
    // the flag before the path: were it to take a value it would swallow the path, and the upload would miss its target
    final String[] args = { "upload", "--url", "u", "--user", "perf-writer", "--password-env", "P", "--replace-release", "dir" };
    assertThat(BenchMain.parse(args, true, BenchMain.UPLOAD_OPTIONS)).containsEntry("replace-release", "true").containsEntry("", "dir");
  }

  @Test
  void uploadOfMissingPathExits2() {
    final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    assertThat(run(buf, "upload", "--url", "http://localhost:1", "--user", "perf-writer", "--password-env", "PATH",
        tmp.resolve("nope").toString())).isEqualTo(2);
  }

  @Test
  void uploadWithUnreachableServerExits1AndNamesTheFile() throws Exception {
    // from the classpath: CI runs this shard with -Pfork-isolation, whose working directory is not the module
    try (final InputStream in = BenchMainTest.class.getResourceAsStream("/run-sample.json")) {
      final byte[] sample = in.readAllBytes();
      Files.write(tmp.resolve("b.json"), sample);
      Files.write(tmp.resolve("a.json"), sample);
    }
    final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    final PrintStream savedErr = System.err;
    final ByteArrayOutputStream err = new ByteArrayOutputStream();
    System.setErr(new PrintStream(err, true, StandardCharsets.UTF_8));
    try {
      assertThat(run(buf, "upload", "--url", "http://127.0.0.1:1", "--user", "perf-writer", "--password-env", "PATH",
          tmp.toString())).isEqualTo(1);
    } finally {
      System.setErr(savedErr);
    }
    final String text = err.toString(StandardCharsets.UTF_8);
    assertThat(text).contains("a.json").contains("b.json");
    assertThat(text.indexOf("a.json")).isLessThan(text.indexOf("b.json"));
  }

  @Test
  void unknownOptionIsRejectedWithItsName() {
    final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    final PrintStream previous = System.err;
    final ByteArrayOutputStream err = new ByteArrayOutputStream();
    System.setErr(new PrintStream(err, true, StandardCharsets.UTF_8));
    try {
      assertThat(run(buf, "run", "--workload", "all", "--scale", "XS", "--image", "i", "--variant", "jvm", "--skip-idle-check",
          "--bench-comit", "abc", "--out", tmp.toString())).isEqualTo(2);
    } finally {
      System.setErr(previous);
    }
    assertThat(err.toString(StandardCharsets.UTF_8)).contains("--bench-comit");
    assertThat(run(buf, "export", "--url", "u", "--user", "x", "--password-env", "P", "--out", "o", "--typo", "1")).isEqualTo(2);
    assertThat(run(buf, "init-store", "--url", "u", "--nope", "1")).isEqualTo(2);
    assertThat(run(buf, "ldbc-convert", "--summary", "s", "--bogus", "1")).isEqualTo(2);
    assertThat(run(buf, "upload", "--url", "u", "--user", "x", "--password-env", "P", "--oops", "1", "dir")).isEqualTo(2);
  }

  @Test
  void everyFlagTheInfraScriptsPassIsAccepted() {
    // the flags scripts/run-all.sh and the workflows of arcadedb-perf-infra pass to the jar
    final String[] run = { "run", "--workload", "w", "--scale", "M", "--image", "i", "--variant", "jvm", "--trigger", "nightly",
        "--machine-id", "perf-01", "--commit", "c", "--server-cpus", "auto", "--skip-idle-check", "--out", "o", "--version", "v",
        "--tag", "t", "--bench-commit", "b", "--commit-date", "2026-01-01T00:00:00Z", "--reps", "3", "--warmup", "1" };
    assertThat(BenchMain.parse(run, false, BenchMain.RUN_OPTIONS)).containsKeys("workload", "bench-commit", "skip-idle-check",
        "commit-date", "reps", "warmup");
    final String[] convert = { "ldbc-convert", "--summary", "s", "--dataset", "d", "--commit", "c", "--machine-id", "m",
        "--trigger", "t", "--out", "o", "--started-at", "2026-01-01T00:00:00Z", "--version", "v", "--tag", "t", "--bench-commit",
        "b", "--commit-date", "2026-01-01T00:00:00Z" };
    assertThat(BenchMain.parse(convert, false, BenchMain.LDBC_CONVERT_OPTIONS)).containsKeys("summary", "started-at", "commit-date");
    final String[] upload = { "upload", "dir", "--url", "u", "--user", "perf-writer", "--password-env", "P", "--replace-release" };
    assertThat(BenchMain.parse(upload, true, BenchMain.UPLOAD_OPTIONS)).containsKeys("", "url").containsEntry("replace-release", "true");
    final String[] export = { "export", "--url", "u", "--user", "perf-reader", "--password-env", "P", "--out", "o" };
    assertThat(BenchMain.parse(export, false, BenchMain.EXPORT_OPTIONS)).containsKeys("out");
    final String[] init = { "init-store", "--url", "u", "--root-user", "root", "--root-password-env", "R", "--writer-password-env",
        "W", "--reader-password-env", "E" };
    assertThat(BenchMain.parse(init, false, BenchMain.INIT_STORE_OPTIONS)).containsKeys("root-password-env");
    final String[] gate = { "gate", "--data", "d", "--issues", "i.json", "--bench-changes", "b.json", "--out", "plan.json" };
    assertThat(BenchMain.parse(gate, false, BenchMain.GATE_OPTIONS)).containsKeys("data", "issues", "bench-changes", "out");
    final String[] gatePairs = { "gate-pairs", "--data", "d", "--issues", "i.json" };
    assertThat(BenchMain.parse(gatePairs, false, BenchMain.GATE_PAIRS_OPTIONS)).containsKeys("data", "issues");
  }

  @Test
  void gateOptionsAreKnown() {
    // every option the gate commands declare parses, so the set is the authority and a new option cannot be left unparseable
    for (final Set<String> known : List.of(BenchMain.GATE_OPTIONS, BenchMain.GATE_PAIRS_OPTIONS)) {
      final List<String> args = new ArrayList<>();
      args.add("gate");
      for (final String option : known) {
        args.add("--" + option);
        args.add("v");
      }
      assertThat(BenchMain.parse(args.toArray(new String[0]), false, known)).containsOnlyKeys(known);
    }
    // the scan still reaches the options the gate workflow passes
    assertThat(BenchMain.GATE_OPTIONS).as("BenchMain.GATE_OPTIONS still reaches every option the gate workflow passes")
        .contains("data", "issues", "bench-changes", "out");
    assertThat(BenchMain.GATE_PAIRS_OPTIONS).as("BenchMain.GATE_PAIRS_OPTIONS still reaches every option the gate workflow passes")
        .contains("data", "issues");
  }

  /** A nightly run file of one case with one "ms" metric, lower is better. */
  private static String nightlyRun(final String runId, final String commit, final String startedAt, final double median) {
    return "{\"runId\":\"" + runId + "\",\"trigger\":\"nightly\",\"startedAt\":\"" + startedAt + "\",\"arcadedb\":{\"commit\":\""
        + commit + "\",\"variant\":\"jvm\"},\"machine\":{\"id\":\"perf-01\"},\"config\":{\"benchCommit\":\"bench-" + commit
        + "\"},\"results\":[{\"workload\":\"w\",\"case\":\"c\",\"scale\":\"M\",\"status\":\"ok\",\"metrics\":[{\"name\":\"time\","
        + "\"unit\":\"ms\",\"better\":\"lower\",\"median\":" + median + ",\"values\":[" + median + "," + median + "]}]}]}";
  }

  private Path nightlyPairWorse() throws Exception {
    final Path data = Files.createDirectories(tmp.resolve("data"));
    Files.writeString(data.resolve("2026-11-20T03-00-00Z-aaaaaaaaaa-jvm.json"),
        nightlyRun("2026-11-20T03-00-00Z-aaaaaaaaaa-jvm", "aaaaaaaaaa", "2026-11-20T03:00:00Z", 100));
    Files.writeString(data.resolve("2026-11-21T03-00-00Z-bbbbbbbbbb-jvm.json"),
        nightlyRun("2026-11-21T03-00-00Z-bbbbbbbbbb-jvm", "bbbbbbbbbb", "2026-11-21T03:00:00Z", 200));
    return data;
  }

  @Test
  void gateWritesAPlan() throws Exception {
    final Path data = nightlyPairWorse();
    final Path issues = Files.writeString(tmp.resolve("issues.json"), "[]");
    final Path out = tmp.resolve("plan.json");
    final ByteArrayOutputStream buf = new ByteArrayOutputStream();

    assertThat(run(buf, "gate", "--data", data.toString(), "--issues", issues.toString(), "--out", out.toString())).isZero();

    final JSONObject plan = new JSONObject(Files.readString(out));
    assertThat(plan.getString("label")).isEqualTo("performance-regression");
    final JSONArray actions = plan.getJSONArray("actions");
    assertThat(actions.length()).isEqualTo(1);
    assertThat(actions.getJSONObject(0).getString("op")).isEqualTo("create");
    assertThat(actions.getJSONObject(0).getString("title")).contains("aaaaaaa..bbbbbbb");
    assertThat(buf.toString(StandardCharsets.UTF_8)).isEqualTo("plan: 1 create, 0 update, 0 comment, 0 close" + System.lineSeparator());
  }

  @Test
  void gateReadsTheBenchChangesFile() throws Exception {
    final Path data = nightlyPairWorse();
    final Path issues = Files.writeString(tmp.resolve("issues.json"), "[]");
    final Path bench = Files.writeString(tmp.resolve("bench-changes.json"), "{\"bench-aaaaaaaaaa..bench-bbbbbbbbbb\": false}");
    final Path withAnswer = tmp.resolve("with.json");
    final Path without = tmp.resolve("without.json");
    final ByteArrayOutputStream buf = new ByteArrayOutputStream();

    assertThat(run(buf, "gate", "--data", data.toString(), "--issues", issues.toString(), "--bench-changes", bench.toString(), "--out",
        withAnswer.toString())).isZero();
    assertThat(run(buf, "gate", "--data", data.toString(), "--issues", issues.toString(), "--out", without.toString())).isZero();

    assertThat(Files.readString(withAnswer)).isNotEqualTo(Files.readString(without));
  }

  @Test
  void gateWithoutARequiredOptionExits2() throws Exception {
    final Path data = nightlyPairWorse();
    final Path issues = Files.writeString(tmp.resolve("issues.json"), "[]");
    final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    final ByteArrayOutputStream err = new ByteArrayOutputStream();
    final PrintStream previous = System.err;
    System.setErr(new PrintStream(err, true, StandardCharsets.UTF_8));
    try {
      assertThat(run(buf, "gate", "--data", data.toString(), "--issues", issues.toString())).isEqualTo(2);
      assertThat(run(buf, "gate", "--data", data.toString(), "--out", tmp.resolve("p.json").toString())).isEqualTo(2);
      assertThat(run(buf, "gate", "--issues", issues.toString(), "--out", tmp.resolve("p.json").toString())).isEqualTo(2);
      assertThat(run(buf, "gate-pairs")).isEqualTo(2);
      assertThat(run(buf, "gate", "--data", data.toString(), "--issues", issues.toString(), "--out", "p.json", "--typo", "x"))
          .isEqualTo(2);
    } finally {
      System.setErr(previous);
    }
    assertThat(err.toString(StandardCharsets.UTF_8)).contains("--out").contains("--issues").contains("--data").contains("--typo");
    assertThat(Files.exists(tmp.resolve("p.json"))).isFalse();
  }

  @Test
  void gateWithAnIssuesFileThatIsNotAnArrayExits2() throws Exception {
    final Path data = nightlyPairWorse();
    final Path issues = Files.writeString(tmp.resolve("issues.json"), "{}");
    final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    final PrintStream previous = System.err;
    System.setErr(new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8));
    try {
      assertThat(run(buf, "gate", "--data", data.toString(), "--issues", issues.toString(), "--out", tmp.resolve("p.json").toString()))
          .isEqualTo(2);
    } finally {
      System.setErr(previous);
    }
  }

  @Test
  void gateWarnsOnStderrAndStillExits0() throws Exception {
    final Path data = nightlyPairWorse();
    Files.writeString(data.resolve("broken.json"), "{ not json");
    final Path issues = Files.writeString(tmp.resolve("issues.json"), "[{\"number\": 7, \"state\": \"open\", \"body\": \"no marker\"}]");
    final Path out = tmp.resolve("plan.json");
    final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    final ByteArrayOutputStream err = new ByteArrayOutputStream();
    final PrintStream previous = System.err;
    System.setErr(new PrintStream(err, true, StandardCharsets.UTF_8));
    try {
      assertThat(run(buf, "gate", "--data", data.toString(), "--issues", issues.toString(), "--out", out.toString())).isZero();
    } finally {
      System.setErr(previous);
    }
    assertThat(err.toString(StandardCharsets.UTF_8)).contains("broken.json").contains("#7");
    assertThat(buf.toString(StandardCharsets.UTF_8)).doesNotContain("broken.json").doesNotContain("#7");
    assertThat(new JSONObject(Files.readString(out)).getJSONArray("actions").length()).isEqualTo(1);
  }

  @Test
  void gatePairsPrintsEachNeededPair() throws Exception {
    final Path data = nightlyPairWorse();
    final ByteArrayOutputStream buf = new ByteArrayOutputStream();

    assertThat(run(buf, "gate-pairs", "--data", data.toString())).isZero();

    assertThat(buf.toString(StandardCharsets.UTF_8)).isEqualTo("bench-aaaaaaaaaa bench-bbbbbbbbbb" + System.lineSeparator());
  }

  @Test
  void gatePairsCoversTheLinesOfOpenIssues() throws Exception {
    // the issue follows the line from a baseline whose bench commit no verdict of the data has
    final Path data = nightlyPairWorse();
    final Path issuesFile = tmp.resolve("issues.json");
    final Path out = tmp.resolve("plan.json");
    final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    Files.writeString(issuesFile, "[]");
    assertThat(run(buf, "gate", "--data", data.toString(), "--issues", issuesFile.toString(), "--out", out.toString())).isZero();
    final String body = new JSONObject(Files.readString(out)).getJSONArray("actions").getJSONObject(0).getString("body")
        .replace("bench-aaaaaaaaaa", "bench-0000000000");
    Files.writeString(issuesFile, new JSONArray().put(new JSONObject().put("number", 1).put("state", "open").put("body", body)).toString());
    Files.writeString(data.resolve("2026-11-22T03-00-00Z-cccccccccc-jvm.json"),
        nightlyRun("2026-11-22T03-00-00Z-cccccccccc-jvm", "cccccccccc", "2026-11-22T03:00:00Z", 200));

    final ByteArrayOutputStream pairs = new ByteArrayOutputStream();
    assertThat(run(pairs, "gate-pairs", "--data", data.toString(), "--issues", issuesFile.toString())).isZero();
    // the body row shows the recorded baseline against the run the line was reported worse in
    assertThat(pairs.toString(StandardCharsets.UTF_8)).contains("bench-0000000000 bench-bbbbbbbbbb");

    final ByteArrayOutputStream noIssues = new ByteArrayOutputStream();
    assertThat(run(noIssues, "gate-pairs", "--data", data.toString())).isZero();
    assertThat(noIssues.toString(StandardCharsets.UTF_8)).doesNotContain("bench-0000000000");
  }

  @Test
  void failingServerIntrospectionFallsBackInsteadOfAborting() {
    final JSONObject fallback = new JSONObject().put("x", 1);
    assertThat(BenchMain.orFallback(() -> {
      throw new IllegalStateException("docker gone");
    }, fallback, "server config")).isSameAs(fallback);
    assertThat(BenchMain.orFallback(() -> null, fallback, "image info")).isNull();
    final JSONObject ok = new JSONObject().put("y", 2);
    assertThat(BenchMain.orFallback(() -> ok, fallback, "image info")).isSameAs(ok);
  }
}
