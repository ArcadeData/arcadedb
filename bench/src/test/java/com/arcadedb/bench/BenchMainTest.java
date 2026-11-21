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
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
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
