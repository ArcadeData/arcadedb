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

import com.arcadedb.bench.env.CpuTopology;
import com.arcadedb.bench.env.IdleCheck;
import com.arcadedb.bench.env.MachineProbe;
import com.arcadedb.bench.env.Processes;
import com.arcadedb.bench.gate.BenchChanges;
import com.arcadedb.bench.gate.Planner;
import com.arcadedb.bench.gate.RawData;
import com.arcadedb.bench.gate.TrackedIssue;
import com.arcadedb.bench.ldbc.LdbcAdapter;
import com.arcadedb.bench.model.CaseResult;
import com.arcadedb.bench.model.RunInfo;
import com.arcadedb.bench.model.RunResult;
import com.arcadedb.bench.runner.BenchRunner;
import com.arcadedb.bench.runner.Workload;
import com.arcadedb.bench.runner.WorkloadContext;
import com.arcadedb.bench.server.ServerUnderTest;
import com.arcadedb.bench.store.Exporter;
import com.arcadedb.bench.store.ResultsStore;
import com.arcadedb.bench.store.Uploader;
import com.arcadedb.bench.workload.Scale;
import com.arcadedb.bench.workload.WorkloadRegistry;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Command line entry point: {@code bench run ...}, {@code bench list}, {@code bench init-store ...},
 * {@code bench upload ...}, {@code bench export ...}, {@code bench ldbc-convert ...}, {@code bench gate ...} and
 * {@code bench gate-pairs ...}. Exit codes: 0 ok (even when cases failed), 1 an upload failed, 2 bad arguments, 3 the idle check failed.
 */
public final class BenchMain {
  /** Starts the server under test; a seam so tests run without Docker. */
  interface ServerFactory {
    ServerUnderTest start(String image, String cpuset, int memLimitGb, String heap);
  }

  private BenchMain() {
  }

  public static void main(final String[] args) {
    System.exit(run(args, WorkloadRegistry.all(),
        (image, cpuset, memLimitGb, heap) -> ServerUnderTest.start(image, cpuset, memLimitGb, heap, new HashMap<>()), System.out));
  }

  static int run(final String[] args, final List<Workload> registry, final ServerFactory serverFactory, final PrintStream out) {
    if (args.length == 0) {
      System.err.println("usage: bench <run|list|init-store|upload|export|ldbc-convert|gate|gate-pairs> [options]");
      return 2;
    }
    switch (args[0]) {
    case "list":
      return list(registry, out);
    case "run":
      try {
        return runCommand(parse(args, false, RUN_OPTIONS), registry, serverFactory, out);
      } catch (final IllegalArgumentException e) {
        System.err.println("error: " + e.getMessage());
        return 2;
      }
    case "init-store":
      try {
        return initStore(parse(args, false, INIT_STORE_OPTIONS));
      } catch (final IllegalArgumentException e) {
        System.err.println("error: " + e.getMessage());
        return 2;
      }
    case "export":
      try {
        return export(parse(args, false, EXPORT_OPTIONS));
      } catch (final IllegalArgumentException e) {
        System.err.println("error: " + e.getMessage());
        return 2;
      }
    case "ldbc-convert":
      try {
        return ldbcConvert(parse(args, false, LDBC_CONVERT_OPTIONS), out);
      } catch (final IllegalArgumentException e) {
        System.err.println("error: " + e.getMessage());
        return 2;
      }
    case "upload":
      try {
        return upload(parse(args, true, UPLOAD_OPTIONS), out, System.err);
      } catch (final IllegalArgumentException e) {
        System.err.println("error: " + e.getMessage());
        return 2;
      }
    case "gate":
      try {
        return gate(parse(args, false, GATE_OPTIONS), out, System.err);
      } catch (final IllegalArgumentException e) {
        System.err.println("error: " + e.getMessage());
        return 2;
      }
    case "gate-pairs":
      try {
        return gatePairs(parse(args, false, GATE_PAIRS_OPTIONS), out, System.err);
      } catch (final IllegalArgumentException e) {
        System.err.println("error: " + e.getMessage());
        return 2;
      }
    default:
      System.err.println(
          "unknown command '" + args[0] + "', expected run, list, init-store, upload, export, ldbc-convert, gate or gate-pairs");
      return 2;
    }
  }

  private static int list(final List<Workload> registry, final PrintStream out) {
    for (final Workload w : registry)
      out.println(w.name() + "  " + String.join(",", w.cases()));
    return 0;
  }

  /** Key under which {@link #parse} keeps the one positional argument of {@code upload}. */
  private static final String POSITIONAL = "";

  /** The options each subcommand accepts; any other {@code --flag} is rejected, so a typo cannot silently fall back to a default. */
  static final Set<String> RUN_OPTIONS = Set.of("workload", "scale", "image", "variant", "out", "reps", "warmup", "timeout-min",
      "prepare-timeout-min", "case-timeout-min", "seed", "mem-limit-gb", "heap", "trigger", "machine-id", "tag", "params",
      "skip-idle-check", "server-cpus", "client-cpus", "commit", "version", "bench-commit", "commit-date");
  static final Set<String> INIT_STORE_OPTIONS = Set.of("url", "root-user", "root-password-env", "writer-password-env",
      "reader-password-env");
  static final Set<String> EXPORT_OPTIONS = Set.of("url", "user", "password-env", "out");
  static final Set<String> LDBC_CONVERT_OPTIONS = Set.of("summary", "dataset", "commit", "out", "started-at", "version", "tag",
      "trigger", "machine-id", "bench-commit", "commit-date");
  static final Set<String> UPLOAD_OPTIONS = Set.of("url", "user", "password-env", "replace-release");
  static final Set<String> GATE_OPTIONS = Set.of("data", "issues", "bench-changes", "out");
  static final Set<String> GATE_PAIRS_OPTIONS = Set.of("data", "issues");

  /** Options that are flags: present or absent, they take no value. */
  private static final Set<String> FLAGS = Set.of("skip-idle-check", "replace-release");

  static Map<String, String> parse(final String[] args, final boolean allowPositional, final Set<String> known) {
    final Map<String, String> options = new HashMap<>();
    for (int i = 1; i < args.length; i++) {
      final String arg = args[i];
      if (!arg.startsWith("--")) {
        if (!allowPositional || options.containsKey(POSITIONAL))
          throw new IllegalArgumentException("unexpected argument '" + arg + "'");
        options.put(POSITIONAL, arg);
        continue;
      }
      final String key = arg.substring(2);
      if (!known.contains(key))
        throw new IllegalArgumentException("unknown option " + arg);
      if (FLAGS.contains(key))
        options.put(key, "true");
      else {
        if (i + 1 >= args.length)
          throw new IllegalArgumentException("missing value for " + arg);
        options.put(key, args[++i]);
      }
    }
    return options;
  }

  private static String required(final Map<String, String> o, final String key) {
    final String v = o.get(key);
    if (v == null)
      throw new IllegalArgumentException("missing required option --" + key);
    return v;
  }

  /** The password held by the environment variable the option names; never taken from the command line itself. */
  private static String passwordFromEnv(final Map<String, String> o, final String key) {
    final String variable = required(o, key);
    final String value = System.getenv(variable);
    if (value == null || value.isEmpty())
      throw new IllegalArgumentException("environment variable " + variable + " (--" + key + ") is not set");
    return value;
  }

  private static int initStore(final Map<String, String> o) {
    final String url = required(o, "url");
    final String root = passwordFromEnv(o, "root-password-env");
    final String writer = passwordFromEnv(o, "writer-password-env");
    final String reader = passwordFromEnv(o, "reader-password-env");
    ResultsStore.init(url, o.getOrDefault("root-user", "root"), root, writer, reader);
    return 0;
  }

  /** Converts an LDBC Graphalytics {@code mode1-summary.json} into one results file with source "ldbc". */
  private static int ldbcConvert(final Map<String, String> o, final PrintStream out) {
    final Path summaryPath = Path.of(required(o, "summary"));
    final String dataset = required(o, "dataset");
    final String commit = required(o, "commit");
    final String commitDate = commitDate(o);
    final Path dir = Path.of(required(o, "out"));
    final JSONObject summary;
    final Instant startedAt;
    try {
      summary = new JSONObject(Files.readString(summaryPath, StandardCharsets.UTF_8));
      startedAt = o.containsKey("started-at") ? Instant.parse(o.get("started-at"))
          : Files.getLastModifiedTime(summaryPath).toInstant();
    } catch (final IOException e) {
      throw new IllegalArgumentException("cannot read " + summaryPath + ": " + e.getMessage());
    } catch (final DateTimeParseException e) {
      throw new IllegalArgumentException("--started-at must be an ISO-8601 instant, got '" + o.get("started-at") + "'");
    } catch (final RuntimeException e) {
      throw new IllegalArgumentException(summaryPath + " is not valid JSON: " + e.getMessage());
    }
    final JSONObject arcadedb = new JSONObject().put("commit", commit).put("version", o.getOrDefault("version", "unknown"))
        .put("variant", "jvm").put("image", "embedded").put("imageDigest", "n/a").put("revision", commit);
    if (o.containsKey("tag"))
      arcadedb.put("tag", o.get("tag"));
    if (commitDate != null)
      arcadedb.put("commitDate", commitDate);
    final JSONObject config = new JSONObject().put("dataset", dataset).put("framework", "graphalytics").put("mode1", true)
        .put("benchCommit", o.getOrDefault("bench-commit", "unknown"));
    final String runId = RunInfo.newRunId(startedAt, commit.substring(0, Math.min(10, commit.length())), "jvm") + "-ldbc";
    final RunInfo info = new RunInfo(runId, startedAt, startedAt, o.getOrDefault("trigger", "manual"), "ldbc", arcadedb,
        MachineProbe.probe(o.getOrDefault("machine-id", "local")), config);
    new RunResult(info, LdbcAdapter.convert(summary, info, dataset).results()).write(dir);
    out.println(dir.resolve(runId + ".json"));
    return 0;
  }

  private static int export(final Map<String, String> o) {
    final String url = required(o, "url");
    final String user = required(o, "user");
    final String password = passwordFromEnv(o, "password-env");
    final Path out = Path.of(required(o, "out"));
    try {
      Exporter.export(url, user, password, out);
    } catch (final RuntimeException e) {
      System.err.println("export failed: " + e.getMessage());
      return 1;
    }
    return 0;
  }

  /**
   * The {@code upload} command. With {@code --replace-release} each run also replaces the earlier runs of its release (see
   * {@link Uploader}), and every replaced runId is printed as {@code replaced <runId>} so the caller can drop its raw file.
   */
  private static int upload(final Map<String, String> o, final PrintStream out, final PrintStream err) {
    final boolean replaceRelease = o.containsKey("replace-release");
    final String url = required(o, "url");
    final String user = required(o, "user");
    final String password = passwordFromEnv(o, "password-env");
    final String target = o.get(POSITIONAL);
    if (target == null)
      throw new IllegalArgumentException("missing the results file or directory to upload");
    final Path path = Path.of(target);
    final List<Path> files = new ArrayList<>();
    if (Files.isDirectory(path)) {
      try (final Stream<Path> list = Files.list(path)) {
        list.filter(p -> p.getFileName().toString().endsWith(".json")).sorted().forEach(files::add);
      } catch (final IOException e) {
        throw new IllegalArgumentException("cannot read directory " + path + ": " + e.getMessage());
      }
    } else if (Files.isRegularFile(path))
      files.add(path);
    else
      throw new IllegalArgumentException("no such file or directory: " + path);

    int failed = 0;
    for (final Path file : files) {
      try {
        for (final String replaced : Uploader.upload(url, user, password, RunResult.read(file), replaceRelease))
          out.println("replaced " + replaced);
      } catch (final RuntimeException e) {
        failed++;
        err.println("upload of " + file + " failed: " + e.getMessage());
      }
    }
    return failed == 0 ? 0 : 1;
  }

  /**
   * The {@code gate} command: judges the raw run files under {@code --data} against the regression issues the workflow listed in
   * {@code --issues}, and writes the plan of GitHub actions to {@code --out}. It makes no network call and runs no git: whether the
   * bench changed between two runs comes from {@code --bench-changes}, the workflow's answers to {@code gate-pairs}. A skipped run
   * file or issue is warned on {@code err} and the command still exits 0.
   */
  private static int gate(final Map<String, String> o, final PrintStream out, final PrintStream err) {
    final Path data = existingDirectory(required(o, "data"));
    final Path issuesFile = existingFile(required(o, "issues"));
    final Path outFile = Path.of(required(o, "out"));
    final BenchChanges bench = BenchChanges.read(o.containsKey("bench-changes") ? Path.of(o.get("bench-changes")) : null, err);
    final List<TrackedIssue> issues = TrackedIssue.readAll(issuesFile, err);
    final JSONObject plan = Planner.plan(RawData.read(data, err), issues, bench);
    for (final String warning : Planner.oversized(plan))
      err.println("warning: " + warning);
    try {
      Files.writeString(outFile, plan.toString(2), StandardCharsets.UTF_8);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    final Map<String, Integer> counts = new HashMap<>();
    final JSONArray actions = plan.getJSONArray("actions");
    for (int i = 0; i < actions.length(); i++)
      counts.merge(actions.getJSONObject(i).getString("op"), 1, Integer::sum);
    out.println("plan: " + counts.getOrDefault("create", 0) + " create, " + counts.getOrDefault("update", 0) + " update, "
        + counts.getOrDefault("comment", 0) + " comment, " + counts.getOrDefault("close", 0) + " close");
    return 0;
  }

  /**
   * The {@code gate-pairs} command: prints, one {@code A B} per line, each pair of bench commits whose difference a {@code gate} over
   * the same data and issues will show, so the workflow can answer them with git before it runs the gate. {@code --issues} is
   * optional: without it no issue is tracked.
   */
  private static int gatePairs(final Map<String, String> o, final PrintStream out, final PrintStream err) {
    final Path data = existingDirectory(required(o, "data"));
    final List<TrackedIssue> issues = o.containsKey("issues") ? TrackedIssue.readAll(existingFile(o.get("issues")), err) : List.of();
    for (final String pair : BenchChanges.pairsNeeded(RawData.read(data, err), issues)) {
      final int dots = pair.indexOf("..");
      out.println(pair.substring(0, dots) + " " + pair.substring(dots + 2));
    }
    return 0;
  }

  private static Path existingFile(final String value) {
    final Path file = Path.of(value);
    if (!Files.isRegularFile(file))
      throw new IllegalArgumentException("no such file: " + file);
    return file;
  }

  private static Path existingDirectory(final String value) {
    final Path dir = Path.of(value);
    if (!Files.isDirectory(dir))
      throw new IllegalArgumentException("no such directory: " + dir);
    return dir;
  }

  private static int intOption(final Map<String, String> o, final String key, final int def) {
    final String v = o.get(key);
    if (v == null)
      return def;
    try {
      return Integer.parseInt(v);
    } catch (final NumberFormatException e) {
      throw new IllegalArgumentException("--" + key + " must be an integer, got '" + v + "'");
    }
  }

  private static int runCommand(final Map<String, String> o, final List<Workload> registry, final ServerFactory serverFactory,
      final PrintStream out) {
    final String workloadName = required(o, "workload");
    final Scale scale;
    try {
      scale = Scale.valueOf(required(o, "scale"));
    } catch (final IllegalArgumentException e) {
      throw new IllegalArgumentException("--scale must be one of XS, S, M, L");
    }
    final String image = required(o, "image");
    final String variant = required(o, "variant");
    final String outDir = required(o, "out");
    final int reps = intOption(o, "reps", 5);
    final int warmup = intOption(o, "warmup", 2);
    // each warm-up and repetition; prepare (which may load the data set) has its own; the case cap is a hang detector, 0 = none
    final int timeoutMin = intOption(o, "timeout-min", 30);
    final int prepareTimeoutMin = intOption(o, "prepare-timeout-min", timeoutMin);
    final int caseTimeoutMin = intOption(o, "case-timeout-min", 120);
    // built before any work starts, so a bad limit is refused up front
    final BenchRunner runner = new BenchRunner(reps, warmup, Duration.ofMinutes(timeoutMin), Duration.ofMinutes(prepareTimeoutMin),
        Duration.ofMinutes(caseTimeoutMin));
    final long seed = intOption(o, "seed", 42);
    final int memLimitGb = intOption(o, "mem-limit-gb", 16);
    final String heap = o.getOrDefault("heap", "8g");
    final String trigger = o.getOrDefault("trigger", "manual");
    final String machineId = o.getOrDefault("machine-id", "local");
    final String tag = o.get("tag");
    final String commitDate = commitDate(o);
    final JSONObject params;
    try {
      params = new JSONObject(o.getOrDefault("params", "{}"));
    } catch (final RuntimeException e) {
      throw new IllegalArgumentException("--params is not valid JSON: " + e.getMessage());
    }

    final List<Workload> selected = new ArrayList<>();
    if (workloadName.equals("all"))
      selected.addAll(registry);
    else {
      for (final Workload w : registry)
        if (w.name().equals(workloadName))
          selected.add(w);
      if (selected.isEmpty()) {
        final List<String> names = new ArrayList<>();
        for (final Workload w : registry)
          names.add(w.name());
        throw new IllegalArgumentException("unknown workload '" + workloadName + "', valid: all, " + String.join(", ", names));
      }
    }

    if (!o.containsKey("skip-idle-check")) {
      final Optional<String> reason = IdleCheck.checkHost();
      if (reason.isPresent()) {
        System.err.println("host is not idle: " + reason.get());
        return 3;
      }
    }

    final String serverCpus = resolveCpus(o.getOrDefault("server-cpus", "auto"), true);
    final String clientCpus = resolveCpus(o.getOrDefault("client-cpus", "auto"), false);

    final Instant startedAt = Instant.now();
    final List<CaseResult> results = new ArrayList<>();
    JSONObject serverConfig = null;
    JSONObject imageInfo = null;
    for (final Workload w : selected) {
      if (runner.hasAbandonedWorker()) {
        // the runner records every case as not started; a server started now would only compete with the stuck worker
        results.addAll(runner.run(w, new WorkloadContext(null, scale, seed, params), null));
        continue;
      }
      final ServerUnderTest server;
      try {
        server = serverFactory.start(image, serverCpus, memLimitGb, heap);
      } catch (final Throwable t) {
        for (final String caseName : w.cases())
          results.add(new CaseResult(w.name(), caseName, scale.name(), params, reps, warmup, List.of(), CaseResult.Status.FAILED,
              "server start failed: " + t.getClass().getName() + ": " + t.getMessage()));
        continue;
      }
      try (server) {
        if (server != null && serverConfig == null) {
          serverConfig = orFallback(server::config, new JSONObject(), "server config");
          imageInfo = orFallback(server::imageInfo, null, "image info");
        }
        results.addAll(runner.run(w, new WorkloadContext(server, scale, seed, params), null));
      }
    }

    final String commit = o.getOrDefault("commit",
        imageInfo != null ? imageInfo.getString("revision", "unknown") : "unknown");
    final JSONObject arcadedb = new JSONObject().put("commit", commit).put("version", o.getOrDefault("version", "unknown"))
        .put("variant", variant).put("image", image)
        .put("imageDigest", imageInfo != null ? imageInfo.getString("imageDigest", "unknown") : "unknown")
        .put("revision", imageInfo != null ? imageInfo.getString("revision", "unknown") : "unknown");
    if (tag != null)
      arcadedb.put("tag", tag);
    if (commitDate != null)
      arcadedb.put("commitDate", commitDate);
    final JSONObject config = serverConfig != null ? serverConfig : new JSONObject();
    config.put("benchCommit", o.getOrDefault("bench-commit", "unknown"));
    config.put("reps", reps).put("warmup", warmup).put("timeoutMin", timeoutMin)
        .put("prepareTimeoutMin", prepareTimeoutMin).put("caseTimeoutMin", caseTimeoutMin).put("seed", seed)
        .put("serverCpus", serverCpus.isEmpty() ? "none" : serverCpus)
        .put("clientCpus", clientCpus.isEmpty() ? "none" : clientCpus);

    final String runId = RunInfo.newRunId(startedAt, commit.substring(0, Math.min(10, commit.length())), variant);
    final RunInfo info = new RunInfo(runId, startedAt, Instant.now(), trigger, "bench", arcadedb,
        MachineProbe.probe(machineId), config);
    final Path dir = Path.of(outDir);
    new RunResult(info, results).write(dir);
    out.println(dir.resolve(runId + ".json"));
    return 0;
  }

  /**
   * The {@code --commit-date} option: when the measured commit was made, as git prints it ({@code %cI}, with an offset) or as a
   * UTC instant. The dashboard places a point at this date rather than at the run's start, so a release measured today lands
   * where its code is. Returned as a UTC instant, {@code null} when the option is absent.
   */
  static String commitDate(final Map<String, String> o) {
    final String value = o.get("commit-date");
    if (value == null)
      return null;
    try {
      return OffsetDateTime.parse(value).toInstant().toString();
    } catch (final DateTimeParseException e) {
      throw new IllegalArgumentException("--commit-date must be an ISO-8601 date-time with an offset, got '" + value + "'");
    }
  }

  /** The value of an introspection call; a failure is logged and replaced by the fallback instead of aborting the run. */
  static JSONObject orFallback(final Supplier<JSONObject> call, final JSONObject fallback, final String what) {
    try {
      return call.get();
    } catch (final RuntimeException e) {
      System.err.println("warning: cannot read the " + what + ", recording unknown values: " + e);
      return fallback;
    }
  }

  /** "auto" is the P-cores (server) or E-cores (client) from lscpu; "none" or an unavailable lscpu means no pinning. */
  private static String resolveCpus(final String value, final boolean server) {
    if (value.equals("none"))
      return "";
    if (!value.equals("auto"))
      return value;
    final String text = Processes.output(Duration.ofSeconds(10), "lscpu", "--extended=CPU,CORE,MAXMHZ");
    if (text == null)
      return "";
    try {
      final CpuTopology topology = CpuTopology.parse(text);
      final String cpus = server ? topology.performanceCpus() : topology.efficiencyCpus();
      return cpus == null ? "" : cpus;
    } catch (final RuntimeException e) {
      return "";
    }
  }
}
