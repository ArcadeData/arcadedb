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
package com.arcadedb.bench.workload;

import com.arcadedb.bench.runner.Measurement;
import com.arcadedb.bench.runner.Workload;
import com.arcadedb.bench.runner.WorkloadContext;
import com.arcadedb.bench.workload.issues.IssueSampler;
import com.arcadedb.bench.workload.issues.IssueScenario;
import com.arcadedb.bench.workload.issues.IssueScenarios;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Fixed performance issues kept fixed: one case per issue ({@code issue-<number>}), each judged by the ratio of two forms of the
 * same question on the same server, or by the exponent of a sweep, against the issue's limit. A ratio or an exponent does not
 * move with the host, so a regression of the fix fails its case on a noisy host too. See {@link IssueScenarios} for the cases.
 * <p>
 * Each case has its own database, {@code bench_issue_<number>}, loaded in {@code prepare} only when it does not already hold the
 * dataset of this scale and seed. The forms are checked once in {@code prepare}; a repetition times two warm-up samples and then
 * five, each a batch of executions of each form, alternating.
 */
public class IssuesWorkload implements Workload {
  private static final Logger LOGGER = LoggerFactory.getLogger(IssuesWorkload.class);

  static final String MARKER_TYPE = "BenchDataset";

  private final Map<String, IssueScenario> scenarios = new LinkedHashMap<>();
  private final IssueSampler                sampler   = new IssueSampler(System::nanoTime);

  public IssuesWorkload() {
    for (final IssueScenario s : scenarios())
      scenarios.put(s.caseName(), s);
  }

  /** The scenarios in the plan's order. */
  public static List<IssueScenario> scenarios() {
    return IssueScenarios.all();
  }

  static String database(final IssueScenario scenario) {
    return "bench_issue_" + scenario.issue();
  }

  @Override
  public String name() {
    return "issues";
  }

  @Override
  public List<String> cases() {
    return new ArrayList<>(scenarios.keySet());
  }

  @Override
  public void prepare(final WorkloadContext ctx, final String caseName) {
    final IssueScenario scenario = scenario(caseName);
    // a seed of its own per case, so a filtered run measures what a full run does
    final long seed = ctx.seed() + scenario.issue();
    final String database = database(scenario);
    final String marker = ctx.scale().name() + "/" + seed;
    scenario.bind(ctx.scale(), seed);
    if (!datasetReady(ctx, database, marker)) {
      LOGGER.info("{}: loading the dataset of scale {} into {}", caseName, ctx.scale(), database);
      final long start = System.nanoTime();
      RemoteClients.recreateDatabase(ctx.server(), database);
      try (final RemoteDatabase db = RemoteClients.open(ctx.server(), database)) {
        scenario.load(db, ctx.scale(), seed);
        // written last: a load cut short leaves no marker, and the next run loads again
        db.command("sql", "CREATE DOCUMENT TYPE " + MARKER_TYPE).close();
        db.command("sql", "INSERT INTO " + MARKER_TYPE + " SET dataset = :m", Map.<String, Object>of("m", marker)).close();
      }
      LOGGER.info("{}: dataset loaded in {} s", caseName, (System.nanoTime() - start) / 1_000_000_000L);
    }
    try (final RemoteDatabase db = RemoteClients.open(ctx.server(), database)) {
      scenario.checkForms(db);
    }
  }

  @Override
  public Measurement measure(final WorkloadContext ctx, final String caseName) throws Exception {
    final IssueScenario scenario = scenario(caseName);
    try (final RemoteDatabase db = RemoteClients.open(ctx.server(), database(scenario))) {
      return scenario.measure(db, sampler, ctx.scale());
    }
  }

  private IssueScenario scenario(final String caseName) {
    final IssueScenario scenario = scenarios.get(caseName);
    if (scenario == null)
      throw new IllegalArgumentException("Unknown case: " + caseName);
    return scenario;
  }

  /** True when the database exists and its marker names this scale and seed. */
  private static boolean datasetReady(final WorkloadContext ctx, final String database, final String marker) {
    if (!RemoteClients.databaseExists(ctx.server(), database))
      return false;
    try (final RemoteDatabase db = RemoteClients.open(ctx.server(), database)) {
      if (!db.getSchema().existsType(MARKER_TYPE))
        return false;
      try (final ResultSet rs = db.query("sql", "SELECT dataset FROM " + MARKER_TYPE)) {
        return rs.hasNext() && marker.equals(rs.next().getProperty("dataset"));
      }
    }
  }
}
