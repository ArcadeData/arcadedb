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
package com.arcadedb.bench.ldbc;

import com.arcadedb.bench.model.CaseResult;
import com.arcadedb.bench.model.Metric;
import com.arcadedb.bench.model.RunInfo;
import com.arcadedb.bench.model.RunResult;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Converts the {@code mode1-summary.json} written by the LDBC Graphalytics ArcadeDB platform driver
 * ({@code scripts/run_mode1.py}) into the results model, so framework runs land in the same store as the bench runs.
 */
public final class LdbcAdapter {
  public static final String WORKLOAD = "graph-analytics";
  public static final String METRIC = "processing_s";

  private LdbcAdapter() {
  }

  /**
   * One case per (mode, algorithm). The framework does its own runs, so every case has reps 1 and warmup 0. A mode without
   * any algorithm (the framework run failed) becomes one FAILED case {@code <mode>-_run} carrying the mode's outcome.
   */
  public static RunResult convert(final JSONObject summary, final RunInfo info, final String dataset) {
    final List<CaseResult> results = new ArrayList<>();
    for (final String mode : summary.keySet()) {
      final JSONObject modeObject = summary.getJSONObject(mode);
      final JSONObject algorithms = modeObject.getJSONObject("algorithms", new JSONObject());
      if (algorithms.isEmpty()) {
        results.add(new CaseResult(WORKLOAD, mode + "-_run", dataset, new JSONObject(), 1, 0, List.of(), CaseResult.Status.FAILED,
            modeObject.getString("outcome", "no algorithm results")));
        continue;
      }
      for (final String algorithm : algorithms.keySet()) {
        final JSONObject a = algorithms.getJSONObject(algorithm);
        final double time = a.getDouble("processing_time", 0.0);
        final boolean success = a.getBoolean("success", false);
        final String caseName = mode + "-" + algorithm.toLowerCase(Locale.ROOT);
        final List<Metric> metrics = success ?
            List.of(new Metric(METRIC, "s", Metric.Better.LOWER, time, new double[] { time })) :
            List.of();
        results.add(new CaseResult(WORKLOAD, caseName, dataset, new JSONObject(), 1, 0, metrics,
            success ? CaseResult.Status.OK : CaseResult.Status.FAILED,
            success ? null : "algorithm " + algorithm + " failed in mode " + mode));
      }
    }
    return new RunResult(info, results);
  }
}
