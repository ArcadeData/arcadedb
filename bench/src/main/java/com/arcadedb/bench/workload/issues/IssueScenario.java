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
package com.arcadedb.bench.workload.issues;

import com.arcadedb.bench.runner.CaseFailedWithMetrics;
import com.arcadedb.bench.runner.Measurement;
import com.arcadedb.bench.workload.Scale;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;

import java.util.Locale;
import java.util.Map;

/**
 * One fixed performance issue kept fixed: a dataset, the forms whose timings judge it and the limit they must stay under.
 * The workload calls {@link #bind} first, then {@link #load} only when the dataset is missing, then {@link #checkForms} once
 * and {@link #measure} once per repetition.
 */
public interface IssueScenario {
  /** Two warm-up samples are discarded before the {@value #SAMPLES} timed ones of each repetition. */
  int WARMUP_SAMPLES = 2;
  int SAMPLES        = 5;

  /** A statement of a form: the language and the text; its parameters come from the scenario. */
  record Statement(String language, String text) {
    public static Statement sql(final String text) {
      return new Statement("sql", text);
    }

    public static Statement cypher(final String text) {
      return new Statement("opencypher", text);
    }

    /** True when the statement orders its answer: its rows are then compared in order, otherwise as a multiset. */
    public boolean ordered() {
      return text.toUpperCase(Locale.ROOT).contains("ORDER BY");
    }
  }

  @FunctionalInterface
  interface Loader {
    void load(RemoteDatabase db, Scale scale, long seed);
  }

  int issue();

  default String caseName() {
    return "issue-" + issue();
  }

  /** The table's limit: a ratio factor, or an exponent. */
  double limit();

  /** Fixes the scale and the seed the parameters and the expected answers derive from; untimed. */
  void bind(Scale scale, long seed);

  /** Creates the schema and loads the dataset into an empty database. */
  void load(RemoteDatabase db, Scale scale, long seed);

  /** Fails when the forms do not answer what they must: a case whose forms differ measures nothing. */
  void checkForms(RemoteDatabase db);

  /** One repetition: warm-up samples, then the timed ones; a failed verdict throws with the metrics kept. */
  Measurement measure(RemoteDatabase db, IssueSampler sampler, Scale scale) throws CaseFailedWithMetrics;

  /**
   * The least time one timed sample takes: a sample runs rounds of both forms (or of every size) until it has lasted this
   * long, so the {@value #SAMPLES} samples of a repetition last at least 5 s at S and up, as the S sizing rule asks, whatever
   * the batch. XS only proves the case runs.
   */
  static double sampleTargetMs(final Scale scale) {
    return scale == Scale.XS ? 50.0 : 1_000.0;
  }

  /** The least time of one warm-up sample: the runner's own warm-up repetitions do the long warming. */
  static double warmupSampleMs(final Scale scale) {
    return scale == Scale.XS ? 20.0 : 250.0;
  }

  /** The batch of a round is sized to about a tenth of a sample, so a sample overshoots its target by at most that. */
  static double roundTargetMs(final Scale scale) {
    return sampleTargetMs(scale) / 10;
  }

  /** How long the per-execution cost is measured before the batch is sized. */
  static double calibrationMs(final Scale scale) {
    return scale == Scale.XS ? 20.0 : 200.0;
  }

  /** Runs the statement and reads its answer to the end. */
  static int drain(final RemoteDatabase db, final Statement statement, final Map<String, Object> params) {
    int rows = 0;
    try (final ResultSet rs = db.query(statement.language(), statement.text(), params)) {
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
    }
    return rows;
  }
}
