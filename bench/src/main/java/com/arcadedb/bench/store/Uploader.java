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
import com.arcadedb.bench.model.RunResult;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Writes one {@link RunResult} into the results server. The whole upload is one transaction: the rows of the same runId are
 * deleted and the new ones inserted, so a repeated upload replaces the run and a failed one leaves the previous copy.
 * <p>
 * A release measured again gets a new runId. With {@code replaceRelease} the upload also removes, in the same transaction, the
 * earlier release runs it stands for: same tag, machine, variant and source, with a measurement at one of its scales. A release
 * then keeps one point per line instead of two at the same commit.
 */
public final class Uploader {
  public static final String STATUS_METRIC = "_status";

  private Uploader() {
  }

  public static void upload(final String url, final String user, final String password, final RunResult run) {
    upload(url, user, password, run, false);
  }

  /**
   * Uploads {@code run}; with {@code replaceRelease} it also deletes the earlier runs of the same release (see the class comment).
   *
   * @return the runIds it replaced, empty without {@code replaceRelease}
   * @throws IllegalArgumentException with {@code replaceRelease} for a run that is not a tagged release run
   */
  public static List<String> upload(final String url, final String user, final String password, final RunResult run,
      final boolean replaceRelease) {
    final String runId = run.info().runId();
    final String startedAt = run.info().startedAt().toString();
    final JSONObject arcadedb = run.info().arcadedb();
    final String commit = arcadedb.getString("commit", "unknown");
    final String variant = arcadedb.getString("variant", "unknown");
    final String machineId = run.info().machine().getString("id", "unknown");
    final String tag = arcadedb.getString("tag", "");
    if (replaceRelease && !"release".equals(run.info().trigger()))
      throw new IllegalArgumentException("only a release run can replace an earlier one, this run's trigger is '" + run.info().trigger() + "'");
    if (replaceRelease && tag.isEmpty())
      throw new IllegalArgumentException("a release run needs a tag to replace an earlier one of the same release");

    final List<String> replaced = new ArrayList<>();
    try (final RemoteDatabase db = ResultsStore.open(url, user, password)) {
      db.begin();
      try {
        db.command("sql", "DELETE FROM Measurement WHERE runId = :runId", Map.of("runId", runId)).close();
        db.command("sql", "DELETE FROM Run WHERE runId = :runId", Map.of("runId", runId)).close();
        if (replaceRelease)
          for (final String old : earlierRunsOfRelease(db, run, tag, variant, machineId)) {
            db.command("sql", "DELETE FROM Measurement WHERE runId = :runId", Map.of("runId", old)).close();
            db.command("sql", "DELETE FROM Run WHERE runId = :runId", Map.of("runId", old)).close();
            replaced.add(old);
          }

        final Map<String, Object> runParams = new HashMap<>();
        runParams.put("runId", runId);
        runParams.put("schemaVersion", RunResult.SCHEMA_VERSION);
        runParams.put("startedAt", startedAt);
        runParams.put("finishedAt", run.info().finishedAt().toString());
        runParams.put("trigger", run.info().trigger());
        runParams.put("source", run.info().source());
        runParams.put("arcadedb", arcadedb.toMap());
        runParams.put("machine", run.info().machine().toMap());
        runParams.put("config", run.info().config().toMap());
        db.command("sql", "INSERT INTO Run CONTENT :doc", Map.of("doc", runParams)).close();

        for (final CaseResult c : run.results()) {
          if (c.metrics().isEmpty())
            insert(db, runId, c, STATUS_METRIC, null, null, null, null, commit, variant, machineId, startedAt);
          else
            for (final Metric m : c.metrics())
              insert(db, runId, c, m.name(), m.unit(), m.better().toJSON(), m.median(), m.values(), commit, variant, machineId,
                  startedAt);
        }
        db.commit();
        return replaced;
      } catch (final RuntimeException e) {
        try {
          db.rollback();
        } catch (final RuntimeException suppressed) {
          e.addSuppressed(suppressed);
        }
        throw e;
      }
    }
  }

  /** The other runs of the same release, machine, variant and source that measured at least one scale of {@code run}. */
  private static List<String> earlierRunsOfRelease(final RemoteDatabase db, final RunResult run, final String tag,
      final String variant, final String machineId) {
    final Set<String> scales = new LinkedHashSet<>();
    for (final CaseResult c : run.results())
      scales.add(c.scale());
    final Map<String, Object> p = new HashMap<>();
    p.put("runId", run.info().runId());
    p.put("tag", tag);
    p.put("variant", variant);
    p.put("source", run.info().source());
    p.put("machineId", machineId);
    final List<String> candidates = new ArrayList<>();
    try (final ResultSet rs = db.query("sql", "SELECT runId FROM Run WHERE `trigger` = 'release' AND arcadedb.tag = :tag"
        + " AND arcadedb.variant = :variant AND source = :source AND machine.id = :machineId AND runId <> :runId", p)) {
      while (rs.hasNext())
        candidates.add(rs.next().getProperty("runId"));
    }
    final List<String> sameScale = new ArrayList<>();
    for (final String candidate : candidates)
      try (final ResultSet rs = db.query("sql", "SELECT count(*) AS n FROM Measurement WHERE runId = :runId AND scale IN :scales",
          Map.of("runId", candidate, "scales", new ArrayList<>(scales)))) {
        if (((Number) rs.next().getProperty("n")).longValue() > 0)
          sameScale.add(candidate);
      }
    return sameScale;
  }

  private static void insert(final RemoteDatabase db, final String runId, final CaseResult c, final String metric, final String unit,
      final String better, final Double median, final double[] values, final String commit, final String variant,
      final String machineId, final String startedAt) {
    final List<Double> list;
    if (values == null)
      list = null;
    else {
      list = new ArrayList<>(values.length);
      for (final double v : values)
        list.add(v);
    }
    final Map<String, Object> p = new HashMap<>();
    p.put("runId", runId);
    p.put("workload", c.workload());
    p.put("case", c.caseName());
    p.put("scale", c.scale());
    p.put("metric", metric);
    p.put("unit", unit);
    p.put("better", better);
    p.put("median", median);
    p.put("values", list);
    p.put("status", c.status().name().toLowerCase(Locale.ROOT));
    p.put("error", c.error());
    p.put("commit", commit);
    p.put("variant", variant);
    p.put("machineId", machineId);
    p.put("startedAt", startedAt);
    db.command("sql", "INSERT INTO Measurement CONTENT :doc", Map.of("doc", p)).close();
  }
}
