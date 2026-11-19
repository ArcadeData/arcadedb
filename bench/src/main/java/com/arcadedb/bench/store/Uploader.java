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
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Writes one {@link RunResult} into the results server. The whole upload is one transaction: the rows of the same runId are
 * deleted and the new ones inserted, so a repeated upload replaces the run and a failed one leaves the previous copy.
 */
public final class Uploader {
  public static final String STATUS_METRIC = "_status";

  private Uploader() {
  }

  public static void upload(final String url, final String user, final String password, final RunResult run) {
    final String runId = run.info().runId();
    final String startedAt = run.info().startedAt().toString();
    final JSONObject arcadedb = run.info().arcadedb();
    final String commit = arcadedb.getString("commit", "unknown");
    final String variant = arcadedb.getString("variant", "unknown");
    final String machineId = run.info().machine().getString("id", "unknown");

    try (final RemoteDatabase db = ResultsStore.open(url, user, password)) {
      db.begin();
      try {
        db.command("sql", "DELETE FROM Measurement WHERE runId = :runId", Map.of("runId", runId)).close();
        db.command("sql", "DELETE FROM Run WHERE runId = :runId", Map.of("runId", runId)).close();

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
