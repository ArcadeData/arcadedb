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
package com.arcadedb.bench.model;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * The outcome of one case. A failed or timed-out case is kept with its status and error, never dropped.
 */
public record CaseResult(String workload, String caseName, String scale, JSONObject params, int reps, int warmup,
                         List<Metric> metrics, Status status, String error, JSONObject notes, JSONArray histograms) {
  public enum Status {
    OK, FAILED, TIMEOUT
  }

  public CaseResult(final String workload, final String caseName, final String scale, final JSONObject params, final int reps,
      final int warmup, final List<Metric> metrics, final Status status, final String error) {
    this(workload, caseName, scale, params, reps, warmup, metrics, status, error, new JSONObject(), new JSONArray());
  }

  public CaseResult {
    if (notes == null)
      notes = new JSONObject();
    if (histograms == null)
      histograms = new JSONArray();
  }

  /**
   * Returns a copy with the note set, leaving this instance untouched.
   */
  public CaseResult withNote(final String key, final Object value) {
    final JSONObject copy = new JSONObject(notes.toString());
    copy.put(key, value);
    return new CaseResult(workload, caseName, scale, params, reps, warmup, metrics, status, error, copy, histograms);
  }

  public CaseResult withStatus(final Status newStatus, final String newError) {
    return new CaseResult(workload, caseName, scale, params, reps, warmup, metrics, newStatus, newError, notes, histograms);
  }

  public JSONObject toJSON() {
    final JSONArray array = new JSONArray();
    for (final Metric metric : metrics)
      array.put(metric.toJSON());
    final JSONObject json = new JSONObject().put("workload", workload).put("case", caseName).put("scale", scale).put("params", params)
        .put("reps", reps).put("warmup", warmup).put("metrics", array)
        .put("status", status.name().toLowerCase(Locale.ROOT)).put("error", error == null ? JSONObject.NULL : error);
    if (!notes.isEmpty())
      json.put("notes", notes);
    if (histograms.length() > 0)
      json.put("histograms", histograms);
    return json;
  }

  public static CaseResult fromJSON(final JSONObject json) {
    final JSONArray array = json.getJSONArray("metrics", new JSONArray());
    final List<Metric> metrics = new ArrayList<>(array.length());
    for (int i = 0; i < array.length(); i++)
      metrics.add(Metric.fromJSON(array.getJSONObject(i)));
    return new CaseResult(json.getString("workload"), json.getString("case"), json.getString("scale"),
        json.getJSONObject("params", new JSONObject()), json.getInt("reps", 0), json.getInt("warmup", 0), metrics,
        Status.valueOf(json.getString("status", "ok").toUpperCase(Locale.ROOT)),
        json.isNull("error") ? null : json.getString("error"), json.getJSONObject("notes", new JSONObject()),
        json.getJSONArray("histograms", new JSONArray()));
  }
}
