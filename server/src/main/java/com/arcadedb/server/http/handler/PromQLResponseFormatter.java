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
package com.arcadedb.server.http.handler;

import com.arcadedb.engine.timeseries.promql.PromQLResult;
import com.arcadedb.engine.timeseries.promql.PromQLResult.InstantVector;
import com.arcadedb.engine.timeseries.promql.PromQLResult.MatrixResult;
import com.arcadedb.engine.timeseries.promql.PromQLResult.MatrixSeries;
import com.arcadedb.engine.timeseries.promql.PromQLResult.RangeSeries;
import com.arcadedb.engine.timeseries.promql.PromQLResult.RangeVector;
import com.arcadedb.engine.timeseries.promql.PromQLResult.ScalarResult;
import com.arcadedb.engine.timeseries.promql.PromQLResult.VectorSample;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.List;
import java.util.Map;

/**
 * Shared JSON formatting utility for Prometheus API responses.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class PromQLResponseFormatter {

  private PromQLResponseFormatter() {
  }

  static String formatSuccess(final PromQLResult result) {
    final JSONObject response = new JSONObject();
    response.put("status", "success");

    final JSONObject data = new JSONObject();
    // A switch over the sealed result, so a kind without a branch is a compile error: a range vector - what an instant query of
    // `metric[5m]` answers - had none, and was answered as a success with an empty 'data' (issue #11982)
    switch (result) {
    case InstantVector iv -> {
      data.put("resultType", "vector");
      data.put("result", formatVector(iv));
    }
    case MatrixResult mr -> {
      data.put("resultType", "matrix");
      final JSONArray series = new JSONArray();
      for (final MatrixSeries s : mr.series())
        series.put(formatSeries(s.labels(), s.values()));
      data.put("result", series);
    }
    case RangeVector rv -> {
      // the raw samples of each series in the window, as Prometheus answers an instant query of a range selector
      data.put("resultType", "matrix");
      final JSONArray series = new JSONArray();
      for (final RangeSeries s : rv.series())
        series.put(formatSeries(s.labels(), s.values()));
      data.put("result", series);
    }
    case ScalarResult sr -> {
      data.put("resultType", "scalar");
      final JSONArray val = new JSONArray();
      val.put(sr.timestampMs() / 1000.0);
      val.put(doubleToString(sr.value()));
      data.put("result", val);
    }
    }

    response.put("data", data);
    return response.toString();
  }

  private static JSONArray formatVector(final InstantVector iv) {
    final JSONArray result = new JSONArray();
    for (final VectorSample sample : iv.samples()) {
      final JSONObject entry = new JSONObject();
      entry.put("metric", labelsToJson(sample.labels()));
      final JSONArray value = new JSONArray();
      value.put(sample.timestampMs() / 1000.0);
      value.put(doubleToString(sample.value()));
      entry.put("value", value);
      result.put(entry);
    }
    return result;
  }

  /** One series of a matrix: its labels and its {@code [seconds, "value"]} pairs. */
  private static JSONObject formatSeries(final Map<String, String> labels, final List<double[]> points) {
    final JSONObject entry = new JSONObject();
    entry.put("metric", labelsToJson(labels));
    final JSONArray values = new JSONArray();
    for (final double[] point : points) {
      final JSONArray pair = new JSONArray();
      pair.put(point[0] / 1000.0);
      pair.put(doubleToString(point[1]));
      values.put(pair);
    }
    entry.put("values", values);
    return entry;
  }

  static String formatLabelsResponse(final List<String> labels) {
    final JSONObject response = new JSONObject();
    response.put("status", "success");
    final JSONArray data = new JSONArray();
    for (final String label : labels)
      data.put(label);
    response.put("data", data);
    return response.toString();
  }

  static String formatSeriesResponse(final List<Map<String, String>> seriesList) {
    final JSONObject response = new JSONObject();
    response.put("status", "success");
    final JSONArray data = new JSONArray();
    for (final Map<String, String> labels : seriesList)
      data.put(labelsToJson(labels));
    response.put("data", data);
    return response.toString();
  }

  /**
   * Whether a value NAMES a label, in the sense the Prometheus data model gives that word (issue #7712).
   * <p>
   * There, a label whose value is empty is defined as ABSENT: {@code host=""} selects the series that do not
   * carry {@code host} at all, {@code /label/{name}/values} does not offer the empty string among a label's
   * values, and a series is identified by its NON-empty labels, so two series differing only in an empty label
   * are one series. Every ArcadeDB read path spells a null tag {@code ""} - {@code TimeSeriesBucket} returns it
   * for a zero-length STRING, {@code TimeSeriesTagDictionary} maps both null and {@code ""} onto the same id, and
   * {@code TimeSeriesSealedStore.compressColumn} writes a null tag as {@code ""} - so without this rule a metric
   * whose samples include a null tag offered a blank entry in a Grafana label picker that selects nothing, and
   * reported one series more than Prometheus would for the same data.
   * <p>
   * This is a PRESENTATION rule of the Prometheus surface and nothing else: the engine keeps reporting exactly
   * what the samples hold, and {@code POST /api/v1/ts/{database}/query} still answers {@code ""} for the same
   * column, because a tag that genuinely holds the empty string is a value there.
   */
  static boolean isLabelValuePresent(final String value) {
    return value != null && !value.isEmpty();
  }

  static String formatError(final String errorType, final String message) {
    final JSONObject response = new JSONObject();
    response.put("status", "error");
    response.put("errorType", errorType);
    response.put("error", message);
    return response.toString();
  }

  private static JSONObject labelsToJson(final Map<String, String> labels) {
    final JSONObject json = new JSONObject();
    for (final Map.Entry<String, String> entry : labels.entrySet())
      json.put(entry.getKey(), entry.getValue());
    return json;
  }

  private static String doubleToString(final double value) {
    if (Double.isNaN(value))
      return "NaN";
    if (Double.isInfinite(value))
      return value > 0 ? "+Inf" : "-Inf";
    if (value == Math.floor(value) && !Double.isInfinite(value))
      return Long.toString((long) value);
    return Double.toString(value);
  }
}
