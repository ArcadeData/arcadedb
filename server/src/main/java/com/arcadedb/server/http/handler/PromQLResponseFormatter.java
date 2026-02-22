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
    if (result instanceof InstantVector iv) {
      data.put("resultType", "vector");
      data.put("result", formatVector(iv));
    } else if (result instanceof MatrixResult mr) {
      data.put("resultType", "matrix");
      data.put("result", formatMatrix(mr));
    } else if (result instanceof ScalarResult sr) {
      data.put("resultType", "scalar");
      final JSONArray val = new JSONArray();
      val.put(sr.timestampMs() / 1000.0);
      val.put(doubleToString(sr.value()));
      data.put("result", val);
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

  private static JSONArray formatMatrix(final MatrixResult mr) {
    final JSONArray result = new JSONArray();
    for (final MatrixSeries series : mr.series()) {
      final JSONObject entry = new JSONObject();
      entry.put("metric", labelsToJson(series.labels()));
      final JSONArray values = new JSONArray();
      for (final double[] point : series.values()) {
        final JSONArray pair = new JSONArray();
        pair.put(point[0] / 1000.0);
        pair.put(doubleToString(point[1]));
        values.put(pair);
      }
      entry.put("values", values);
      result.put(entry);
    }
    return result;
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
