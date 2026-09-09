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

import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.TagFilter;
import com.arcadedb.engine.timeseries.TimeSeriesGateway;
import com.arcadedb.engine.timeseries.TimeSeriesGateway.TypeResolution;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Adapts the JSON request shapes of the TimeSeries HTTP handlers onto {@link TimeSeriesGateway}, which holds the
 * protocol-neutral semantics shared with the gRPC {@code TimeSeries*} RPCs (issue #7305). Nothing here decides
 * anything: it only turns {@code JSONObject}/{@code JSONArray} into the plain Java shapes the gateway takes, so
 * a change to how a tag filter or a projection is resolved lands on both protocols at once.
 */
final class TimeSeriesHandlerUtils {

  private TimeSeriesHandlerUtils() {
  }

  static TagFilter buildTagFilter(final JSONObject tagsJson, final List<ColumnDefinition> columns) {
    if (tagsJson == null || tagsJson.keySet().isEmpty())
      return null;

    // LinkedHashMap so the predicates are conjoined in the order the request stated them, as they were when
    // this method iterated the JSONObject directly.
    final Map<String, Object> tags = new LinkedHashMap<>();
    for (final String tagName : tagsJson.keySet())
      tags.put(tagName, tagsJson.get(tagName));

    return TimeSeriesGateway.buildTagFilter(tags, columns);
  }

  static int[] resolveColumnIndices(final JSONArray fieldsJson, final List<ColumnDefinition> columns) {
    if (fieldsJson == null || fieldsJson.length() == 0)
      return null;

    final List<String> fields = new ArrayList<>(fieldsJson.length());
    for (int f = 0; f < fieldsJson.length(); f++)
      fields.add(fieldsJson.getString(f));

    return TimeSeriesGateway.resolveColumnIndices(fields, columns);
  }

  static int findColumnIndex(final String fieldName, final List<ColumnDefinition> columns) {
    return TimeSeriesGateway.findColumnIndex(fieldName, columns);
  }

  /**
   * Renders a failed {@link TimeSeriesGateway#resolveForRead} as the 400 the TimeSeries read endpoints answer
   * with. The three cases stay distinct: a type that IS a TimeSeries type whose storage failed to load used to
   * share the "is not a TimeSeries type" message, which sent an operator chasing the wrong cause (issue #6356
   * follow-up, claude-review on PR #6779).
   * <p>
   * The engine-unavailable body is built with {@link JSONObject} rather than string concatenation because the
   * reason embeds a file path that could contain a double quote or a backslash, which raw concatenation would
   * turn into invalid JSON.
   */
  static ExecutionResponse resolutionError(final String typeName, final TypeResolution resolved) {
    return switch (resolved.failure()) {
      case NOT_FOUND -> new ExecutionResponse(400,
          "{ \"error\" : \"Type '" + typeName + "' does not exist\"}");
      case NOT_TIME_SERIES -> new ExecutionResponse(400,
          "{ \"error\" : \"Type '" + typeName + "' is not a TimeSeries type\"}");
      case ENGINE_UNAVAILABLE -> new ExecutionResponse(400, new JSONObject().put("error",
          "TimeSeries type '" + typeName + "' has no storage engine available: " + resolved.unavailableReason())
          .toString());
    };
  }
}
