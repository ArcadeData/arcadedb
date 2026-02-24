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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.AggregationType;
import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.MultiColumnAggregationRequest;
import com.arcadedb.engine.timeseries.MultiColumnAggregationResult;
import com.arcadedb.engine.timeseries.TagFilter;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Grafana DataFrame query endpoint.
 * Endpoint: POST /api/v1/ts/{database}/grafana/query
 *
 * Accepts multi-target queries and returns Grafana DataFrame wire format (columnar arrays with schema metadata).
 */
public class PostGrafanaQueryHandler extends AbstractServerHttpHandler {

  public PostGrafanaQueryHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) throws Exception {

    final Deque<String> databaseParam = exchange.getQueryParameters().get("database");
    if (databaseParam == null || databaseParam.isEmpty())
      return new ExecutionResponse(400, "{ \"error\" : \"Database parameter is required\"}");

    if (payload == null || !payload.has("targets"))
      return new ExecutionResponse(400, "{ \"error\" : \"'targets' array is required\"}");

    final DatabaseInternal database = httpServer.getServer().getDatabase(databaseParam.getFirst(), false, false);

    final long fromTs = payload.getLong("from", Long.MIN_VALUE);
    final long toTs = payload.getLong("to", Long.MAX_VALUE);
    final int maxDataPoints = payload.getInt("maxDataPoints", 0);

    final JSONArray targets = payload.getJSONArray("targets");
    final JSONObject results = new JSONObject();

    for (int t = 0; t < targets.length(); t++) {
      final JSONObject target = targets.getJSONObject(t);
      final String refId = target.getString("refId", "A");
      final String typeName = target.getString("type");

      if (!database.getSchema().existsType(typeName)) {
        results.put(refId, buildErrorFrame("Type '" + typeName + "' does not exist"));
        continue;
      }

      final DocumentType docType = database.getSchema().getType(typeName);
      if (!(docType instanceof LocalTimeSeriesType tsType) || tsType.getEngine() == null) {
        results.put(refId, buildErrorFrame("Type '" + typeName + "' is not a TimeSeries type"));
        continue;
      }

      final TimeSeriesEngine engine = tsType.getEngine();
      final List<ColumnDefinition> columns = tsType.getTsColumns();

      // Build tag filter
      final TagFilter tagFilter = target.has("tags")
          ? TimeSeriesHandlerUtils.buildTagFilter(target.getJSONObject("tags"), columns)
          : null;

      final JSONObject frameResult;
      if (target.has("aggregation"))
        frameResult = executeAggregation(target, engine, columns, fromTs, toTs, maxDataPoints, tagFilter);
      else
        frameResult = executeRawQuery(target, engine, columns, fromTs, toTs, tagFilter);

      results.put(refId, frameResult);
    }

    final JSONObject response = new JSONObject();
    response.put("results", results);
    return new ExecutionResponse(200, response.toString());
  }

  private JSONObject executeRawQuery(final JSONObject target, final TimeSeriesEngine engine,
      final List<ColumnDefinition> columns, final long fromTs, final long toTs,
      final TagFilter tagFilter) throws Exception {

    final int[] columnIndices = target.has("fields")
        ? TimeSeriesHandlerUtils.resolveColumnIndices(target.getJSONArray("fields"), columns)
        : null;

    final List<Object[]> rows = engine.query(fromTs, toTs, columnIndices, tagFilter);

    // Build schema fields and columnar data
    final List<ColumnDefinition> selectedColumns = new ArrayList<>();
    if (columnIndices == null) {
      selectedColumns.addAll(columns);
    } else {
      for (final int idx : columnIndices)
        selectedColumns.add(columns.get(idx));
    }

    final JSONArray schemaFields = new JSONArray();
    for (final ColumnDefinition col : selectedColumns) {
      final JSONObject field = new JSONObject();
      field.put("name", col.getName());
      field.put("type", grafanaFieldType(col));
      schemaFields.put(field);
    }

    // Transpose rows to columnar format
    final int numCols = selectedColumns.size();
    final JSONArray[] columnArrays = new JSONArray[numCols];
    for (int c = 0; c < numCols; c++)
      columnArrays[c] = new JSONArray();

    for (final Object[] row : rows) {
      for (int c = 0; c < numCols; c++)
        columnArrays[c].put(row[c]);
    }

    final JSONArray valuesArray = new JSONArray();
    for (final JSONArray col : columnArrays)
      valuesArray.put(col);

    return buildFrame(schemaFields, valuesArray);
  }

  private JSONObject executeAggregation(final JSONObject target, final TimeSeriesEngine engine,
      final List<ColumnDefinition> columns, final long fromTs, final long toTs,
      final int maxDataPoints, final TagFilter tagFilter) throws Exception {

    final JSONObject aggJson = target.getJSONObject("aggregation");
    final JSONArray requestsJson = aggJson.getJSONArray("requests");

    // Determine bucket interval: explicit or auto-calculated from maxDataPoints
    long bucketInterval = aggJson.getLong("bucketInterval", 0);
    if (bucketInterval <= 0 && maxDataPoints > 0 && fromTs != Long.MIN_VALUE && toTs != Long.MAX_VALUE)
      bucketInterval = Math.max(1, (toTs - fromTs) / maxDataPoints);
    if (bucketInterval <= 0)
      bucketInterval = 60000; // fallback: 1 minute

    final List<MultiColumnAggregationRequest> requests = new ArrayList<>();
    final List<String> aliases = new ArrayList<>();

    for (int i = 0; i < requestsJson.length(); i++) {
      final JSONObject req = requestsJson.getJSONObject(i);
      final String fieldName = req.getString("field");
      final AggregationType aggType = AggregationType.valueOf(req.getString("type"));
      final String alias = req.getString("alias", fieldName + "_" + aggType.name().toLowerCase());

      final int colIndex = TimeSeriesHandlerUtils.findColumnIndex(fieldName, columns);
      if (colIndex < 0)
        return buildErrorFrame("Field '" + fieldName + "' not found in type");

      requests.add(new MultiColumnAggregationRequest(colIndex, aggType, alias));
      aliases.add(alias);
    }

    final MultiColumnAggregationResult aggResult = engine.aggregateMulti(fromTs, toTs, requests, bucketInterval,
        tagFilter);

    final List<Long> timestamps = aggResult.getBucketTimestamps();

    // Schema: time + one field per aggregation
    final JSONArray schemaFields = new JSONArray();
    final JSONObject timeField = new JSONObject();
    timeField.put("name", "time");
    timeField.put("type", "time");
    schemaFields.put(timeField);

    for (final String alias : aliases) {
      final JSONObject field = new JSONObject();
      field.put("name", alias);
      field.put("type", "number");
      schemaFields.put(field);
    }

    // Columnar data: timestamps column + one column per aggregation
    final JSONArray timeValues = new JSONArray();
    for (final long ts : timestamps)
      timeValues.put(ts);

    final JSONArray[] aggColumns = new JSONArray[aliases.size()];
    for (int r = 0; r < aliases.size(); r++)
      aggColumns[r] = new JSONArray();

    for (final long ts : timestamps) {
      for (int r = 0; r < requests.size(); r++)
        aggColumns[r].put(aggResult.getValue(ts, r));
    }

    final JSONArray valuesArray = new JSONArray();
    valuesArray.put(timeValues);
    for (final JSONArray col : aggColumns)
      valuesArray.put(col);

    return buildFrame(schemaFields, valuesArray);
  }

  private static JSONObject buildFrame(final JSONArray schemaFields, final JSONArray values) {
    final JSONObject schema = new JSONObject();
    schema.put("fields", schemaFields);

    final JSONObject data = new JSONObject();
    data.put("values", values);

    final JSONObject frame = new JSONObject();
    frame.put("schema", schema);
    frame.put("data", data);

    final JSONArray frames = new JSONArray();
    frames.put(frame);

    final JSONObject result = new JSONObject();
    result.put("frames", frames);
    return result;
  }

  private static JSONObject buildErrorFrame(final String message) {
    final JSONObject result = new JSONObject();
    result.put("error", message);
    result.put("frames", new JSONArray());
    return result;
  }

  private static String grafanaFieldType(final ColumnDefinition col) {
    if (col.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
      return "time";

    final Type dt = col.getDataType();
    return switch (dt) {
      case DOUBLE, FLOAT, INTEGER, SHORT, LONG, BYTE, DECIMAL -> "number";
      case BOOLEAN -> "boolean";
      case STRING -> "string";
      case DATETIME, DATE -> "time";
      default -> "string";
    };
  }
}
