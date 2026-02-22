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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * HTTP handler for TimeSeries query endpoint.
 * Endpoint: POST /api/v1/ts/{database}/query
 */
public class PostTimeSeriesQueryHandler extends AbstractServerHttpHandler {

  private static final int DEFAULT_LIMIT = 20_000;

  public PostTimeSeriesQueryHandler(final HttpServer httpServer) {
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

    if (payload == null || !payload.has("type"))
      return new ExecutionResponse(400, "{ \"error\" : \"'type' parameter is required\"}");

    final String typeName = payload.getString("type");
    final DatabaseInternal database = httpServer.getServer().getDatabase(databaseParam.getFirst(), false, false);

    if (!database.getSchema().existsType(typeName))
      return new ExecutionResponse(400, "{ \"error\" : \"Type '" + typeName + "' does not exist\"}");

    final DocumentType docType = database.getSchema().getType(typeName);
    if (!(docType instanceof LocalTimeSeriesType tsType) || tsType.getEngine() == null)
      return new ExecutionResponse(400, "{ \"error\" : \"Type '" + typeName + "' is not a TimeSeries type\"}");

    final TimeSeriesEngine engine = tsType.getEngine();
    final List<ColumnDefinition> columns = tsType.getTsColumns();

    final long fromTs = payload.getLong("from", Long.MIN_VALUE);
    final long toTs = payload.getLong("to", Long.MAX_VALUE);

    // Build tag filter
    final TagFilter tagFilter = buildTagFilter(payload, columns);

    // Check if aggregation is requested
    if (payload.has("aggregation"))
      return executeAggregation(payload, engine, columns, typeName, fromTs, toTs, tagFilter);

    return executeRawQuery(payload, engine, columns, typeName, fromTs, toTs, tagFilter);
  }

  private ExecutionResponse executeRawQuery(final JSONObject payload, final TimeSeriesEngine engine,
      final List<ColumnDefinition> columns, final String typeName, final long fromTs, final long toTs,
      final TagFilter tagFilter) throws Exception {

    final int limit = payload.getInt("limit", DEFAULT_LIMIT);

    // Resolve field projection
    final int[] columnIndices = resolveColumnIndices(payload, columns);

    final List<Object[]> rows = engine.query(fromTs, toTs, columnIndices, tagFilter);

    // Build column names for response
    final JSONArray colNames = new JSONArray();
    if (columnIndices == null) {
      for (final ColumnDefinition col : columns)
        colNames.put(col.getName());
    } else {
      for (final int idx : columnIndices)
        colNames.put(columns.get(idx).getName());
    }

    // Build rows array, applying limit
    final JSONArray rowsArray = new JSONArray();
    final int count = Math.min(rows.size(), limit);
    for (int i = 0; i < count; i++) {
      final Object[] row = rows.get(i);
      final JSONArray rowArray = new JSONArray();
      for (final Object val : row)
        rowArray.put(val);
      rowsArray.put(rowArray);
    }

    final JSONObject result = new JSONObject();
    result.put("type", typeName);
    result.put("columns", colNames);
    result.put("rows", rowsArray);
    result.put("count", count);

    return new ExecutionResponse(200, result.toString());
  }

  private ExecutionResponse executeAggregation(final JSONObject payload, final TimeSeriesEngine engine,
      final List<ColumnDefinition> columns, final String typeName, final long fromTs, final long toTs,
      final TagFilter tagFilter) throws Exception {

    final JSONObject aggJson = payload.getJSONObject("aggregation");
    final long bucketInterval = aggJson.getLong("bucketInterval");
    final JSONArray requestsJson = aggJson.getJSONArray("requests");

    final List<MultiColumnAggregationRequest> requests = new ArrayList<>();
    final JSONArray aggNames = new JSONArray();

    for (int i = 0; i < requestsJson.length(); i++) {
      final JSONObject req = requestsJson.getJSONObject(i);
      final String fieldName = req.getString("field");
      final AggregationType aggType = AggregationType.valueOf(req.getString("type"));
      final String alias = req.getString("alias", fieldName + "_" + aggType.name().toLowerCase());

      // Find column index by name
      int colIndex = -1;
      for (int c = 0; c < columns.size(); c++) {
        if (columns.get(c).getName().equals(fieldName)) {
          colIndex = c;
          break;
        }
      }

      if (colIndex < 0)
        return new ExecutionResponse(400, "{ \"error\" : \"Field '" + fieldName + "' not found in type\"}");

      requests.add(new MultiColumnAggregationRequest(colIndex, aggType, alias));
      aggNames.put(alias);
    }

    final MultiColumnAggregationResult aggResult = engine.aggregateMulti(fromTs, toTs, requests, bucketInterval,
        tagFilter);

    final List<Long> timestamps = aggResult.getBucketTimestamps();
    final JSONArray buckets = new JSONArray();

    for (final long ts : timestamps) {
      final JSONObject bucket = new JSONObject();
      bucket.put("timestamp", ts);
      final JSONArray values = new JSONArray();
      for (int r = 0; r < requests.size(); r++)
        values.put(aggResult.getValue(ts, r));
      bucket.put("values", values);
      buckets.put(bucket);
    }

    final JSONObject result = new JSONObject();
    result.put("type", typeName);
    result.put("aggregations", aggNames);
    result.put("buckets", buckets);
    result.put("count", timestamps.size());

    return new ExecutionResponse(200, result.toString());
  }

  private TagFilter buildTagFilter(final JSONObject payload, final List<ColumnDefinition> columns) {
    if (!payload.has("tags"))
      return null;
    return TimeSeriesHandlerUtils.buildTagFilter(payload.getJSONObject("tags"), columns);
  }

  private int[] resolveColumnIndices(final JSONObject payload, final List<ColumnDefinition> columns) {
    if (!payload.has("fields"))
      return null;
    return TimeSeriesHandlerUtils.resolveColumnIndices(payload.getJSONArray("fields"), columns);
  }
}
