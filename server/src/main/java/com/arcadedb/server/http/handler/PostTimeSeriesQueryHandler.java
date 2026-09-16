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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.AggregationType;
import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.MultiColumnAggregationRequest;
import com.arcadedb.engine.timeseries.MultiColumnAggregationResult;
import com.arcadedb.engine.timeseries.TagFilter;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.engine.timeseries.TimeSeriesGateway;
import com.arcadedb.engine.timeseries.TimeSeriesGateway.TypeResolution;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.logging.Level;

/**
 * HTTP handler for TimeSeries query endpoint.
 * Endpoint: POST /api/v1/ts/{database}/query
 */
public class PostTimeSeriesQueryHandler extends AbstractServerHttpHandler {

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

    // Enforce database-level authorization (GHSA-x8mg-6r4p-87pf): this handler does not extend DatabaseAbstractHandler.
    // Checked before any payload/parameter validation so an unauthorized caller cannot probe the target database.
    checkAuthorizationOnDatabase(user, databaseParam.getFirst());

    // Every member below is resolved through TimeSeriesHandlerUtils so an absent, null or wrongly-typed one is
    // refused by NAME in the 'error' field, instead of reaching JSONObject's raising getters and being answered as
    // a bare "Invalid JSON payload" whose specifics production mode conceals in 'detail' (issue #7340).
    //
    // A missing body goes through the SAME refusal rather than a second, differently worded one: 'type' is absent
    // either way, and the caller that reads the message cannot tell - nor care - which branch produced it.
    if (payload == null)
      return TimeSeriesHandlerUtils.badRequest(TimeSeriesHandlerUtils.missingMember("type", "a string"));

    final String typeName;
    try {
      typeName = TimeSeriesHandlerUtils.requireString(payload, "type", "type");
    } catch (final IllegalArgumentException e) {
      return TimeSeriesHandlerUtils.badRequest(e);
    }

    final DatabaseInternal database = httpServer.getServer().getDatabase(databaseParam.getFirst(), false, false);

    // Type resolution and the per-type read ACL both live in TimeSeriesGateway, shared with the gRPC
    // TimeSeriesQuery RPC (issue #7305). The ACL matters here more than anywhere else: a TimeSeries type owns
    // no record bucket, so this type-name check is the only thing that can enforce a "readRecord" denial on it.
    // It throws SecurityException -> HTTP 403, and it runs BEFORE the engine-availability branch so a denied
    // caller gets the 403 and not the unavailable-engine diagnostic, which names a file path on disk.
    final TypeResolution resolved = TimeSeriesGateway.resolveForRead(database, typeName);
    if (!resolved.isSuccess())
      return TimeSeriesHandlerUtils.resolutionError(typeName, resolved);

    final TimeSeriesEngine engine = resolved.engine();
    final List<ColumnDefinition> columns = resolved.columns();

    // A tag name that resolves to no TAG column, or a value that could never have been written, is refused with a
    // 400 naming it rather than dropped (issues #7334, #7394): a dropped term WIDENS the conjunction, and a query
    // over every row of the range is indistinguishable, to the caller, from a correct filter that happened to
    // match everything. The range bounds join the same try because a non-numeric 'from' is the same class of
    // client error and used to be answered through the concealed 'detail' field (issue #7340).
    final long fromTs;
    final long toTs;
    final TagFilter tagFilter;
    try {
      fromTs = TimeSeriesHandlerUtils.optLong(payload, "from", Long.MIN_VALUE, "from");
      toTs = TimeSeriesHandlerUtils.optLong(payload, "to", Long.MAX_VALUE, "to");
      tagFilter = buildTagFilter(payload, columns);
    } catch (final IllegalArgumentException e) {
      return TimeSeriesHandlerUtils.badRequest(e);
    }

    // Check if aggregation is requested. isNull rather than has: an explicit "aggregation": null means the caller
    // stated no aggregation, the same reading the optional 'tags' and 'fields' members get, and the same one the
    // Grafana endpoint gives a target's own "aggregation": null.
    if (!payload.isNull("aggregation"))
      return executeAggregation(payload, engine, columns, typeName, fromTs, toTs, tagFilter);

    return executeRawQuery(payload, engine, columns, typeName, fromTs, toTs, tagFilter);
  }

  private ExecutionResponse executeRawQuery(final JSONObject payload, final TimeSeriesEngine engine,
      final List<ColumnDefinition> columns, final String typeName, final long fromTs, final long toTs,
      final TagFilter tagFilter) throws Exception {

    // Same cap and same semantics as the query/command endpoints: a non-positive value means unlimited. Note
    // that here the cap governs serialization only - the engine query below materializes the whole range
    // regardless, so removing the cap does not widen an already unbounded fetch.
    // requireIntLimit rather than payload.getInt: the latter narrows with Number.intValue(), so a limit an int
    // cannot hold would wrap to a negative value and be read as unlimited, exactly as on the other endpoints.
    final Object rawLimit = payload.opt("limit");
    final boolean callerSuppliedLimit = rawLimit != null;

    final int limit;
    final int[] columnIndices;
    try {
      limit = callerSuppliedLimit ? requireIntLimit(rawLimit, "limit") : getDefaultRowLimit();
      // Resolve field projection
      columnIndices = resolveColumnIndices(payload, columns);
    } catch (final IllegalArgumentException e) {
      // Both refusals name the member, and both used to travel to the caller through the generic mapper, which
      // renders an IllegalArgumentException as "Cannot execute command" and hides the sentence that says WHICH
      // member was wrong in the 'detail' field production mode conceals (issue #7340).
      return TimeSeriesHandlerUtils.badRequest(e);
    }

    final List<Object[]> rows = engine.query(fromTs, toTs, columnIndices, tagFilter);

    // The hard ceiling no caller can widen (issue #5719): a caller that states a huge 'limit', or an unlimited
    // one, is refused rather than served an arbitrarily large response. Checked here, before the JSON is built,
    // so the ceiling at least keeps the second and larger copy of the range out of the heap - it cannot keep the
    // first one out, because engine.query() above materializes the whole range before any limit is known. That
    // is a bound on the fetch, and it belongs in the engine, not in this handler.
    final int maxResultRows = getMaxResultRows();
    final int ceiling = applyMaxResultRows(limit, maxResultRows);
    if (ceiling != limit && rows.size() > ceiling)
      throw resultSetTooLarge(maxResultRows);

    // Build column names for response
    final JSONArray colNames = new JSONArray(TimeSeriesGateway.columnNames(columns, columnIndices));

    // Build rows array, applying limit
    final JSONArray rowsArray = new JSONArray();
    final int count = limit > 0 ? Math.min(rows.size(), limit) : rows.size();
    for (int i = 0; i < count; i++) {
      final Object[] row = rows.get(i);
      final JSONArray rowArray = new JSONArray();
      for (final Object val : row)
        // A raw sample can be NaN too, and it means the same "no measurement" there as in an aggregate.
        putSampleValue(rowArray, val);
      rowsArray.put(rowArray);
    }

    final JSONObject result = new JSONObject();
    result.put("type", typeName);
    result.put("columns", colNames);
    result.put("rows", rowsArray);
    result.put("count", count);
    // A response cut by the limit must not look like a complete one (issue #5711).
    final boolean truncated = rows.size() > count;
    result.put("limit", limit > 0 ? limit : -1);
    result.put("truncated", truncated);

    if (truncated && !callerSuppliedLimit)
      // The caller stated no limit, so this truncation is the only one it did not ask for: an operator must be
      // able to find it in the log, exactly as on the query and command endpoints.
      LogManager.instance().log(this, Level.WARNING,
          "Query on time series type '%s' returned %d rows, more than the default HTTP limit of %d: the response has been "
              + "truncated. Set 'limit' in the request, or raise '%s'.", typeName, rows.size(), limit,
          GlobalConfiguration.SERVER_HTTP_QUERY_DEFAULT_LIMIT.getKey());

    return new ExecutionResponse(200, result.toString());
  }

  private ExecutionResponse executeAggregation(final JSONObject payload, final TimeSeriesEngine engine,
      final List<ColumnDefinition> columns, final String typeName, final long fromTs, final long toTs,
      final TagFilter tagFilter) throws Exception {

    final long bucketInterval;
    final List<MultiColumnAggregationRequest> requests = new ArrayList<>();
    final JSONArray aggNames = new JSONArray();

    // One try around the whole member resolution: every refusal inside it is the same class of client error and
    // gets the same answer - an explicit 400 whose 'error' field names the member. Letting any of them reach the
    // generic mapper renders it as "Invalid JSON payload"/"Cannot execute command" with the specifics in the
    // 'detail' field buildErrorBody conceals in production mode (issues #7325, #7340).
    try {
      final JSONObject aggJson = TimeSeriesHandlerUtils.requireObject(payload, "aggregation", "aggregation");
      bucketInterval = TimeSeriesHandlerUtils.requireLong(aggJson, "bucketInterval", "aggregation.bucketInterval");
      final JSONArray requestsJson = TimeSeriesHandlerUtils.requireArray(aggJson, "requests", "aggregation.requests");

      for (int i = 0; i < requestsJson.length(); i++) {
        final String reqPath = "aggregation.requests[" + i + "]";
        final JSONObject req = TimeSeriesHandlerUtils.requireObjectElement(requestsJson, i, reqPath);
        final String fieldName = TimeSeriesHandlerUtils.requireString(req, "field", reqPath + ".field");
        final AggregationType aggType = TimeSeriesHandlerUtils.resolveAggregationType(req, i);
        final String alias = TimeSeriesHandlerUtils.optString(req, "alias",
            fieldName + "_" + aggType.name().toLowerCase(), reqPath + ".alias");

        // The shared helper, as the Grafana handler and the gRPC aggregation path already use: this was the last
        // site outside the gateway still hand-rolling the lookup, and therefore the last place the full-schema vs
        // non-timestamp index conventions could be confused (claude-review on PR #7323).
        final int colIndex = TimeSeriesHandlerUtils.findColumnIndex(fieldName, columns);

        if (colIndex < 0)
          // JSONObject rather than concatenation: fieldName is caller text, and a double quote in it would turn a
          // hand-built body into one no client can parse (claude-review on PR #7680).
          return TimeSeriesHandlerUtils.badRequest(
              new IllegalArgumentException("Field '" + fieldName + "' not found in type"));

        requests.add(new MultiColumnAggregationRequest(colIndex, aggType, alias));
        aggNames.put(alias);
      }
    } catch (final IllegalArgumentException e) {
      return TimeSeriesHandlerUtils.badRequest(e);
    }

    final MultiColumnAggregationResult aggResult = engine.aggregateMulti(fromTs, toTs, requests, bucketInterval,
        tagFilter);

    final List<Long> timestamps = aggResult.getBucketTimestamps();

    // The same ceiling the raw branch enforces (issue #5719). This branch reads no 'limit' at all, so the
    // ceiling is the only bound there is: a small 'bucketInterval' over a wide range produces one response row
    // per bucket, which is the same unbounded response the raw branch was refused for. The engine's own
    // MAX_FLAT_BUCKETS only chooses between a flat array and a map, it is not a response-size bound.
    final int maxResultRows = getMaxResultRows();
    if (maxResultRows > 0 && timestamps.size() > maxResultRows)
      throw resultSetTooLarge(maxResultRows);

    final JSONArray buckets = new JSONArray();

    for (final long ts : timestamps) {
      final JSONObject bucket = new JSONObject();
      bucket.put("timestamp", ts);
      final JSONArray values = new JSONArray();
      for (int r = 0; r < requests.size(); r++)
        // NOT values.put(double): an absent MIN/MAX answers NaN, which that overload rewrites to 0.
        putSampleValue(values, aggResult.getValue(ts, r));
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
    if (payload.isNull("tags"))
      return null;
    return TimeSeriesHandlerUtils.buildTagFilter(
        TimeSeriesHandlerUtils.requireObject(payload, "tags", "tags"), columns);
  }

  private int[] resolveColumnIndices(final JSONObject payload, final List<ColumnDefinition> columns) {
    if (payload.isNull("fields"))
      return null;
    return TimeSeriesHandlerUtils.resolveColumnIndices(
        TimeSeriesHandlerUtils.requireArray(payload, "fields", "fields"), columns, "fields");
  }
}
