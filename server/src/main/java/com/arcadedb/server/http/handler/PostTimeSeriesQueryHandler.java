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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.AggregationMetrics;
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
import com.arcadedb.server.monitor.TimeSeriesReadMetrics;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

/**
 * HTTP handler for TimeSeries query endpoint.
 * Endpoint: POST /api/v1/ts/{database}/query
 * <p>
 * On {@link DatabaseAbstractHandler} since issue #7402, so a request carrying {@code arcadedb-session-id} reads
 * through the transaction that session opened - and under the session's lock, on the session's principal, with
 * the session's idle clock refreshed - instead of on whatever context the Undertow worker happened to carry.
 * That base class also subsumes the {@code checkAuthorizationOnDatabase} call this handler used to make by
 * hand: it is the database-level gate of GHSA-x8mg-6r4p-87pf and the per-type principal binding of
 * GHSA-c23x-pqcj-7hfm in one, which is what that helper existed to stand in for.
 * <p>
 * {@link #requiresTransaction()} is false: this is a read, and an auto-commit wrapper around it would only add a
 * commit with nothing to commit. A consequence of that answer, shared with {@code GET /query}, is that an
 * unresolvable session id degrades to a session-less read rather than being refused - see
 * {@link DatabaseAbstractHandler#rejectsUnresolvableSession()}.
 */
public class PostTimeSeriesQueryHandler extends DatabaseAbstractHandler {

  public PostTimeSeriesQueryHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }

  /**
   * False: this route reads, and a read detaches from the session rather than committing or rolling it back
   * (issue #7402 states that invariant; issue #7734 makes it hold on the failure path too). The reachable case
   * is {@code resultSetTooLarge}, thrown when the caller's {@code limit} is above - or absent and therefore
   * lowered to - {@code arcadedb.server.httpQueryMaxResultRows}: a 413 on a READ must not destroy the caller's
   * WRITE transaction.
   */
  @Override
  protected boolean participatesInSessionTransaction() {
    return false;
  }

  @Override
  protected boolean requiresTransaction() {
    return false;
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final Database db, final JSONObject payload) throws Exception {

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

    final DatabaseInternal database = (DatabaseInternal) db;

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
      return executeAggregation(payload, engine, database.getName(), columns, typeName, fromTs, toTs, tagFilter);

    return executeRawQuery(payload, engine, database.getName(), columns, typeName, fromTs, toTs, tagFilter);
  }

  private ExecutionResponse executeRawQuery(final JSONObject payload, final TimeSeriesEngine engine,
      final String databaseName, final List<ColumnDefinition> columns, final String typeName, final long fromTs,
      final long toTs, final TagFilter tagFilter) throws Exception {

    // Same cap and same semantics as the query/command endpoints: a non-positive value means unlimited.
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

    // The hard ceiling no caller can widen (issue #5719): a caller that states a huge 'limit', or an unlimited
    // one, is refused rather than served an arbitrarily large response.
    final int maxResultRows = getMaxResultRows();
    final int ceiling = applyMaxResultRows(limit, maxResultRows);

    // The bound on the FETCH (issue #7336). Everything this method has to decide - the row count, whether the
    // response was cut short, and whether the ceiling refuses it - is answered by the rows up to the ceiling
    // plus ONE: that extra row is what tells a cut answer from a complete one, and nothing beyond it is ever
    // looked at. engine.query() answered the same questions by merging every shard's full range into one sorted
    // ArrayList first, so '{"from": 0, "to": 9999999999999, "limit": 10}' over millions of samples cost O(N)
    // heap and O(N log N) time to serialize ten rows.
    // A non-positive ceiling means the ceiling is disabled AND the caller asked for everything, which is the one
    // request that genuinely has no bound; Integer.MAX_VALUE is left alone rather than overflowed, and is
    // unlimited in practice because no List can hold more.
    final int fetchLimit = ceiling <= 0 || ceiling == Integer.MAX_VALUE ? 0 : ceiling + 1;

    // What the read actually did, published to whatever the server's metrics subsystem feeds (issue #7717).
    // null - and therefore free - whenever metrics are off.
    final AggregationMetrics readMetrics = TimeSeriesReadMetrics.start();
    final List<Object[]> rows;
    try {
      rows = engine.queryAscending(fromTs, toTs, columnIndices, tagFilter, fetchLimit, readMetrics);
    } finally {
      TimeSeriesReadMetrics.publish(readMetrics, databaseName, typeName, TimeSeriesReadMetrics.SURFACE_TS_QUERY);
    }

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
      // The message no longer states the total: the fetch now stops one row past the cap, so the only honest
      // thing that can be said is that there were more (issue #7336). Counting them is the O(N) walk the cap
      // exists to avoid, and the remedy the operator needs does not depend on the number.
      LogManager.instance().log(this, Level.WARNING,
          "Query on time series type '%s' returned more rows than the default HTTP limit of %d: the response has been "
              + "truncated to %d rows. Set 'limit' in the request, or raise '%s'.", typeName, limit, count,
          GlobalConfiguration.SERVER_HTTP_QUERY_DEFAULT_LIMIT.getKey());

    return new ExecutionResponse(200, result.toString());
  }

  private ExecutionResponse executeAggregation(final JSONObject payload, final TimeSeriesEngine engine,
      final String databaseName, final List<ColumnDefinition> columns, final String typeName, final long fromTs,
      final long toTs, final TagFilter tagFilter) throws Exception {

    final long bucketInterval;
    final List<MultiColumnAggregationRequest> requests = new ArrayList<>();
    final JSONArray aggNames = new JSONArray();

    // One try around the whole member resolution: every refusal inside it is the same class of client error and
    // gets the same answer - an explicit 400 whose 'error' field names the member. Letting any of them reach the
    // generic mapper renders it as "Invalid JSON payload"/"Cannot execute command" with the specifics in the
    // 'detail' field buildErrorBody conceals in production mode (issues #7325, #7340).
    try {
      final JSONObject aggJson = TimeSeriesHandlerUtils.requireObject(payload, "aggregation", "aggregation");
      // The VALUE of a member #7340 only guaranteed to be present and numeric. A non-positive bucketInterval is
      // refused here rather than handed to the engine, which reads it as "one bucket over the whole range" and
      // used to answer a 200 that looks like a legitimate answer to a legitimate question; the gRPC RPC and the
      // Java client have always refused the same input (issue #7675). The rule lives in TimeSeriesGateway so the
      // three surfaces cannot drift, and the member is named in this endpoint's own spelling.
      bucketInterval = TimeSeriesGateway.requireBucketInterval(
          TimeSeriesHandlerUtils.requireLong(aggJson, "bucketInterval", "aggregation.bucketInterval"),
          "aggregation.bucketInterval");
      final JSONArray requestsJson = TimeSeriesHandlerUtils.requireArray(aggJson, "requests", "aggregation.requests");
      // An empty array is not an aggregation of nothing: it used to answer a bucket per interval whose 'values'
      // array was empty, a shape no client has anything to do with (issue #7675).
      TimeSeriesGateway.requireAggregationRequests(requestsJson.length(), "aggregation.requests");

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

        // Refused here rather than inside the engine, where it used to surface as zeros before compaction and a
        // 500 after it (issue #7725). The catch below renders it as the same named 400 as the refusals above.
        TimeSeriesGateway.requireAggregatableColumn(columns.get(colIndex), aggType);

        requests.add(new MultiColumnAggregationRequest(colIndex, aggType, alias));
        aggNames.put(alias);
      }
    } catch (final IllegalArgumentException e) {
      return TimeSeriesHandlerUtils.badRequest(e);
    }

    // The same ceiling the raw branch enforces (issue #5719). This branch reads no 'limit' at all, so the
    // ceiling is the only bound there is: a small 'bucketInterval' over a wide range produces one response row
    // per bucket, which is the same unbounded response the raw branch was refused for. The engine's own
    // MAX_FLAT_BUCKETS only chooses between a flat array and a map, it is not a response-size bound.
    final int maxResultRows = getMaxResultRows();

    // Carried INTO the scan, so a request that will be refused stops costing the whole range first (issue
    // #7724). The engine stops one block past the ceiling, which leaves a result the check below necessarily
    // refuses - it is over the ceiling by construction - so the refusal and its wording are unchanged and only
    // its price differs.
    final AggregationMetrics readMetrics = TimeSeriesReadMetrics.start();
    final MultiColumnAggregationResult aggResult;
    try {
      aggResult = engine.aggregateMulti(fromTs, toTs, requests, bucketInterval, tagFilter, readMetrics, maxResultRows);
    } finally {
      TimeSeriesReadMetrics.publish(readMetrics, databaseName, typeName, TimeSeriesReadMetrics.SURFACE_TS_QUERY);
    }

    final List<Long> timestamps = aggResult.getBucketTimestamps();

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
