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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.timeseries.AggregationType;
import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.MultiColumnAggregationRequest;
import com.arcadedb.engine.timeseries.MultiColumnAggregationResult;
import com.arcadedb.engine.timeseries.TagFilter;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.engine.timeseries.TimeSeriesGateway;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.Type;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.util.ArrayList;
import java.util.List;

/**
 * Grafana DataFrame query endpoint.
 * Endpoint: POST /api/v1/ts/{database}/grafana/query
 *
 * Accepts multi-target queries and returns Grafana DataFrame wire format (columnar arrays with schema metadata).
 * <p>
 * On {@link DatabaseAbstractHandler} since issue #7681, which finished for the ten Grafana and Prometheus
 * routes what issue #7402 did for the three documented {@code /api/v1/ts} ones. A request carrying
 * {@code arcadedb-session-id} now reads through the transaction that session opened - under the session's
 * lock, on the session's principal, with the session's idle clock refreshed - instead of on whatever context
 * the Undertow worker happened to carry. Before, the header was accepted by the transport and dropped by the
 * handler, which is the failure mode that cannot be noticed from the answer.
 * <p>
 * That base class also subsumes the {@code checkAuthorizationOnDatabase} call this handler used to make by
 * hand - the helper whose own javadoc said it existed "Because these handlers do not extend
 * {@code DatabaseAbstractHandler}". It is the database-level gate of GHSA-x8mg-6r4p-87pf and the per-type
 * principal binding of GHSA-c23x-pqcj-7hfm in one, which is what that helper stood in for.
 * <p>
 * {@link #requiresTransaction()} is false: this is a read, and an auto-commit wrapper around it would only add
 * a commit with nothing to commit. A consequence of that answer, shared with {@code POST /api/v1/ts/{database}/query},
 * is that an unresolvable session id degrades to a session-less read rather than being refused - see
 * {@link DatabaseAbstractHandler#rejectsUnresolvableSession()}.
 */
public class PostGrafanaQueryHandler extends DatabaseAbstractHandler {

  public PostGrafanaQueryHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }

  @Override
  protected boolean requiresTransaction() {
    return false;
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final Database db, final JSONObject payload) throws Exception {

    // A missing body goes through the SAME refusal an absent 'targets' member gets below, rather than a second,
    // differently worded one: 'targets' is absent either way (issue #7340).
    if (payload == null)
      return TimeSeriesHandlerUtils.badRequest(TimeSeriesHandlerUtils.missingMember("targets", "a JSON array"));

    final DatabaseInternal database = (DatabaseInternal) db;

    // The request ENVELOPE is refused as a whole, because nothing in it belongs to one target: a 'targets' that is
    // not an array, or a range bound that is not a number, leaves no refId to key an error frame by. The reason
    // travels in 'error' rather than through the generic mapper, whose 'detail' field buildErrorBody conceals in
    // production mode (issue #7340).
    final long fromTs;
    final long toTs;
    final int maxDataPoints;
    final JSONArray targets;
    try {
      // 'targets' first, so that a request which is malformed in more than one way is told about the member the
      // whole endpoint is built around before it is told about a range bound. That is also the order the handler
      // used to check in (claude-review on PR #7680).
      targets = TimeSeriesHandlerUtils.requireArray(payload, "targets", "targets");
      fromTs = TimeSeriesHandlerUtils.optLong(payload, "from", Long.MIN_VALUE, "from");
      toTs = TimeSeriesHandlerUtils.optLong(payload, "to", Long.MAX_VALUE, "to");
      maxDataPoints = TimeSeriesHandlerUtils.optInt(payload, "maxDataPoints", 0, "maxDataPoints");
    } catch (final IllegalArgumentException e) {
      return TimeSeriesHandlerUtils.badRequest(e);
    }

    final JSONObject results = new JSONObject();

    for (int t = 0; t < targets.length(); t++) {
      final String targetPath = "targets[" + t + "]";

      final JSONObject target;
      final String refId;
      try {
        target = TimeSeriesHandlerUtils.requireObjectElement(targets, t, targetPath);
        refId = TimeSeriesHandlerUtils.optString(target, "refId", "A", targetPath + ".refId");
      } catch (final IllegalArgumentException e) {
        // These two are the envelope again rather than the target: without a target object and a refId string
        // there is no key to hang an error frame on, so the whole request is refused (issue #7340).
        return TimeSeriesHandlerUtils.badRequest(e);
      }

      results.put(refId, buildTargetFrame(database, target, targetPath, fromTs, toTs, maxDataPoints));
    }

    final JSONObject response = new JSONObject();
    response.put("results", results);
    return new ExecutionResponse(200, response.toString());
  }

  /**
   * Answers one target, as the frame that goes under its {@code refId}. Every problem with what the TARGET states
   * is caught here and returned as that target's error frame, so one malformed panel cannot blank the panels that
   * are fine (issues #7325, #7334, #7340).
   * <p>
   * The catches are deliberately narrow, around the member resolution alone. An {@link IllegalArgumentException}
   * raised INSIDE the engine - {@code MultiColumnAggregationResult.mergeFrom} raises one when two shards' flat
   * windows disagree - is a broken engine invariant and not a caller mistake: it propagates to the handler mapper
   * as a server error rather than being rendered as a 200 frame whose message carries internal bucket geometry.
   *
   * @param targetPath this target's request path, e.g. {@code targets[0]}, so a refusal names the member the
   *                   caller actually wrote (issue #7340)
   */
  private JSONObject buildTargetFrame(final DatabaseInternal database, final JSONObject target,
      final String targetPath, final long fromTs, final long toTs, final int maxDataPoints) throws Exception {

    final String typeName;
    try {
      typeName = TimeSeriesHandlerUtils.requireString(target, "type", targetPath + ".type");
    } catch (final IllegalArgumentException e) {
      // Everything the target itself states is answered as THIS target's error frame, never as a failure of the
      // request: one mistyped member must not blank the panels that are fine (issues #7325, #7334, #7340). 'type'
      // was the last read of a target that still escaped the per-target loop and failed the whole request.
      return buildErrorFrame(e.getMessage());
    }

    if (!database.getSchema().existsType(typeName))
      return buildErrorFrame("Type '" + typeName + "' does not exist");

    final DocumentType docType = database.getSchema().getType(typeName);
    if (!(docType instanceof LocalTimeSeriesType tsType))
      return buildErrorFrame("Type '" + typeName + "' is not a TimeSeries type");

    // Gated accessor (per-type ACL): a denial fails the whole request with 403 rather than being folded into
    // an error frame, which a Grafana panel would render as a data problem instead of an access problem. Placed
    // before the availability branch below because that frame carries getEngineUnavailableReason(), i.e. a path
    // on disk, which a caller denied on this type must not receive; the accessor returns null in exactly the
    // cases isEngineAvailable() was false, so it replaces that test rather than following it.
    //
    // It throws SecurityException, NOT IllegalArgumentException, so none of the narrow catches in this method
    // folds a 403 into a frame: the denial reaches the handler mapper and answers 403 for the request.
    final TimeSeriesEngine engine = tsType.getEngine(SecurityDatabaseUser.ACCESS.READ_RECORD);
    if (engine == null)
      // Distinct from "not a TimeSeries type" (issue #6356 follow-up, claude-review on PR #6779): this type IS
      // one, its storage just failed to load - the old shared message sent an operator chasing the wrong cause.
      return buildErrorFrame(
          "TimeSeries type '" + typeName + "' has no storage engine available: " + tsType.getEngineUnavailableReason());

    final List<ColumnDefinition> columns = tsType.getTsColumns();

    // Build tag filter. A name that resolves to no TAG column is refused (issue #7334) - it used to be
    // dropped, which turned a typo into a query over every series of the type, and a Grafana panel showing a
    // plausible wrong series is the worst possible answer.
    final TagFilter tagFilter;
    try {
      tagFilter = target.isNull("tags") ? null
          : TimeSeriesHandlerUtils.buildTagFilter(
              TimeSeriesHandlerUtils.requireObject(target, "tags", targetPath + ".tags"), columns);
    } catch (final IllegalArgumentException e) {
      return buildErrorFrame(e.getMessage());
    }

    if (!target.isNull("aggregation"))
      return executeAggregation(target, targetPath, engine, columns, fromTs, toTs, maxDataPoints, tagFilter);

    return executeRawQuery(target, targetPath, engine, columns, fromTs, toTs, tagFilter);
  }

  private JSONObject executeRawQuery(final JSONObject target, final String targetPath,
      final TimeSeriesEngine engine, final List<ColumnDefinition> columns, final long fromTs, final long toTs,
      final TagFilter tagFilter) throws Exception {

    // The try covers the PROJECTION ONLY, never engine.query below. An IllegalArgumentException raised inside the
    // engine is a broken engine invariant, not a caller mistake, and folding it into an error frame would answer
    // 200 with internal state in the message - a server fault rendered to a dashboard as a data problem.
    final int[] columnIndices;
    try {
      columnIndices = target.isNull("fields") ? null
          : TimeSeriesHandlerUtils.resolveColumnIndices(
              TimeSeriesHandlerUtils.requireArray(target, "fields", targetPath + ".fields"), columns,
              targetPath + ".fields");
    } catch (final IllegalArgumentException e) {
      return buildErrorFrame(e.getMessage());
    }

    final List<Object[]> rows = engine.query(fromTs, toTs, columnIndices, tagFilter);

    // Build schema fields and columnar data. The projection's indices count NON-timestamp columns and the
    // engine always prepends the timestamp, so the selection is resolved by the same helper the /ts/query and
    // gRPC paths use rather than by indexing the full schema with them - which named the neighbouring column
    // and left a trailing null (issue #7305).
    final List<ColumnDefinition> selectedColumns = TimeSeriesGateway.selectedColumns(columns, columnIndices);

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
        // Same treatment as the aggregation branch below: a non-finite raw sample is a gap, and it must not
        // reach a dashboard as a number - nor as a JSON literal the response writer refuses to emit.
        putSampleValue(columnArrays[c], row[c]);
    }

    final JSONArray valuesArray = new JSONArray();
    for (final JSONArray col : columnArrays)
      valuesArray.put(col);

    return buildFrame(schemaFields, valuesArray);
  }

  private JSONObject executeAggregation(final JSONObject target, final String targetPath,
      final TimeSeriesEngine engine, final List<ColumnDefinition> columns, final long fromTs, final long toTs,
      final int maxDataPoints, final TagFilter tagFilter) throws Exception {

    final String aggPath = targetPath + ".aggregation";

    final List<MultiColumnAggregationRequest> requests = new ArrayList<>();
    final List<String> aliases = new ArrayList<>();
    final long bucketInterval;

    // The try covers the REQUEST the caller stated and nothing else - engine.aggregateMulti runs below it. An
    // IllegalArgumentException from inside the engine (MultiColumnAggregationResult.mergeFrom raises one when two
    // shards' flat windows disagree) is a broken engine invariant, not a caller mistake: folding it into an error
    // frame would answer 200 and put internal bucket geometry in a panel, which is how a server fault gets
    // mistaken for a data problem. The sibling /ts/query handler keeps the same split.
    try {
      final JSONObject aggJson = TimeSeriesHandlerUtils.requireObject(target, "aggregation", aggPath);
      final JSONArray requestsJson = TimeSeriesHandlerUtils.requireArray(aggJson, "requests", aggPath + ".requests");

      // Determine bucket interval: explicit or auto-calculated from maxDataPoints.
      //
      // The two are not the same member (issue #7675). ABSENT is genuinely optional here and means "derive one",
      // which is what maxDataPoints and the 60000 fallback are for, and that stays. A bucketInterval the caller
      // DID state and stated as <= 0 is a client error, and substituting 60000 for it answered a panel drawn at
      // a resolution nobody asked for - the same input /ts/query used to collapse into a single bucket and gRPC
      // has always refused. isNull() rather than a sentinel: it is the only way to tell "stated 0" from "not
      // stated", and it is the reading every optional member on these endpoints already gets.
      final long resolvedInterval;
      if (aggJson.isNull("bucketInterval")) {
        long derived = 0;
        if (maxDataPoints > 0 && fromTs != Long.MIN_VALUE && toTs != Long.MAX_VALUE)
          derived = Math.max(1, (toTs - fromTs) / maxDataPoints);
        resolvedInterval = derived > 0 ? derived : 60000; // fallback: 1 minute
      } else {
        resolvedInterval = TimeSeriesGateway.requireBucketInterval(
            TimeSeriesHandlerUtils.requireLong(aggJson, "bucketInterval", aggPath + ".bucketInterval"),
            aggPath + ".bucketInterval");
      }
      bucketInterval = resolvedInterval;

      // As on /ts/query: an empty array used to answer a bucket per interval whose 'values' array was empty,
      // which a Grafana panel renders as a frame with a time column and nothing to plot (issue #7675).
      TimeSeriesGateway.requireAggregationRequests(requestsJson.length(), aggPath + ".requests");

      for (int i = 0; i < requestsJson.length(); i++) {
        // Every refusal below is an IllegalArgumentException naming the member, rendered as this target's error
        // frame (issues #7325, #7340).
        final String reqPath = aggPath + ".requests[" + i + "]";
        final JSONObject req = TimeSeriesHandlerUtils.requireObjectElement(requestsJson, i, reqPath);
        final String fieldName = TimeSeriesHandlerUtils.requireString(req, "field", reqPath + ".field");
        final AggregationType aggType = TimeSeriesHandlerUtils.resolveAggregationType(req, reqPath + ".type");
        final String alias = TimeSeriesHandlerUtils.optString(req, "alias",
            fieldName + "_" + aggType.name().toLowerCase(), reqPath + ".alias");

        final int colIndex = TimeSeriesHandlerUtils.findColumnIndex(fieldName, columns);
        if (colIndex < 0)
          return buildErrorFrame("Field '" + fieldName + "' not found in type");

        requests.add(new MultiColumnAggregationRequest(colIndex, aggType, alias));
        aliases.add(alias);
      }
    } catch (final IllegalArgumentException e) {
      return buildErrorFrame(e.getMessage());
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
        // NOT put(double): an absent MIN/MAX answers NaN, which that overload rewrites to 0 - and a dashboard
        // draws a dip to zero instead of the gap the data actually has.
        putSampleValue(aggColumns[r], aggResult.getValue(ts, r));
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
