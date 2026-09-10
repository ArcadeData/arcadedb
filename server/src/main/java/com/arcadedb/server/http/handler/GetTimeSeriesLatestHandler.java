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
import com.arcadedb.engine.timeseries.ColumnDefinition;
import com.arcadedb.engine.timeseries.TagFilter;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.engine.timeseries.TimeSeriesGateway;
import com.arcadedb.engine.timeseries.TimeSeriesGateway.TypeResolution;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.util.Deque;
import java.util.List;

/**
 * HTTP handler for retrieving the latest TimeSeries value.
 * Endpoint: GET /api/v1/ts/{database}/latest?type=weather&tag=location:us-east
 */
public class GetTimeSeriesLatestHandler extends AbstractServerHttpHandler {

  public GetTimeSeriesLatestHandler(final HttpServer httpServer) {
    super(httpServer);
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

    final String typeName = getQueryParameter(exchange, "type");
    if (typeName == null || typeName.isBlank())
      return new ExecutionResponse(400, "{ \"error\" : \"'type' query parameter is required\"}");

    final DatabaseInternal database = httpServer.getServer().getDatabase(databaseParam.getFirst(), false, false);

    // Type resolution and the per-type read ACL both live in TimeSeriesGateway, shared with the gRPC
    // TimeSeriesLatest RPC (issue #7305). The ACL matters here more than anywhere else: a TimeSeries type owns
    // no record bucket, so this type-name check is the only thing that can enforce a "readRecord" denial on it.
    // It throws SecurityException -> HTTP 403, and it runs BEFORE the engine-availability branch so a denied
    // caller gets the 403 and not the unavailable-engine diagnostic, which names a file path on disk.
    final TypeResolution resolved = TimeSeriesGateway.resolveForRead(database, typeName);
    if (!resolved.isSuccess())
      return TimeSeriesHandlerUtils.resolutionError(typeName, resolved);

    final TimeSeriesEngine engine = resolved.engine();
    final List<ColumnDefinition> columns = resolved.columns();

    // Build tag filter from query param
    final TagFilter tagFilter = buildTagFilter(exchange, columns);

    // Query full range and take last element, through the same helper the gRPC TimeSeriesLatest RPC calls
    // (issue #7305) so the two protocols cannot answer different rows.
    final Object[] lastRow = TimeSeriesGateway.latest(engine, tagFilter);

    // Build column names
    final JSONArray colNames = new JSONArray(TimeSeriesGateway.columnNames(columns, null));

    final JSONObject result = new JSONObject();
    result.put("type", typeName);
    result.put("columns", colNames);

    if (lastRow == null) {
      result.put("latest", JSONObject.NULL);
    } else {
      final JSONArray latestArray = new JSONArray();
      for (final Object val : lastRow)
        // putSampleValue, not put(val): a non-finite sample means "no measurement", and every other read path
        // renders it as JSON null - the raw and aggregated branches of /ts/query, the Grafana frames, and the
        // gRPC TimeSeriesLatest RPC added in #7305. Left alone, this loop resolved to JSONArray.put(Object),
        // which does NOT take the NaN-rewriting put(Number) overload, and the endpoint answered the token NaN
        // where its gRPC twin answered null - the two protocols disagreeing on exactly the value this change
        // is about (claude-review on PR #7323).
        putSampleValue(latestArray, val);
      result.put("latest", latestArray);
    }

    return new ExecutionResponse(200, result.toString());
  }

  /**
   * Reads EVERY occurrence of the 'tag' query parameter and conjoins them, so a type with more than one
   * tag column can name a single series: {@code ?tag=host:a&tag=region:eu} means host=a AND region=eu.
   * <p>
   * Deliberately not {@link #getQueryParameter(HttpServerExchange, String)}, which returns the Deque's
   * first entry and so dropped every occurrence past the first (issue #7321). The conjunction itself is
   * the same helper POST /ts/{database}/query uses for its 'tags' object.
   */
  private TagFilter buildTagFilter(final HttpServerExchange exchange, final List<ColumnDefinition> columns) {
    return TimeSeriesHandlerUtils.buildTagFilterFromQueryParams(exchange.getQueryParameters().get("tag"), columns);
  }
}
