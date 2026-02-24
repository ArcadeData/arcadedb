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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalTimeSeriesType;
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

    final String typeName = getQueryParameter(exchange, "type");
    if (typeName == null || typeName.isBlank())
      return new ExecutionResponse(400, "{ \"error\" : \"'type' query parameter is required\"}");

    final DatabaseInternal database = httpServer.getServer().getDatabase(databaseParam.getFirst(), false, false);

    if (!database.getSchema().existsType(typeName))
      return new ExecutionResponse(400, "{ \"error\" : \"Type '" + typeName + "' does not exist\"}");

    final DocumentType docType = database.getSchema().getType(typeName);
    if (!(docType instanceof LocalTimeSeriesType tsType) || tsType.getEngine() == null)
      return new ExecutionResponse(400, "{ \"error\" : \"Type '" + typeName + "' is not a TimeSeries type\"}");

    final TimeSeriesEngine engine = tsType.getEngine();
    final List<ColumnDefinition> columns = tsType.getTsColumns();

    // Build tag filter from query param
    final TagFilter tagFilter = buildTagFilter(exchange, columns);

    // Query full range and take last element
    final List<Object[]> rows = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, tagFilter);

    // Build column names
    final JSONArray colNames = new JSONArray();
    for (final ColumnDefinition col : columns)
      colNames.put(col.getName());

    final JSONObject result = new JSONObject();
    result.put("type", typeName);
    result.put("columns", colNames);

    if (rows.isEmpty()) {
      result.put("latest", JSONObject.NULL);
    } else {
      final Object[] lastRow = rows.get(rows.size() - 1);
      final JSONArray latestArray = new JSONArray();
      for (final Object val : lastRow)
        latestArray.put(val);
      result.put("latest", latestArray);
    }

    return new ExecutionResponse(200, result.toString());
  }

  private TagFilter buildTagFilter(final HttpServerExchange exchange, final List<ColumnDefinition> columns) {
    final String tagParam = getQueryParameter(exchange, "tag");
    if (tagParam == null || tagParam.isBlank())
      return null;

    final int colonIdx = tagParam.indexOf(':');
    if (colonIdx <= 0)
      return null;

    final String tagName = tagParam.substring(0, colonIdx);
    final String tagValue = tagParam.substring(colonIdx + 1);

    // columnIndex for TagFilter is among non-timestamp columns (0-based)
    int nonTsIdx = 0;
    for (final ColumnDefinition col : columns) {
      if (col.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
        continue;
      if (col.getRole() == ColumnDefinition.ColumnRole.TAG && col.getName().equals(tagName))
        return TagFilter.eq(nonTsIdx, tagValue);
      nonTsIdx++;
    }

    return null;
  }
}
