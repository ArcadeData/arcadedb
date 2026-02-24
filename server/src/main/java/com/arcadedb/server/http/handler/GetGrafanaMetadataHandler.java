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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.util.Deque;

/**
 * Grafana metadata endpoint — discovers TimeSeries types, fields, and tags.
 * Endpoint: GET /api/v1/ts/{database}/grafana/metadata
 */
public class GetGrafanaMetadataHandler extends AbstractServerHttpHandler {

  public GetGrafanaMetadataHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final JSONObject payload) throws Exception {

    final Deque<String> databaseParam = exchange.getQueryParameters().get("database");
    if (databaseParam == null || databaseParam.isEmpty())
      return new ExecutionResponse(400, "{ \"error\" : \"Database parameter is required\"}");

    final DatabaseInternal database = httpServer.getServer().getDatabase(databaseParam.getFirst(), false, false);

    final JSONArray typesArray = new JSONArray();

    for (final DocumentType docType : database.getSchema().getTypes()) {
      if (!(docType instanceof LocalTimeSeriesType tsType) || tsType.getEngine() == null)
        continue;

      final JSONObject typeObj = new JSONObject();
      typeObj.put("name", tsType.getName());

      final JSONArray fieldsArray = new JSONArray();
      final JSONArray tagsArray = new JSONArray();

      for (final ColumnDefinition col : tsType.getTsColumns()) {
        if (col.getRole() == ColumnDefinition.ColumnRole.TIMESTAMP)
          continue;

        final JSONObject colObj = new JSONObject();
        colObj.put("name", col.getName());
        colObj.put("dataType", col.getDataType().name());

        if (col.getRole() == ColumnDefinition.ColumnRole.TAG)
          tagsArray.put(colObj);
        else
          fieldsArray.put(colObj);
      }

      typeObj.put("fields", fieldsArray);
      typeObj.put("tags", tagsArray);
      typesArray.put(typeObj);
    }

    final JSONArray aggTypes = new JSONArray();
    for (final AggregationType at : AggregationType.values())
      aggTypes.put(at.name());

    final JSONObject result = new JSONObject();
    result.put("types", typesArray);
    result.put("aggregationTypes", aggTypes);

    return new ExecutionResponse(200, result.toString());
  }
}
