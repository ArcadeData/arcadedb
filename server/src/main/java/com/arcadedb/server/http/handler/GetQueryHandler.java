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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.io.*;

public class GetQueryHandler extends AbstractQueryHandler {
  public GetQueryHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final Database database)
      throws UnsupportedEncodingException {
    final String text = getQueryParameter(exchange, "command");
    if (text == null)
      return new ExecutionResponse(400, "{ \"error\" : \"Command text is null\"}");

    final String language = getQueryParameter(exchange, "language");
    if (language == null)
      return new ExecutionResponse(400, "{ \"error\" : \"Language is null\"}");

    String serializer = getQueryParameter(exchange, "serializer");
    if (serializer == null)
      serializer = "record";

    String limitPar = getQueryParameter(exchange, "limit");
    final int limit;
    if (limitPar == null)
      limit = DEFAULT_LIMIT;
    else
      limit = Integer.parseInt(limitPar);

    final JSONObject response = createResult(user, database);

    try {

      final ResultSet qResult = database.query(language, text);

      serializeResultSet(database, serializer, limit, response, qResult);

    } finally {
      httpServer.getServer().getServerMetrics().meter("http.query").hit();
    }

    return new ExecutionResponse(200, response.toString());
  }

  @Override
  protected boolean requiresTransaction() {
    return false;
  }
}
