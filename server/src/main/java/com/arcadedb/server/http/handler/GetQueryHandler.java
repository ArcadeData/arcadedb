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
import com.arcadedb.server.ServerMetrics;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import org.json.JSONObject;

import java.io.*;
import java.util.*;

public class GetQueryHandler extends AbstractQueryHandler {
  public GetQueryHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  public void execute(final HttpServerExchange exchange, ServerSecurityUser user, final Database database) throws UnsupportedEncodingException {
    final Deque<String> textPar = exchange.getQueryParameters().get("command");
    if (textPar == null || textPar.isEmpty()) {
      exchange.setStatusCode(400);
      exchange.getResponseSender().send("{ \"error\" : \"Command text is null\"}");
      return;
    }
    final String text = textPar.getFirst();

    final Deque<String> languagePar = exchange.getQueryParameters().get("language");
    if (languagePar == null || languagePar.isEmpty()) {
      exchange.setStatusCode(400);
      exchange.getResponseSender().send("{ \"error\" : \"Language is null\"}");
      return;
    }
    final String language = languagePar.getFirst();

    final String serializer;
    Deque<String> serializerPar = exchange.getQueryParameters().get("serializer");
    if (serializerPar == null || serializerPar.isEmpty())
      serializer = "record";
    else
      serializer = serializerPar.getFirst();

    final int limit;
    final Deque<String> limitPar = exchange.getQueryParameters().get("limit");
    if (limitPar == null || limitPar.isEmpty())
      limit = DEFAULT_LIMIT;
    else
      limit = Integer.parseInt(limitPar.getFirst());

    final JSONObject response = createResult(user);

    final ServerMetrics.MetricTimer timer = httpServer.getServer().getServerMetrics().timer("http.query");
    try {

      final ResultSet qResult = database.query(language, text);

      serializeResultSet(database, serializer, limit, response, qResult);

    } finally {
      timer.stop();
    }

    exchange.setStatusCode(200);
    exchange.getResponseSender().send(response.toString());
  }

  @Override
  protected boolean requiresTransaction() {
    return false;
  }
}
