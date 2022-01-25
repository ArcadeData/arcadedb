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
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.server.ServerMetrics;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class PostQueryHandler extends DatabaseAbstractHandler {
  public PostQueryHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  public void execute(final HttpServerExchange exchange, ServerSecurityUser user, final Database database) throws IOException {

    final String payload = parseRequestPayload(exchange);
    if (payload == null || payload.isEmpty()) {
      exchange.setStatusCode(400);
      exchange.getResponseSender().send("{ \"error\" : \"Command text is null\"}");
      return;
    }

    final JSONObject json = new JSONObject(payload);

    final Map<String, Object> requestMap = json.toMap();
    final String language = (String) requestMap.get("language");
    final String command = (String) requestMap.get("command");

    if (command == null || command.isEmpty()) {
      exchange.setStatusCode(400);
      exchange.getResponseSender().send("{ \"error\" : \"Command text is null\"}");
      return;
    }

    final Map<String, Object> paramMap = (Map<String, Object>) requestMap.get("params");
    final ServerMetrics.MetricTimer timer = httpServer.getServer().getServerMetrics().timer("http.command");

    try {
      final ResultSet qResult = command(database, language, command, paramMap);
      final JsonSerializer serializer = httpServer.getJsonSerializer();
      final String result = qResult.stream().map(r -> serializer.serializeResult(r).toString()).collect(Collectors.joining(","));

      exchange.setStatusCode(200);
      exchange.getResponseSender().send("{ \"result\" : [" + result + "] }");

    } finally {
      timer.stop();
    }
  }

  private ResultSet command(Database database, String language, String command, Map<String, Object> paramMap) {
    Object params = mapParams(paramMap);

    if (params instanceof Object[])
      return database.query(language, command, (Object[]) params);

    return database.query(language, command, (Map<String, Object>) params);
  }

  private Object mapParams(Map<String, Object> paramMap) {
    if (paramMap != null) {
      if (!paramMap.isEmpty() && paramMap.containsKey("0")) {
        // ORDINAL
        final Object[] array = new Object[paramMap.size()];
        for (int i = 0; i < array.length; ++i) {
          array[i] = paramMap.get("" + i);
        }
        return array;
      }
    }
    return Optional.ofNullable(paramMap).orElse(Collections.emptyMap());
  }

  @Override
  protected boolean requiresTransaction() {
    return false;
  }
}
