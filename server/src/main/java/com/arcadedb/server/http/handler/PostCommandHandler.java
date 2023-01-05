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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.ServerMetrics;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import com.arcadedb.serializer.json.JSONObject;

import java.io.*;
import java.util.*;

public class PostCommandHandler extends AbstractQueryHandler {

  public PostCommandHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  public void execute(final HttpServerExchange exchange, final ServerSecurityUser user, final Database database) throws IOException {

    final String payload = parseRequestPayload(exchange);
    if (payload == null || payload.isEmpty()) {
      exchange.setStatusCode(400);
      exchange.getResponseSender().send("{ \"error\" : \"Command text is null\"}");
      return;
    }

    final JSONObject json = new JSONObject(payload);

    final Map<String, Object> requestMap = json.toMap();

    final String language = (String) requestMap.get("language");
    String command = decode((String) requestMap.get("command"));
    final int limit = (int) requestMap.getOrDefault("limit", DEFAULT_LIMIT);
    final String serializer = (String) requestMap.getOrDefault("serializer", "record");
    final String profileExecution = (String) requestMap.getOrDefault("profileExecution", null);

    if (command == null || command.isEmpty()) {
      exchange.setStatusCode(400);
      exchange.getResponseSender().send("{ \"error\" : \"Command text is null\"}");
      return;
    }

    Map<String, Object> paramMap = (Map<String, Object>) requestMap.get("params");
    if (paramMap == null)
      paramMap = new HashMap<>();

    final ServerMetrics.MetricTimer timer = httpServer.getServer().getServerMetrics().timer("http.command");

    try {
      if (language.equalsIgnoreCase("sql") || language.equalsIgnoreCase("sqlScript")) {
        final String commandLC = command.toLowerCase().trim();
        if ((commandLC.startsWith("select") || commandLC.startsWith("match")) && !commandLC.endsWith(";")) {
          if (!commandLC.contains(" limit ")) {
            command += " limit " + limit;
          } else {
            final String[] words = commandLC.split(" ");
            if (!"limit".equals(words[words.length - 2]))
              command += " limit " + limit;
          }
        }

        if ("detailed".equalsIgnoreCase(profileExecution))
          paramMap.put("$profileExecution", true);
      }

      final ResultSet qResult = language.equalsIgnoreCase("sqlScript") ?
          executeScript(database, "sql", command, paramMap) :
          executeCommand(database, language, command, paramMap);

      if (qResult == null)
        throw new CommandExecutionException("Error on executing command");

      final JSONObject response = createResult(user);

      serializeResultSet(database, serializer, limit, response, qResult);

      if (profileExecution != null && qResult.getExecutionPlan().isPresent())
        qResult.getExecutionPlan().ifPresent(x -> response.put("explain", qResult.getExecutionPlan().get().prettyPrint(0, 2)));

      final String responseAsString = response.toString();

      exchange.setStatusCode(200);
      exchange.getResponseSender().send(responseAsString);

    } finally {
      timer.stop();
    }
  }

  private ResultSet executeScript(final Database database, final String language, String command, final Map<String, Object> paramMap) {
    final Object params = mapParams(paramMap);

    if (!command.endsWith(";"))
      command += ";";

    if (params instanceof Object[])
      return database.execute(language, command, (Object[]) params);

    return database.execute(language, command, (Map<String, Object>) params);
  }

  private ResultSet executeCommand(final Database database, final String language, final String command, final Map<String, Object> paramMap) {
    final Object params = mapParams(paramMap);

    if (params instanceof Object[])
      return database.command(language, command, (Object[]) params);

    return database.command(language, command, (Map<String, Object>) params);
  }
}
