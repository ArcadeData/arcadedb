/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.server.http.handler;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.integration.importer.Importer;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurity;
import io.undertow.server.HttpServerExchange;
import org.json.JSONObject;

public class ImportDatabaseHandler extends AbstractHandler {
  public ImportDatabaseHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  protected void execute(HttpServerExchange exchange, ServerSecurity.ServerUser user) throws Exception {
    final String payload = parseRequestPayload(exchange);

    final JSONObject json = new JSONObject(payload);

    final String databaseName = json.getString("name");
    if (databaseName == null) {
      exchange.setStatusCode(400);
      exchange.getResponseSender().send("{ \"error\" : \"database name not found\"}");
      return;
    }

    final String databaseURL = json.getString("url");
    if (databaseURL == null) {
      exchange.setStatusCode(400);
      exchange.getResponseSender().send("{ \"error\" : \"URL name not found\"}");
      return;
    }

    final Importer importer = new Importer(new String[] { "-url", databaseURL, "-database",
        httpServer.getServer().getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY) + "/" + databaseName });
    importer.load();

    final Database db = httpServer.getServer().getDatabase(databaseName);

    final JSONObject result = new JSONObject().put("result", new JSONObject().put("types", db.getSchema().getTypes().size()));

    exchange.setStatusCode(200);
    exchange.getResponseSender().send(result.toString());
  }
}
