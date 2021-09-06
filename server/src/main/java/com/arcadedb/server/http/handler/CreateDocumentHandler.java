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

import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurity;
import io.undertow.server.HttpServerExchange;
import org.json.JSONObject;

import java.io.IOException;

public class CreateDocumentHandler extends DatabaseAbstractHandler {
  public CreateDocumentHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  public void execute(final HttpServerExchange exchange, ServerSecurity.ServerUser user, final Database database) throws IOException {
    final String payload = parseRequestPayload(exchange);

    final JSONObject json = new JSONObject(payload);

    final String type = json.getString("@type");
    if (type == null) {
      exchange.setStatusCode(400);
      exchange.getResponseSender().send("{ \"error\" : \"@type attribute not found in the record payload\"}");
      return;
    }

    httpServer.getServer().getServerMetrics().meter("http.create-record").mark();

    database.begin();
    try {
      final MutableDocument document = database.newDocument(type);
      document.save();
      database.commit();

      exchange.setStatusCode(200);
      exchange.getResponseSender().send("{ \"result\" : \"" + document.getIdentity() + "\"}");

    } finally {
      database.rollbackAllNested();
    }
  }
}
