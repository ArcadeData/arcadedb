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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import org.json.JSONObject;

import java.io.IOException;

public class PostCreateDocumentHandler extends DatabaseAbstractHandler {
  public PostCreateDocumentHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  public void execute(final HttpServerExchange exchange, ServerSecurityUser user, final Database database) throws IOException {
    final String payload = parseRequestPayload(exchange);

    final JSONObject json = new JSONObject(payload);

    final String typeName = (String) json.remove("@type");
    if (typeName == null) {
      exchange.setStatusCode(400);
      exchange.getResponseSender().send("{ \"error\" : \"@type attribute not found in the record payload\"}");
      return;
    }

    httpServer.getServer().getServerMetrics().meter("http.create-record").mark();

    final DocumentType type = database.getSchema().getType(typeName);

    final MutableDocument document;
    if (type instanceof VertexType)
      document = database.newVertex(typeName);
    else if (type instanceof EdgeType)
      document = new MutableEdge(database, type, null);
    else
      document = database.newDocument(typeName);

    document.fromJSON(json);
    document.save();

    exchange.setStatusCode(200);
    exchange.getResponseSender().send("{ \"result\" : \"" + document.getIdentity() + "\"}");
  }
}
