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
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;

import java.util.*;

/**
 * Deprecated. Use the query language instead. To retrieve a document by record id, use SQL `SELECT FROM <RID>` statement. Example: `SELECT FROM #10:33`.
 *
 * @Deprecated
 */
@Deprecated
public class GetDocumentHandler extends DatabaseAbstractHandler {
  public GetDocumentHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  @Override
  public ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user, final Database database) {
    final Deque<String> rid = exchange.getQueryParameters().get("rid");
    if (rid == null || rid.isEmpty())
      return new ExecutionResponse(400, "{ \"error\" : \"Record id is null\"}");

    httpServer.getServer().getServerMetrics().meter("http.get-record").mark();

    final String[] ridParts = rid.getFirst().split(":");

    final Document record = (Document) database.lookupByRID(new RID(database, Integer.parseInt(ridParts[0]), Long.parseLong(ridParts[1])), true);

    final String response = "{ \"result\" : " + httpServer.getJsonSerializer().serializeDocument(record).toString() + "}";

    return new ExecutionResponse(200, response);
  }

  @Override
  protected boolean requiresTransaction() {
    return false;
  }
}
