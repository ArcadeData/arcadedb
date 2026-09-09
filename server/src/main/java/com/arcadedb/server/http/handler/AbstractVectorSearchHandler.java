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
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.security.ServerSecurityUser;
import io.micrometer.core.instrument.Metrics;
import io.undertow.server.HttpServerExchange;

/**
 * Base of the three {@code POST /api/v1/vector/{database}/*} routes (issue #7306).
 * <p>
 * Each subclass supplies one search operation from {@code com.arcadedb.query.search}, which is the same code the
 * MCP tools and the gRPC vector RPCs call. Nothing about a legal request is decided here: argument validation and
 * the {@code k} / {@code efSearch} / {@code limit} bounds live in the operation, so the three surfaces cannot
 * disagree about what a request means. This class owns only the HTTP-shaped part - reading the body, running the
 * search off the I/O thread, and counting the call.
 * <p>
 * An {@link IllegalArgumentException} from an operation reaches the client as HTTP 400 and a
 * {@link SecurityException} as HTTP 403, through {@code AbstractServerHttpHandler}'s standard mapping; neither is
 * caught here, so the message the MCP caller sees for a malformed request is the message the HTTP caller sees.
 */
public abstract class AbstractVectorSearchHandler extends DatabaseAbstractHandler {

  protected AbstractVectorSearchHandler(final HttpServer httpServer) {
    super(httpServer);
  }

  /**
   * Runs the search. Implemented by each route with the matching operation from
   * {@code com.arcadedb.query.search}.
   */
  protected abstract JSONObject search(Database database, JSONObject payload);

  /**
   * The Micrometer counter name for this route, so the three searches can be told apart in the metrics.
   */
  protected abstract String metricName();

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange, final ServerSecurityUser user,
      final Database database, final JSONObject payload) {
    if (payload == null)
      return new ExecutionResponse(400, "{ \"error\" : \"The request body must be a JSON object\"}");

    final JSONObject result = search(database, payload);
    Metrics.counter(metricName()).increment();
    return new ExecutionResponse(200, result.toString());
  }

  /**
   * The search is a blocking read: it runs SQL through the query engine and loads every hit by RID, so it must
   * never occupy an I/O thread.
   */
  @Override
  protected boolean mustExecuteOnWorkerThread() {
    return true;
  }

  /**
   * Read-only, exactly like {@code GET /api/v1/query}: the generated statements are checked for idempotency by the
   * operation itself and a transaction would buy the caller nothing but a commit to roll back.
   */
  @Override
  protected boolean requiresTransaction() {
    return false;
  }
}
